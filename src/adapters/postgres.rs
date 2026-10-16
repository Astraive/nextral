#[cfg(feature = "postgres")]
mod enabled {
    use crate::{
        adapters::AdapterHealth,
        contracts::{CoreError, CoreResult},
        domain::{AuditEvent, SessionMessage},
        memory::{deterministic_id, MemoryRecord},
        ports::{AuditWrite, PostgresPort, TenantUserScope},
        prospective::ReminderRecord,
    };
    use postgres::{Client, Transaction};
    use serde_json::{json, Value};
    use sha2::{Digest, Sha256};
    use std::sync::{Arc, Condvar, Mutex};

    const MIGRATIONS: [(&str, &str); 4] = [
        (
            "0001",
            include_str!("../../migrations/postgres/0001_core_schema.sql"),
        ),
        (
            "0002",
            include_str!("../../migrations/postgres/0002_runtime_durability.sql"),
        ),
        (
            "0003",
            include_str!("../../migrations/postgres/0003_lifecycle_execution.sql"),
        ),
        (
            "0004",
            include_str!("../../migrations/postgres/0004_memory_query_indexes.sql"),
        ),
    ];
    const MIGRATION_HISTORY: &str =
        "SELECT version,checksum FROM nextral_schema_migrations ORDER BY version";

    struct PoolInner {
        idle: Vec<Client>,
        active_foreground: usize,
        active_renewal: usize,
    }

    #[derive(Clone, Copy, PartialEq, Eq)]
    pub enum ConnectionKind {
        Foreground,
        Renewal,
    }

    pub struct PooledClient<'a> {
        client: Option<Client>,
        adapter: &'a PostgresAdapter,
        kind: ConnectionKind,
    }

    impl<'a> std::ops::Deref for PooledClient<'a> {
        type Target = Client;
        fn deref(&self) -> &Client {
            self.client.as_ref().unwrap()
        }
    }

    impl<'a> std::ops::DerefMut for PooledClient<'a> {
        fn deref_mut(&mut self) -> &mut Client {
            self.client.as_mut().unwrap()
        }
    }

    impl<'a> Drop for PooledClient<'a> {
        fn drop(&mut self) {
            if let Some(c) = self.client.take() {
                let (lock, cvar) = &*self.adapter.pool;
                if let Ok(mut inner) = lock.lock() {
                    match self.kind {
                        ConnectionKind::Foreground => {
                            inner.active_foreground = inner.active_foreground.saturating_sub(1);
                        }
                        ConnectionKind::Renewal => {
                            inner.active_renewal = inner.active_renewal.saturating_sub(1);
                        }
                    }
                    if !c.is_closed() && inner.idle.len() < 9 {
                        inner.idle.push(c);
                    }
                    cvar.notify_one();
                }
            }
        }
    }

    pub struct PostgresAdapter {
        pub url: String,
        pool: Arc<(Mutex<PoolInner>, Condvar)>,
    }
    impl PartialEq for PostgresAdapter {
        fn eq(&self, other: &Self) -> bool {
            self.url == other.url
        }
    }
    impl Eq for PostgresAdapter {}
    fn db(error: postgres::Error) -> CoreError {
        crate::runtime::observability::record_store_error(
            crate::runtime::observability::StoreBackend::Postgres,
        );
        CoreError::Io(error.to_string())
    }
    fn text<T: serde::Serialize>(v: &T) -> CoreResult<String> {
        serde_json::to_value(v)?
            .as_str()
            .map(str::to_owned)
            .ok_or_else(|| CoreError::Serialization("expected string enum".into()))
    }
    fn migration_checksum(sql: &str) -> String {
        let mut digest = Sha256::new();
        for line in sql.lines() {
            digest.update(line.as_bytes());
            digest.update(b"\n");
        }
        format!("{:x}", digest.finalize())
    }
    fn verify_migration_history(rows: &[postgres::Row]) -> CoreResult<()> {
        for (index, row) in rows.iter().enumerate() {
            let version: &str = row.try_get(0).map_err(db)?;
            let checksum: &str = row.try_get(1).map_err(db)?;
            let Some((expected, sql)) = MIGRATIONS.get(index) else {
                return Err(CoreError::Conflict("unknown migration history".into()));
            };
            if version != *expected {
                return Err(CoreError::Conflict(
                    "unknown or noncontiguous migration history".into(),
                ));
            }
            if checksum != migration_checksum(sql) {
                return Err(CoreError::Conflict(format!(
                    "migration checksum mismatch: {expected}"
                )));
            }
        }
        Ok(())
    }
    fn timestamp_seconds(value: &str) -> CoreResult<f64> {
        Ok(crate::runtime::lifecycle::timestamp_nanos(value)? as f64 / 1_000_000_000.0)
    }
    impl PostgresAdapter {
        pub fn from_config(config: &crate::config::StoreConfig) -> CoreResult<Self> {
            Self::new(&config.postgres_url)
        }
        pub fn new(url: impl Into<String>) -> CoreResult<Self> {
            let url = url.into();
            if url.trim().is_empty() {
                return Err(CoreError::InvalidInput("postgres_url required".into()));
            }
            Ok(Self {
                url,
                pool: Arc::new((
                    Mutex::new(PoolInner {
                        idle: Vec::new(),
                        active_foreground: 0,
                        active_renewal: 0,
                    }),
                    Condvar::new(),
                )),
            })
        }
        fn connect_new(&self) -> CoreResult<Client> {
            let tls = native_tls::TlsConnector::builder()
                .build()
                .map_err(|e| CoreError::Io(e.to_string()))?;
            let url = self.url.replace("sslmode=verify-full", "sslmode=require");
            Client::connect(&url, postgres_native_tls::MakeTlsConnector::new(tls)).map_err(db)
        }
        pub fn checkout(&self, kind: ConnectionKind) -> CoreResult<PooledClient<'_>> {
            let (lock, cvar) = &*self.pool;
            let mut inner = lock.lock().map_err(|e| CoreError::Io(e.to_string()))?;
            loop {
                match kind {
                    ConnectionKind::Foreground => {
                        if inner.active_foreground < 8 {
                            inner.active_foreground += 1;
                            break;
                        }
                    }
                    ConnectionKind::Renewal => {
                        if inner.active_renewal < 1 {
                            inner.active_renewal += 1;
                            break;
                        }
                    }
                }
                inner = cvar.wait(inner).map_err(|e| CoreError::Io(e.to_string()))?;
            }
            let client = if let Some(c) = inner.idle.pop() {
                if !c.is_closed() {
                    Some(c)
                } else {
                    None
                }
            } else {
                None
            };
            drop(inner);
            let client = match client {
                Some(c) => c,
                None => match self.connect_new() {
                    Ok(c) => c,
                    Err(e) => {
                        let mut inner = lock.lock().map_err(|_| CoreError::Io("pool lock".into()))?;
                        match kind {
                            ConnectionKind::Foreground => {
                                inner.active_foreground = inner.active_foreground.saturating_sub(1);
                            }
                            ConnectionKind::Renewal => {
                                inner.active_renewal = inner.active_renewal.saturating_sub(1);
                            }
                        }
                        cvar.notify_one();
                        return Err(e);
                    }
                },
            };
            Ok(PooledClient {
                client: Some(client),
                adapter: self,
                kind,
            })
        }
        fn client(&self) -> CoreResult<PooledClient<'_>> {
            self.checkout(ConnectionKind::Foreground)
        }
        pub fn renewal_client(&self) -> CoreResult<PooledClient<'_>> {
            self.checkout(ConnectionKind::Renewal)
        }
        fn scoped<T>(
            &self,
            tenant: &str,
            user: &str,
            f: impl FnOnce(&mut Transaction<'_>) -> CoreResult<T>,
        ) -> CoreResult<T> {
            crate::store::validate_scope_component(tenant, "tenant_id")?;
            if !user.is_empty() {
                crate::store::validate_scope_component(user, "user_id")?;
            }
            let mut client = self.client()?;
            let mut tx = client.transaction().map_err(db)?;
            tx.query_one("SELECT set_config('nextral.tenant_id',$1,true),set_config('nextral.user_id',$2,true)",&[&tenant,&user]).map_err(db)?;
            let value = f(&mut tx)?;
            tx.commit().map_err(db)?;
            Ok(value)
        }
        pub fn migration_sql(&self) -> &'static str {
            MIGRATIONS[0].1
        }
        pub fn health(&self) -> AdapterHealth {
            AdapterHealth::configured("postgres")
        }
        pub fn readiness(&self) -> CoreResult<Value> {
            let mut client = self.client()?;
            let history = client.query(MIGRATION_HISTORY, &[]).map_err(db)?;
            verify_migration_history(&history)?;
            if history.len() != MIGRATIONS.len() {
                return Err(CoreError::Conflict(format!(
                    "required migration {} has not been applied",
                    MIGRATIONS[history.len()].0
                )));
            }
            Ok(json!({"status":"ready"}))
        }
        pub fn migrate(&self) -> CoreResult<()> {
            let mut c = self.client()?;
            let mut tx = c.transaction().map_err(db)?;
            tx.query_one("SELECT pg_advisory_xact_lock(1852143217)", &[])
                .map_err(db)?;
            tx.batch_execute("CREATE TABLE IF NOT EXISTS nextral_schema_migrations(version TEXT PRIMARY KEY,checksum TEXT NOT NULL,applied_at TIMESTAMPTZ NOT NULL DEFAULT now())").map_err(db)?;
            let history = tx.query(MIGRATION_HISTORY, &[]).map_err(db)?;
            verify_migration_history(&history)?;
            for (version, sql) in &MIGRATIONS[history.len()..] {
                let checksum = migration_checksum(sql);
                tx.batch_execute(sql).map_err(db)?;
                tx.execute(
                    "INSERT INTO nextral_schema_migrations(version,checksum) VALUES($1,$2)",
                    &[version, &checksum],
                )
                .map_err(db)?;
            }
            tx.commit().map_err(db)
        }
        pub fn eligible_sessions(
            &self,
            scope: &TenantUserScope,
            inactive_at_or_before: &str,
            limit: usize,
        ) -> CoreResult<Vec<crate::domain::EligibleSession>> {
            if limit == 0 || limit > 1000 {
                return Err(CoreError::InvalidInput(
                    "session eligibility limit must be between 1 and 1000".into(),
                ));
            }
            if scope.user_id.trim().is_empty() {
                return Err(CoreError::InvalidInput("user_id required".into()));
            }
            let cutoff = timestamp_seconds(inactive_at_or_before)?;
            let limit = limit as i64;
            self.scoped(&scope.tenant_id, &scope.user_id, |tx| {
                tx.query(
                    r#"SELECT session_id,
                        to_char(MAX(created_at) AT TIME ZONE 'UTC',
                            'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
                        COUNT(*)
                    FROM nextral_session_messages
                    WHERE tenant_id=$1 AND user_id=$2
                    GROUP BY session_id
                    HAVING MAX(created_at)<=to_timestamp($3)
                    ORDER BY MAX(created_at),session_id COLLATE "C"
                    LIMIT $4"#,
                    &[&scope.tenant_id, &scope.user_id, &cutoff, &limit],
                )
                .map_err(db)?
                .into_iter()
                .map(|row| {
                    Ok(crate::domain::EligibleSession {
                        session_id: row.try_get(0).map_err(db)?,
                        last_message_at: row.try_get(1).map_err(db)?,
                        message_count: row.try_get::<_, i64>(2).map_err(db)? as u64,
                    })
                })
                .collect()
            })
        }
        fn memory_write(tx: &mut Transaction<'_>, r: &MemoryRecord) -> CoreResult<()> {
            tx.query_one(
                "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
                &[&r.tenant_id],
            )
            .map_err(db)?;
            if r.status == crate::memory::MemoryStatus::Active {
                if let Some(row)=tx.query_opt("SELECT status FROM nextral_memories WHERE tenant_id=$1 AND user_id=$2 AND id=$3",&[&r.tenant_id,&r.user_id,&r.id]).map_err(db)?{
    if row.get::<_,String>(0)!="active"{return Err(CoreError::Conflict("inactive memory cannot be resurrected".into()));}
   }
                for source in &r.source_message_ids {
                    let key = format!("forgotten-source/{}/{}", r.user_id, source);
                    if tx
                        .query_opt(
                            "SELECT 1 FROM nextral_runtime_state WHERE tenant_id=$1 AND key=$2",
                            &[&r.tenant_id, &key],
                        )
                        .map_err(db)?
                        .is_some()
                    {
                        return Err(CoreError::Conflict(
                            "forgotten source cannot be ingested".into(),
                        ));
                    }
                }
            }
            r.validate()?;
            let payload = serde_json::to_value(r)?;
            let dim = r
                .embedding_dim
                .ok_or_else(|| CoreError::InvalidInput("embedding dimension required".into()))?
                as i32;
            let provider = r
                .embedding_provider
                .as_deref()
                .ok_or_else(|| CoreError::InvalidInput("embedding provider required".into()))?;
            let model = r
                .embedding_model
                .as_deref()
                .ok_or_else(|| CoreError::InvalidInput("embedding model required".into()))?;
            let created = timestamp_seconds(&r.created_at)?;
            let updated = timestamp_seconds(&r.updated_at)?;
            let accessed = r
                .last_accessed_at
                .as_deref()
                .map(timestamp_seconds)
                .transpose()?;
            let rows=tx.execute("INSERT INTO nextral_memories(id,tenant_id,user_id,session_id,content,content_type,memory_type,source_type,source_message_ids,importance_score,confidence_score,embedding_provider,embedding_model,embedding_dim,extraction_provider,extraction_model,entities,tags,privacy_level,created_at,updated_at,last_accessed_at,access_count,status,schema_version,record_json) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,to_timestamp($20),to_timestamp($21),to_timestamp($22),$23,$24,$25,$26) ON CONFLICT(tenant_id,id) DO UPDATE SET session_id=excluded.session_id,content=excluded.content,content_type=excluded.content_type,memory_type=excluded.memory_type,source_type=excluded.source_type,source_message_ids=excluded.source_message_ids,importance_score=excluded.importance_score,confidence_score=excluded.confidence_score,embedding_provider=excluded.embedding_provider,embedding_model=excluded.embedding_model,embedding_dim=excluded.embedding_dim,extraction_provider=excluded.extraction_provider,extraction_model=excluded.extraction_model,entities=excluded.entities,tags=excluded.tags,privacy_level=excluded.privacy_level,updated_at=excluded.updated_at,last_accessed_at=excluded.last_accessed_at,access_count=excluded.access_count,status=excluded.status,schema_version=excluded.schema_version,record_json=excluded.record_json WHERE nextral_memories.user_id=excluded.user_id",&[&r.id,&r.tenant_id,&r.user_id,&r.session_id,&r.content,&text(&r.content_type)?,&text(&r.memory_type)?,&text(&r.source_type)?,&serde_json::to_value(&r.source_message_ids)?,&r.importance_score,&r.confidence_score,&provider,&model,&dim,&r.extraction_provider,&r.extraction_model,&serde_json::to_value(&r.entities)?,&serde_json::to_value(&r.tags)?,&text(&r.privacy_level)?,&created,&updated,&accessed,&(r.access_count as i64),&text(&r.status)?,&r.schema_version,&payload]).map_err(db)?;
            if rows == 0 {
                return Err(CoreError::Conflict(
                    "memory id belongs to another user".into(),
                ));
            }
            Ok(())
        }
        pub fn record_accesses(&self, scope: &TenantUserScope, ids: &[&str]) -> CoreResult<()> {
            if ids.is_empty() {
                return Ok(());
            }
            self.scoped(&scope.tenant_id, &scope.user_id, |tx| {
                tx.execute(
                    "UPDATE nextral_memories SET access_count=LEAST(access_count,9223372036854775806)+1,last_accessed_at=statement_timestamp(),record_json=jsonb_set(jsonb_set(record_json,'{access_count}',to_jsonb(LEAST(access_count,9223372036854775806)+1)),'{last_accessed_at}',to_jsonb(floor(extract(epoch FROM statement_timestamp()))::bigint::text)) WHERE tenant_id=$1 AND user_id=$2 AND id=ANY($3) AND status='active'",
                    &[&scope.tenant_id, &scope.user_id, &ids],
                ).map_err(db)?;
                Ok(())
            })
        }
        pub fn get_active_memories(
            &self,
            scope: &TenantUserScope,
            ids: &[&str],
        ) -> CoreResult<Vec<MemoryRecord>> {
            if ids.is_empty() {
                return Ok(Vec::new());
            }
            self.scoped(&scope.tenant_id, &scope.user_id, |tx| {
                tx.query(
                    "SELECT m.record_json FROM nextral_memories m WHERE m.tenant_id=$1 AND m.user_id=$2 AND m.id=ANY($3) AND m.status='active' AND NOT EXISTS(SELECT 1 FROM jsonb_array_elements_text(m.source_message_ids) AS source(id) JOIN nextral_runtime_state s ON s.tenant_id=m.tenant_id AND s.key='forgotten-source/'||m.user_id||'/'||source.id)",
                    &[&scope.tenant_id, &scope.user_id, &ids],
                ).map_err(db)?.into_iter()
                    .map(|row| serde_json::from_value(row.get(0)).map_err(CoreError::from)).collect()
            })
        }
        pub fn current_graph_memory_ids(
            &self,
            scope: &TenantUserScope,
            ids: &[String],
            privacy: &[crate::memory::PrivacyLevel],
        ) -> CoreResult<Vec<String>> {
            if ids.is_empty() || privacy.is_empty() {
                return Ok(Vec::new());
            }
            let privacy = privacy.iter().map(text).collect::<CoreResult<Vec<_>>>()?;
            self.scoped(&scope.tenant_id, &scope.user_id, |tx| {
                tx.query(
                    "SELECT m.id FROM nextral_memories m JOIN nextral_runtime_state g ON g.tenant_id=m.tenant_id AND g.key='graph/'||m.user_id||'/'||m.id WHERE m.tenant_id=$1 AND m.user_id=$2 AND m.id=ANY($3) AND m.status='active' AND m.privacy_level=ANY($4) AND g.value->>'revision' IS NOT NULL AND g.value->'revision'=g.value->'applied_revision' AND (m.record_json-ARRAY['access_count','last_accessed_at','embedding','embedding_dim','embedding_model','embedding_provider'])=((g.value->'record')-ARRAY['access_count','last_accessed_at','embedding','embedding_dim','embedding_model','embedding_provider']) AND NOT EXISTS(SELECT 1 FROM jsonb_array_elements_text(m.source_message_ids) AS source(id) JOIN nextral_runtime_state s ON s.tenant_id=m.tenant_id AND s.key='forgotten-source/'||m.user_id||'/'||source.id)",
                    &[&scope.tenant_id, &scope.user_id, &ids, &privacy],
                ).map_err(db).map(|rows| rows.into_iter().map(|row| row.get(0)).collect())
            })
        }
        fn audit_write(tx: &mut Transaction<'_>, e: &AuditWrite) -> CoreResult<()> {
            let supplied =
                e.id.as_deref()
                    .map(std::borrow::Cow::Borrowed)
                    .unwrap_or_else(|| {
                        std::borrow::Cow::Owned(crate::runtime::lifecycle::worker_owner())
                    });
            let id = deterministic_id(&[&e.tenant_id, supplied.as_ref()]);
            let metadata: Value = serde_json::from_str(&e.metadata_json)?;
            let parameters: &[&(dyn ::postgres::types::ToSql + Sync)] = &[
                &id,
                &e.tenant_id,
                &e.user_id,
                &e.actor_id,
                &e.action,
                &e.target_type,
                &e.target_id,
                &e.reason,
                &e.request_id,
                &e.trace_id,
                &metadata,
            ];
            let inserted = tx.execute("INSERT INTO nextral_audit_events(id,tenant_id,user_id,actor_id,action,target_type,target_id,reason,request_id,trace_id,metadata,created_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,now()) ON CONFLICT(id) DO NOTHING", parameters).map_err(db)?;
            if inserted == 0 && tx.query_opt(
                "SELECT 1 FROM nextral_audit_events WHERE id=$1 AND tenant_id=$2 AND user_id IS NOT DISTINCT FROM $3 AND actor_id=$4 AND action=$5 AND target_type=$6 AND target_id IS NOT DISTINCT FROM $7 AND reason=$8 AND request_id IS NOT DISTINCT FROM $9 AND trace_id IS NOT DISTINCT FROM $10 AND metadata=$11",
                parameters,
            ).map_err(db)?.is_none() {
                return Err(CoreError::Conflict("audit identity reused for a different event".into()));
            }
            Ok(())
        }
        fn outbox_write(
            tx: &mut Transaction<'_>,
            tenant: &str,
            kind: &str,
            id: &str,
            payload: &Value,
        ) -> CoreResult<String> {
            let event = deterministic_id(&[tenant, kind, id, &payload.to_string()]);
            tx.execute("INSERT INTO nextral_outbox_events(id,tenant_id,event_type,aggregate_type,aggregate_id,payload,status,created_at,updated_at) VALUES($1,$2,$3,'memory',$4,$5,'pending',now(),now()) ON CONFLICT(id) DO NOTHING",&[&event,&tenant,&kind,&id,&payload]).map_err(db)?;
            Ok(event)
        }
        fn graph_plan(
            tx: &mut Transaction<'_>,
            record: &MemoryRecord,
            payload: &Value,
        ) -> CoreResult<Value> {
            let mut plan = payload.clone();
            plan["source_fingerprint"] =
                json!(crate::runtime::graphify::source_fingerprint(record)?);
            plan["revision"] = json!(crate::runtime::lifecycle::worker_owner());
            let key = crate::runtime::graphify::graph_state_key(&record.user_id, &record.id);
            tx.execute(
                "INSERT INTO nextral_runtime_state(tenant_id,key,value) VALUES($1,$2,$3) ON CONFLICT(tenant_id,key) DO UPDATE SET value=excluded.value,updated_at=now()",
                &[&record.tenant_id, &key, &plan],
            ).map_err(db)?;
            Ok(plan)
        }
        pub fn commit_graph_replay(
            &self,
            record: &MemoryRecord,
            audit: &AuditWrite,
            payload: &Value,
        ) -> CoreResult<String> {
            self.scoped(&record.tenant_id, &record.user_id, |tx| {
                tx.query_one("SELECT pg_advisory_xact_lock(hashtextextended($1,0))", &[&record.tenant_id]).map_err(db)?;
                let privacy_key = format!("privacy/{}", record.user_id);
                if tx.query_opt(
                    "SELECT 1 FROM nextral_runtime_state WHERE tenant_id=$1 AND key=$2 AND locked_until>now() FOR UPDATE",
                    &[&record.tenant_id, &privacy_key],
                ).map_err(db)?.is_some() {
                    return Err(CoreError::Conflict("source lifecycle is leased".into()));
                }
                let current: MemoryRecord = tx.query_opt(
                    "SELECT record_json FROM nextral_memories WHERE tenant_id=$1 AND user_id=$2 AND id=$3 FOR UPDATE",
                    &[&record.tenant_id, &record.user_id, &record.id],
                ).map_err(db)?.map(|row| serde_json::from_value(row.get(0))).transpose()?
                    .ok_or_else(|| CoreError::NotFound("canonical graph source missing".into()))?;
                if current.status != crate::memory::MemoryStatus::Active
                    || crate::runtime::graphify::source_fingerprint(&current)?
                        != crate::runtime::graphify::source_fingerprint(record)?
                {
                    return Err(CoreError::Conflict("canonical graph source changed".into()));
                }
                for source in &current.source_message_ids {
                    let key = format!("forgotten-source/{}/{}", current.user_id, source);
                    if tx.query_opt("SELECT 1 FROM nextral_runtime_state WHERE tenant_id=$1 AND key=$2", &[&current.tenant_id, &key]).map_err(db)?.is_some() {
                        return Err(CoreError::Conflict("graph source was forgotten".into()));
                    }
                }
                let plan = Self::graph_plan(tx, &current, payload)?;
                Self::audit_write(tx, audit)?;
                Self::outbox_write(tx, &current.tenant_id, "graph.replay", &current.id, &plan)
            })
        }
        pub fn acknowledge_graph(&self, record: &MemoryRecord, revision: &Value) -> CoreResult<()> {
            let key = crate::runtime::graphify::graph_state_key(&record.user_id, &record.id);
            self.scoped(&record.tenant_id, &record.user_id, |tx| {
                tx.execute(
                    "UPDATE nextral_runtime_state SET value=jsonb_set(value,'{applied_revision}',$3),updated_at=now() WHERE tenant_id=$1 AND key=$2 AND value->'revision'=$3",
                    &[&record.tenant_id, &key, &revision],
                ).map_err(db)?;
                Ok(())
            })
        }
        pub fn commit_memory(
            &self,
            r: &MemoryRecord,
            audit: &AuditWrite,
            payload: &Value,
            logical: &str,
        ) -> CoreResult<String> {
            self.scoped(&r.tenant_id, &r.user_id, |tx| {
                tx.query_one(
                    "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
                    &[&r.tenant_id],
                )
                .map_err(db)?;
                if r.status == crate::memory::MemoryStatus::Active {
                    let key = format!("alias/{logical}/{}", r.user_id);
                    if let Some(row) = tx
                        .query_opt(
                            "SELECT value FROM nextral_runtime_state WHERE tenant_id=$1 AND key=$2",
                            &[&r.tenant_id, &key],
                        )
                        .map_err(db)?
                    {
                        let route: Value = row.get(0);
                        if r.embedding_dim.map(u64::from) != route["dimension"].as_u64()
                            || r.embedding_model.as_deref() != route["model"].as_str()
                            || r.embedding_provider.as_deref() != route["provider"].as_str()
                        {
                            return Err(CoreError::Conflict(
                                "embedding route changed; supply vector for active route".into(),
                            ));
                        }
                    }
                }
                Self::memory_write(tx, r)?;
                Self::audit_write(tx, audit)?;
                let plan = Self::graph_plan(tx, r, payload)?;
                Self::outbox_write(tx, &r.tenant_id, "memory.sync", &r.id, &plan)
            })
        }
        pub fn query_memories(&self, request: &crate::memory::MemoryQuery) -> CoreResult<crate::memory::MemoryPage> {
            let mut req = request.clone();
            req.validate()?;

            let effective_privacy = if req.privacy_scope.is_empty() {
                vec![
                    crate::memory::PrivacyLevel::Private,
                    crate::memory::PrivacyLevel::Shared,
                    crate::memory::PrivacyLevel::Sensitive,
                ]
            } else {
                req.privacy_scope.clone()
            };

            let fingerprint = crate::memory::compute_query_fingerprint(
                &req.tenant_id,
                &req.user_id,
                &effective_privacy,
                &req.filters,
            );

            let after = if let Some(ref c) = req.cursor {
                Some(crate::memory::decode_cursor(c, &fingerprint)?)
            } else {
                None
            };

            self.scoped(&req.tenant_id, &req.user_id, |tx| {
                let rows = tx.query(
                    r#"SELECT m.id, m.session_id, m.memory_type, m.content_type, m.source_type, m.privacy_level,
                            CASE
                              WHEN m.status = 'active' AND EXISTS (SELECT 1 FROM nextral_runtime_state s WHERE s.tenant_id = m.tenant_id AND s.key LIKE 'forgotten-source/' || m.user_id || '/%' AND m.source_message_ids ? substring(s.key from 'forgotten-source/[^/]+/(.*)'))
                                THEN 'soft_deleted'
                              ELSE m.status
                            END AS effective_status,
                            to_char(m.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
                            to_char(m.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
                            CASE
                              WHEN EXISTS (SELECT 1 FROM nextral_runtime_state s WHERE s.tenant_id = m.tenant_id AND s.key LIKE 'forgotten-source/' || m.user_id || '/%' AND s.value->>'status' = 'redacted' AND m.source_message_ids ? substring(s.key from 'forgotten-source/[^/]+/(.*)'))
                                THEN '[redacted]'
                              ELSE left(m.content, 256)
                            END AS content_preview,
                            m.tags, m.entities
                     FROM nextral_memories m
                     WHERE m.tenant_id = $1 AND m.user_id = $2
                     ORDER BY m.created_at DESC, m.id DESC"#,
                    &[&req.tenant_id, &req.user_id],
                ).map_err(db)?;

                let mut candidates = Vec::new();
                for r in rows {
                    let id: String = r.get(0);
                    let session_id: Option<String> = r.get(1);
                    let memory_type: String = r.get(2);
                    let content_type: String = r.get(3);
                    let source_type: String = r.get(4);
                    let privacy_level_str: String = r.get(5);
                    let effective_status_str: String = r.get(6);
                    let created_at: String = r.get(7);
                    let updated_at: String = r.get(8);
                    let content_preview: String = r.get(9);
                    let tags: Vec<String> = serde_json::from_value(r.get(10)).unwrap_or_default();
                    let entities: Vec<String> = serde_json::from_value(r.get(11)).unwrap_or_default();

                    let privacy_level: crate::memory::PrivacyLevel = serde_json::from_value(serde_json::Value::String(privacy_level_str))
                        .map_err(|e| CoreError::Serialization(e.to_string()))?;
                    let status: crate::memory::MemoryStatus = serde_json::from_value(serde_json::Value::String(effective_status_str))
                        .map_err(|e| CoreError::Serialization(e.to_string()))?;

                    if !effective_privacy.contains(&privacy_level) {
                        continue;
                    }

                    if let Some(ref types) = req.filters.memory_types {
                        let parsed_mt: Result<crate::memory::MemoryType, _> = serde_json::from_value(serde_json::Value::String(memory_type.clone()));
                        if let Ok(ref mt) = parsed_mt {
                            if !types.is_empty() && !types.contains(mt) {
                                continue;
                            }
                        }
                    }
                    if let Some(ref types) = req.filters.content_types {
                        let parsed_ct: Result<crate::memory::ContentType, _> = serde_json::from_value(serde_json::Value::String(content_type.clone()));
                        if let Ok(ref ct) = parsed_ct {
                            if !types.is_empty() && !types.contains(ct) {
                                continue;
                            }
                        }
                    }
                    if let Some(ref types) = req.filters.source_types {
                        let parsed_st: Result<crate::memory::SourceType, _> = serde_json::from_value(serde_json::Value::String(source_type.clone()));
                        if let Ok(ref st) = parsed_st {
                            if !types.is_empty() && !types.contains(st) {
                                continue;
                            }
                        }
                    }
                    if let Some(ref sid) = req.filters.session_id {
                        if session_id.as_deref() != Some(sid.as_str()) {
                            continue;
                        }
                    }
                    if let Some(ref tags_any) = req.filters.tags_any {
                        if !tags_any.is_empty() && !tags_any.iter().any(|t| tags.contains(t)) {
                            continue;
                        }
                    }
                    if let Some(ref tags_all) = req.filters.tags_all {
                        if !tags_all.iter().all(|t| tags.contains(t)) {
                            continue;
                        }
                    }
                    if let Some(ref entities_any) = req.filters.entities_any {
                        if !entities_any.is_empty() && !entities_any.iter().any(|e| entities.contains(e)) {
                            continue;
                        }
                    }
                    if let Some(ref entities_all) = req.filters.entities_all {
                        if !entities_all.iter().all(|e| entities.contains(e)) {
                            continue;
                        }
                    }
                    if let Some(ref gte) = req.filters.created_at_gte {
                        if created_at.as_str() < gte.as_str() {
                            continue;
                        }
                    }
                    if let Some(ref lt) = req.filters.created_at_lt {
                        if created_at.as_str() >= lt.as_str() {
                            continue;
                        }
                    }

                    let effective_statuses = req.filters.statuses.as_ref().filter(|s| !s.is_empty());
                    let status_matched = match effective_statuses {
                        Some(statuses) => statuses.contains(&status),
                        None => status == crate::memory::MemoryStatus::Active,
                    };
                    if !status_matched {
                        continue;
                    }

                    let mem_summary = crate::memory::MemorySummary {
                        id,
                        tenant_id: req.tenant_id.clone(),
                        user_id: req.user_id.clone(),
                        session_id,
                        memory_type: serde_json::from_value(serde_json::Value::String(memory_type)).unwrap_or(crate::memory::MemoryType::Semantic),
                        content_type: serde_json::from_value(serde_json::Value::String(content_type)).unwrap_or(crate::memory::ContentType::Fact),
                        source_type: serde_json::from_value(serde_json::Value::String(source_type)).unwrap_or(crate::memory::SourceType::Manual),
                        privacy_level,
                        status,
                        importance_score: 0.0,
                        confidence_score: None,
                        created_at,
                        updated_at,
                        preview: content_preview.clone(),
                        content_preview,
                    };
                    candidates.push(mem_summary);
                }

                if let Some((after_created_at, after_id)) = after {
                    candidates.retain(|r| {
                        r.created_at < after_created_at || (r.created_at == after_created_at && r.id < after_id)
                    });
                }

                let has_next = candidates.len() > req.limit;
                let items: Vec<_> = candidates.into_iter().take(req.limit).collect();
                let next_cursor = if has_next {
                    if let Some(last) = items.last() {
                        Some(crate::memory::encode_cursor(&last.created_at, &last.id, &fingerprint))
                    } else {
                        None
                    }
                } else {
                    None
                };

                Ok(crate::memory::MemoryPage { items, next_cursor })
            })
        }
        
        pub fn list_memories(&self, s: &TenantUserScope) -> CoreResult<Vec<MemoryRecord>> {
            self.scoped(&s.tenant_id,&s.user_id,|tx|tx.query("SELECT record_json FROM nextral_memories WHERE tenant_id=$1 AND user_id=$2 ORDER BY created_at,id",&[&s.tenant_id,&s.user_id]).map_err(db)?.into_iter().map(|r|serde_json::from_value(r.get(0)).map_err(CoreError::from)).collect())
        }
        pub fn state_load(&self, tenant: &str, key: &str) -> CoreResult<Option<Value>> {
            self.scoped(tenant, "", |tx| {
                Ok(tx
                    .query_opt(
                        "SELECT value FROM nextral_runtime_state WHERE tenant_id=$1 AND key=$2",
                        &[&tenant, &key],
                    )
                    .map_err(db)?
                    .map(|r| r.get(0)))
            })
        }
        pub fn state_save(&self, tenant: &str, key: &str, value: &Value) -> CoreResult<()> {
            self.scoped(tenant,"",|tx|{tx.execute("INSERT INTO nextral_runtime_state(tenant_id,key,value) VALUES($1,$2,$3) ON CONFLICT(tenant_id,key) DO UPDATE SET value=excluded.value,updated_at=now()",&[&tenant,&key,&value]).map_err(db)?;Ok(())})
        }
        pub fn state_save_claimed(
            &self,
            tenant: &str,
            key: &str,
            owner: &str,
            value: &Value,
        ) -> CoreResult<Option<Value>> {
            self.scoped(tenant, "", |tx| {
                let row = tx.query_opt(
                    "SELECT value,locked_by=$3 AND locked_until>clock_timestamp() FROM nextral_runtime_state WHERE tenant_id=$1 AND key=$2 FOR UPDATE",
                    &[&tenant, &key, &owner],
                ).map_err(db)?.ok_or_else(|| CoreError::NotFound("scheduled job state missing".into()))?;
                if row.get::<_, Option<bool>>(1) != Some(true) {
                    return Err(CoreError::Conflict("scheduled job claim was lost".into()));
                }
                let current: Value = row.get(0);
                if matches!(current["status"].as_str(), Some("completed" | "terminal_failed" | "cancelled"))
                    || current.get("request") != value.get("request")
                {
                    return Ok(Some(current));
                }
                tx.execute(
                    "UPDATE nextral_runtime_state SET value=$3,updated_at=now() WHERE tenant_id=$1 AND key=$2",
                    &[&tenant, &key, &value],
                ).map_err(db)?;
                Ok(None)
            })
        }
        pub fn state_list(&self, tenant: &str, prefix: &str) -> CoreResult<Vec<(String, Value)>> {
            self.scoped(tenant,"",|tx|Ok(tx.query("SELECT key,value FROM nextral_runtime_state WHERE tenant_id=$1 AND starts_with(key,$2)",&[&tenant,&prefix]).map_err(db)?.into_iter().map(|r|(r.get(0),r.get(1))).collect()))
        }
        fn bump_policy_version(
            tx: &mut Transaction<'_>,
            s: &TenantUserScope,
            key: &str,
            operation: &str,
        ) -> CoreResult<()> {
            let version_key = format!("policy-version/{}", s.user_id);
            let previous = tx
                .query_opt(
                    "SELECT value FROM nextral_runtime_state WHERE tenant_id=$1 AND key=$2",
                    &[&s.tenant_id, &version_key],
                )
                .map_err(db)?
                .map(|r| r.get::<_, Value>(0).to_string())
                .unwrap_or_default();
            let version = deterministic_id(&[&previous, key, operation]);
            let value = json!(version);
            tx.execute("INSERT INTO nextral_runtime_state(tenant_id,key,value) VALUES($1,$2,$3) ON CONFLICT(tenant_id,key) DO UPDATE SET value=excluded.value,updated_at=now()",&[&s.tenant_id,&version_key,&value]).map_err(db)?;
            Self::outbox_write(
                tx,
                &s.tenant_id,
                "policy.invalidate",
                &s.user_id,
                &json!({"tenant_id":s.tenant_id,"user_id":s.user_id,"version":version}),
            )?;
            Ok(())
        }
        pub fn policy_mutate(
            &self,
            s: &TenantUserScope,
            key: &str,
            value: Option<&Value>,
            audit: &AuditWrite,
        ) -> CoreResult<Option<Value>> {
            self.scoped(&s.tenant_id,&s.user_id,|tx|{
   tx.query_one("SELECT pg_advisory_xact_lock(hashtextextended($1,0))",&[&s.tenant_id]).map_err(db)?;
   let previous=tx.query_opt("SELECT value FROM nextral_runtime_state WHERE tenant_id=$1 AND key=$2",&[&s.tenant_id,&key]).map_err(db)?.map(|r|r.get::<_,Value>(0));
   let mut canonical=value.cloned();
   if let Some(value)=&mut canonical{
    if let Some(previous)=previous{value["created_at"]=previous["created_at"].clone();}
    tx.execute("INSERT INTO nextral_runtime_state(tenant_id,key,value) VALUES($1,$2,$3) ON CONFLICT(tenant_id,key) DO UPDATE SET value=excluded.value,updated_at=now()",&[&s.tenant_id,&key,&*value]).map_err(db)?;
   }else{
    if previous.is_none(){return Err(CoreError::NotFound("procedural policy not found".into()));}
    tx.execute("DELETE FROM nextral_runtime_state WHERE tenant_id=$1 AND key=$2",&[&s.tenant_id,&key]).map_err(db)?;
   }
   Self::audit_write(tx,audit)?;
   Self::bump_policy_version(tx,s,key,&canonical.as_ref().map(Value::to_string).unwrap_or_else(||"remove".into()))?;
   Ok(canonical)
  })
        }
        pub fn claim(
            &self,
            tenant: &str,
            key: &str,
            owner: &str,
            now: u64,
            lease: u64,
        ) -> CoreResult<bool> {
            self.scoped(tenant,"",|tx|Ok(tx.execute("INSERT INTO nextral_runtime_state(tenant_id,key,value,locked_by,locked_until) VALUES($1,$2,'{}',$3,to_timestamp($4)) ON CONFLICT(tenant_id,key) DO UPDATE SET locked_by=excluded.locked_by,locked_until=excluded.locked_until WHERE nextral_runtime_state.locked_until IS NULL OR nextral_runtime_state.locked_until<=to_timestamp($5) OR nextral_runtime_state.locked_by=$3",&[&tenant,&key,&owner,&((now.saturating_add(lease)) as f64),&(now as f64)]).map_err(db)?>0))
        }
        pub fn release(&self, tenant: &str, key: &str, owner: &str) -> CoreResult<()> {
            self.scoped(tenant,"",|tx|{tx.execute("UPDATE nextral_runtime_state SET locked_by=NULL,locked_until=NULL WHERE tenant_id=$1 AND key=$2 AND locked_by=$3",&[&tenant,&key,&owner]).map_err(db)?;Ok(())})
        }
        pub fn append_message(&self, m: &SessionMessage) -> CoreResult<()> {
            self.scoped(&m.tenant_id,&m.user_id,|tx|{
   let value=serde_json::to_value(m)?;let epoch=timestamp_seconds(&m.created_at)?;
   let rows=tx.execute("INSERT INTO nextral_session_messages(id,tenant_id,user_id,session_id,role,content,source_metadata,created_at,record_json,idempotency_key) VALUES($1,$2,$3,$4,$5,$6,$7,to_timestamp($8),$9,$10) ON CONFLICT(tenant_id,user_id,session_id,idempotency_key) DO UPDATE SET id=nextral_session_messages.id WHERE nextral_session_messages.content=excluded.content AND nextral_session_messages.role=excluded.role",&[&m.id,&m.tenant_id,&m.user_id,&m.session_id,&m.role,&m.content,&m.source_metadata,&epoch,&value,&m.idempotency_key]).map_err(db)?;
   if rows==0{return Err(CoreError::Conflict("session idempotency key reused for different message".into()));}Ok(())
  })
        }
        pub fn session_tail(
            &self,
            s: &TenantUserScope,
            session: &str,
            limit: usize,
        ) -> CoreResult<Vec<SessionMessage>> {
            self.scoped(&s.tenant_id,&s.user_id,|tx|{let mut messages:Vec<SessionMessage>=tx.query("SELECT record_json FROM nextral_session_messages WHERE tenant_id=$1 AND user_id=$2 AND session_id=$3 ORDER BY sequence DESC LIMIT $4",&[&s.tenant_id,&s.user_id,&session,&(limit.min(100_001) as i64)]).map_err(db)?.into_iter().map(|r|serde_json::from_value(r.get(0)).map_err(CoreError::from)).collect::<CoreResult<_>>()?;messages.reverse();Ok(messages)})
        }
        pub fn session_head(
            &self,
            s: &TenantUserScope,
            session: &str,
        ) -> CoreResult<(Option<String>, usize)> {
            self.scoped(&s.tenant_id,&s.user_id,|tx|{let rows=tx.query("SELECT id FROM nextral_session_messages WHERE tenant_id=$1 AND user_id=$2 AND session_id=$3 ORDER BY sequence DESC LIMIT 20",&[&s.tenant_id,&s.user_id,&session]).map_err(db)?;Ok((rows.first().map(|r|r.get(0)),rows.len()))})
        }
        pub fn reminders(&self, s: &TenantUserScope) -> CoreResult<Vec<ReminderRecord>> {
            self.scoped(&s.tenant_id,&s.user_id,|tx|tx.query("SELECT record_json FROM nextral_reminders WHERE tenant_id=$1 AND user_id=$2 ORDER BY due_at,id",&[&s.tenant_id,&s.user_id]).map_err(db)?.into_iter().map(|r|serde_json::from_value(r.get(0)).map_err(CoreError::from)).collect())
        }
        pub fn claim_outbox(
            &self,
            tenant: &str,
            owner: &str,
        ) -> CoreResult<Vec<(String, String, Value)>> {
            self.scoped(tenant,"",|tx|Ok(tx.query(
   "UPDATE nextral_outbox_events SET status='processing',locked_by=$2,locked_until=now()+interval '300 seconds',attempt_count=attempt_count+1 WHERE id IN (SELECT e.id FROM nextral_outbox_events e WHERE e.tenant_id=$1 AND e.status IN ('pending','retry','processing') AND (e.locked_until IS NULL OR e.locked_until<now()) AND (e.next_attempt_at IS NULL OR e.next_attempt_at<=now()) AND NOT EXISTS(SELECT 1 FROM nextral_runtime_state s WHERE s.tenant_id=e.tenant_id AND s.key='privacy/'||(e.payload->'record'->>'user_id') AND s.locked_until>now()) ORDER BY e.created_at FOR UPDATE SKIP LOCKED LIMIT 1) RETURNING id,event_type,payload",
   &[&tenant,&owner]).map_err(db)?.into_iter().map(|r|(r.get(0),r.get(1),r.get(2))).collect()))
        }
        pub fn renew_outbox(&self, tenant: &str, id: &str, owner: &str) -> CoreResult<bool> {
            self.scoped(tenant,"",|tx|Ok(tx.execute("UPDATE nextral_outbox_events SET locked_until=now()+interval '300 seconds' WHERE tenant_id=$1 AND id=$2 AND locked_by=$3 AND locked_until>now()",&[&tenant,&id,&owner]).map_err(db)?>0))
        }
        pub fn finish_outbox(
            &self,
            tenant: &str,
            id: &str,
            owner: &str,
            error: Option<&str>,
        ) -> CoreResult<()> {
            let(status,age)=self.scoped(tenant,"",|tx|{
   let row=tx.query_opt("UPDATE nextral_outbox_events SET status=CASE WHEN $4::text IS NULL THEN 'completed' WHEN attempt_count>=10 THEN 'failed' ELSE 'retry' END,last_error=$4,locked_by=NULL,locked_until=NULL,next_attempt_at=CASE WHEN $4::text IS NULL THEN NULL ELSE now()+interval '30 seconds'*least(attempt_count,10) END,updated_at=now() WHERE tenant_id=$1 AND id=$2 AND locked_by=$3 RETURNING status,extract(epoch from (now()-created_at))::bigint",&[&tenant,&id,&owner,&error]).map_err(db)?.ok_or_else(||CoreError::Conflict("outbox claim was lost".into()))?;
   Ok((row.get::<_,String>(0),row.get::<_,i64>(1).max(0) as u64))
  })?;
            use crate::runtime::observability::{record_job, JobOutcome};
            record_job(
                match status.as_str() {
                    "completed" => JobOutcome::Completed,
                    "failed" => JobOutcome::Failed,
                    _ => JobOutcome::Retried,
                },
                age,
            );
            Ok(())
        }
        pub fn claim_fenced(
            &self,
            tenant: &str,
            user: Option<&str>,
            resource: crate::runtime::lifecycle::LeaseResource,
            key: &str,
            owner: &str,
            lease_seconds: u64,
        ) -> CoreResult<Option<crate::runtime::lifecycle::LeaseClaim>> {
            self.scoped(tenant, user.unwrap_or(""), |tx| {
                let rows = tx.query(
                    "INSERT INTO nextral_runtime_state (tenant_id, key, value, locked_by, locked_until, lease_fence, updated_at) VALUES ($1, $2, '{}'::jsonb, $3, clock_timestamp() + ($4 || ' seconds')::interval, 1, clock_timestamp()) ON CONFLICT (tenant_id, key) DO UPDATE SET locked_by = $3, locked_until = clock_timestamp() + ($4 || ' seconds')::interval, lease_fence = nextral_runtime_state.lease_fence + 1, updated_at = clock_timestamp() WHERE nextral_runtime_state.locked_until IS NULL OR nextral_runtime_state.locked_until < clock_timestamp() OR nextral_runtime_state.locked_by = $3 RETURNING lease_fence",
                    &[&tenant, &key, &owner, &(lease_seconds as i64)],
                ).map_err(db)?;
                if let Some(row) = rows.first() {
                    let fence: i64 = row.get(0);
                    Ok(Some(crate::runtime::lifecycle::LeaseClaim {
                        tenant_id: tenant.to_string(),
                        user_id: user.map(str::to_string),
                        resource,
                        key: key.to_string(),
                        owner: owner.to_string(),
                        fence: fence as u64,
                    }))
                } else {
                    Ok(None)
                }
            })
        }

        pub fn renew_fenced(
            &self,
            claim: &crate::runtime::lifecycle::LeaseClaim,
            lease_seconds: u64,
        ) -> CoreResult<bool> {
            self.scoped(&claim.tenant_id, claim.user_id.as_deref().unwrap_or(""), |tx| {
                let rows = tx.execute(
                    "UPDATE nextral_runtime_state SET locked_until = clock_timestamp() + ($4 || ' seconds')::interval, updated_at = clock_timestamp() WHERE tenant_id = $1 AND key = $2 AND locked_by = $3 AND lease_fence = $5 AND locked_until > clock_timestamp()",
                    &[&claim.tenant_id, &claim.key, &claim.owner, &(lease_seconds as i64), &(claim.fence as i64)],
                ).map_err(db)?;
                Ok(rows > 0)
            })
        }

        pub fn release_fenced(
            &self,
            claim: &crate::runtime::lifecycle::LeaseClaim,
        ) -> CoreResult<()> {
            self.scoped(&claim.tenant_id, claim.user_id.as_deref().unwrap_or(""), |tx| {
                tx.execute(
                    "UPDATE nextral_runtime_state SET locked_by = NULL, locked_until = NULL, updated_at = clock_timestamp() WHERE tenant_id = $1 AND key = $2 AND locked_by = $3 AND lease_fence = $4",
                    &[&claim.tenant_id, &claim.key, &claim.owner, &(claim.fence as i64)],
                ).map_err(db)?;
                Ok(())
            })
        }

        pub fn save_guarded(
            &self,
            claim: &crate::runtime::lifecycle::LeaseClaim,
            target_key: &str,
            value: &Value,
        ) -> CoreResult<crate::runtime::lifecycle::GuardedWriteResult> {
            self.scoped(&claim.tenant_id, claim.user_id.as_deref().unwrap_or(""), |tx| {
                let lease_row = tx.query_opt(
                    "SELECT locked_by, lease_fence, (locked_until IS NOT NULL AND locked_until > clock_timestamp()) FROM nextral_runtime_state WHERE tenant_id = $1 AND key = $2 FOR UPDATE",
                    &[&claim.tenant_id, &claim.key],
                ).map_err(db)?.ok_or_else(|| CoreError::LeaseLost("lease row not found".into()))?;

                let locked_by: Option<String> = lease_row.get(0);
                let fence: i64 = lease_row.get(1);
                let valid: bool = lease_row.get(2);

                if locked_by.as_deref() != Some(&claim.owner) || fence as u64 != claim.fence {
                    return Err(CoreError::LeaseLost("lease fence or owner mismatch".into()));
                }
                if !valid {
                    return Err(CoreError::LeaseLost("lease expired".into()));
                }

                let target_row = tx.query_opt(
                    "SELECT value FROM nextral_runtime_state WHERE tenant_id = $1 AND key = $2 FOR UPDATE",
                    &[&claim.tenant_id, &target_key],
                ).map_err(db)?;

                if let Some(row) = target_row {
                    let existing: Value = row.get(0);
                    if matches!(existing["status"].as_str(), Some("cancelled" | "completed" | "terminal_failed")) {
                        return Ok(crate::runtime::lifecycle::GuardedWriteResult::Superseded(existing));
                    }
                }

                tx.execute(
                    "INSERT INTO nextral_runtime_state (tenant_id, key, value, updated_at) VALUES ($1, $2, $3, clock_timestamp()) ON CONFLICT (tenant_id, key) DO UPDATE SET value = $3, updated_at = clock_timestamp()",
                    &[&claim.tenant_id, &target_key, value],
                ).map_err(db)?;

                Ok(crate::runtime::lifecycle::GuardedWriteResult::Applied)
            })
        }

        pub fn commit_reminder_delivery(
            &self,
            claim: &crate::runtime::lifecycle::LeaseClaim,
            state_key: &str,
            state: &Value,
            reminder: &ReminderRecord,
            audits: &[AuditEvent],
        ) -> CoreResult<()> {
            self.scoped(&reminder.tenant_id, &reminder.user_id, |tx| {
                let lease_row = tx.query_opt(
                    "SELECT locked_by, lease_fence, (locked_until IS NOT NULL AND locked_until > clock_timestamp()) FROM nextral_runtime_state WHERE tenant_id=$1 AND key=$2 FOR UPDATE",
                    &[&claim.tenant_id, &claim.key],
                ).map_err(db)?.ok_or_else(|| CoreError::LeaseLost("lease row not found".into()))?;

                let locked_by: Option<String> = lease_row.get(0);
                let lease_fence: i64 = lease_row.get(1);
                let valid: bool = lease_row.get(2);

                if locked_by.as_deref() != Some(&claim.owner) || lease_fence as u64 != claim.fence {
                    return Err(CoreError::LeaseLost("lease fence or owner mismatch".into()));
                }
                if !valid {
                    return Err(CoreError::LeaseLost("lease expired".into()));
                }

                tx.execute(
                    "INSERT INTO nextral_runtime_state(tenant_id, key, value, updated_at) VALUES($1, $2, $3, clock_timestamp()) ON CONFLICT (tenant_id, key) DO UPDATE SET value=$3, updated_at=clock_timestamp()",
                    &[&reminder.tenant_id, &state_key, state],
                ).map_err(db)?;

                let reminder_json = serde_json::to_value(reminder)?;
                let due = timestamp_seconds(&reminder.due_at)?;
                let last = reminder.last_attempt_at.as_deref().map(timestamp_seconds).transpose()?;
                let next = reminder.next_attempt_at.as_deref().map(timestamp_seconds).transpose()?;
                let terminal = reminder.terminal_reason.as_ref().map(text).transpose()?;
                tx.execute(
                    "INSERT INTO nextral_reminders(id, tenant_id, user_id, source_memory_id, kind, title, details, due_at, timezone, priority, status, attempt_count, last_attempt_at, next_attempt_at, dedupe_key, created_at, updated_at, channel, allowed_channels, quiet_hours, expires_at, privacy_level, terminal_reason, last_error, record_json) VALUES ($1,$2,$3,$4,$5,$6,$7,to_timestamp($8),$9,$10,$11,$12,to_timestamp($13),to_timestamp($14),$15,clock_timestamp(),clock_timestamp(),$16,$17,$18,to_timestamp($19),$20,$21,$22,$23) ON CONFLICT(tenant_id, id) DO UPDATE SET details=$7, due_at=to_timestamp($8), status=$11, attempt_count=$12, last_attempt_at=to_timestamp($13), next_attempt_at=to_timestamp($14), updated_at=clock_timestamp(), terminal_reason=$21, last_error=$22, record_json=$23",
                    &[
                        &reminder.id, &reminder.tenant_id, &reminder.user_id, &reminder.source_memory_id,
                        &text(&reminder.kind)?, &reminder.title, &reminder.details, &due,
                        &reminder.timezone, &text(&reminder.priority)?, &text(&reminder.status)?,
                        &(reminder.attempt_count as i32), &last, &next, &reminder.dedupe_key,
                        &reminder.channel, &reminder.allowed_channels, &reminder.quiet_hours.as_ref().map(serde_json::to_value).transpose()?,
                        &reminder.expires_at.as_deref().map(timestamp_seconds).transpose()?,
                        &text(&reminder.privacy_level)?, &terminal, &reminder.last_error, &reminder_json,
                    ],
                ).map_err(db)?;

                for audit in audits {
                    Self::audit_write(tx, &audit_write(audit)?)?;
                }
                Ok(())
            })
        }

        pub fn job_enqueue(&self, job: &crate::runtime::prospective::LifecycleScheduledJob) -> CoreResult<()> {
            self.scoped(&job.request.tenant_id, &job.request.user_id, |tx| {
                let req_json = serde_json::to_value(&job.request)?;
                let next_due = timestamp_seconds(&job.next_attempt_at)?;
                tx.execute(
                    "INSERT INTO nextral_lifecycle_jobs (tenant_id, user_id, id, request, progress, status, next_attempt_at, attempts, updated_at, lease_fence) VALUES ($1, $2, $3, $4, '{}'::jsonb, $5, to_timestamp($6), $7, clock_timestamp(), 0) ON CONFLICT (tenant_id, user_id, id) DO NOTHING",
                    &[&job.request.tenant_id, &job.request.user_id, &job.id, &req_json, &job.status, &next_due, &(job.attempts as i32)],
                ).map_err(db)?;
                Ok(())
            })
        }

        pub fn job_get(&self, tenant: &str, user: &str, id: &str) -> CoreResult<Option<crate::runtime::prospective::LifecycleScheduledJob>> {
            self.scoped(tenant, user, |tx| {
                let row = tx.query_opt(
                    "SELECT id, request, status, attempts, extract(epoch from next_attempt_at)::bigint, extract(epoch from created_at)::bigint, extract(epoch from updated_at)::bigint, completed_memory_ids, last_error FROM nextral_lifecycle_jobs WHERE tenant_id=$1 AND user_id=$2 AND id=$3",
                    &[&tenant, &user, &id],
                ).map_err(db)?;
                if let Some(r) = row {
                    let req: crate::runtime::prospective::LifecycleJobRequest = serde_json::from_value(r.get(1))?;
                    let due: i64 = r.get(4);
                    let created: i64 = r.get(5);
                    let updated: i64 = r.get(6);
                    Ok(Some(crate::runtime::prospective::LifecycleScheduledJob {
                        id: r.get(0),
                        request: req,
                        status: r.get(2),
                        attempts: r.get::<_, i32>(3) as u32,
                        next_attempt_at: due.to_string(),
                        created_at: created.to_string(),
                        updated_at: updated.to_string(),
                        completed_memory_ids: r.get::<_, Option<Vec<String>>>(7).unwrap_or_default(),
                        last_error: r.get(8),
                    }))
                } else {
                    Ok(None)
                }
            })
        }

        pub fn job_list(&self, tenant: &str, user: &str) -> CoreResult<Vec<crate::runtime::prospective::LifecycleScheduledJob>> {
            self.scoped(tenant, user, |tx| {
                let rows = tx.query(
                    "SELECT id, request, status, attempts, extract(epoch from next_attempt_at)::bigint, extract(epoch from created_at)::bigint, extract(epoch from updated_at)::bigint, completed_memory_ids, last_error FROM nextral_lifecycle_jobs WHERE tenant_id=$1 AND user_id=$2 ORDER BY next_attempt_at ASC, id ASC",
                    &[&tenant, &user],
                ).map_err(db)?;
                let mut jobs = Vec::new();
                for r in rows {
                    let req: crate::runtime::prospective::LifecycleJobRequest = serde_json::from_value(r.get(1))?;
                    let due: i64 = r.get(4);
                    let created: i64 = r.get(5);
                    let updated: i64 = r.get(6);
                    jobs.push(crate::runtime::prospective::LifecycleScheduledJob {
                        id: r.get(0),
                        request: req,
                        status: r.get(2),
                        attempts: r.get::<_, i32>(3) as u32,
                        next_attempt_at: due.to_string(),
                        created_at: created.to_string(),
                        updated_at: updated.to_string(),
                        completed_memory_ids: r.get::<_, Option<Vec<String>>>(7).unwrap_or_default(),
                        last_error: r.get(8),
                    });
                }
                Ok(jobs)
            })
        }

        pub fn job_claim_due(
            &self,
            tenant: &str,
            user: &str,
            owner: &str,
            now_nanos: u128,
            limit: usize,
            lease_seconds: u64,
        ) -> CoreResult<Vec<(crate::runtime::prospective::LifecycleScheduledJob, crate::runtime::lifecycle::LeaseClaim)>> {
            let due_epoch = (now_nanos / 1_000_000_000) as f64;
            self.scoped(tenant, user, |tx| {
                let rows = tx.query(
                    "UPDATE nextral_lifecycle_jobs SET status='running', locked_by=$3, locked_until=clock_timestamp() + ($4 || ' seconds')::interval, lease_fence=nextral_lifecycle_jobs.lease_fence + 1, updated_at=clock_timestamp() WHERE (tenant_id, user_id, id) IN (SELECT tenant_id, user_id, id FROM nextral_lifecycle_jobs WHERE tenant_id=$1 AND user_id=$2 AND status IN ('queued', 'running', 'retry_pending') AND (locked_until IS NULL OR locked_until < clock_timestamp()) AND next_attempt_at <= to_timestamp($5) ORDER BY next_attempt_at ASC, id ASC FOR UPDATE SKIP LOCKED LIMIT $6) RETURNING id, request, status, attempts, extract(epoch from next_attempt_at)::bigint, extract(epoch from created_at)::bigint, extract(epoch from updated_at)::bigint, completed_memory_ids, last_error, lease_fence",
                    &[&tenant, &user, &owner, &(lease_seconds as i64), &due_epoch, &(limit as i64)],
                ).map_err(db)?;

                let mut results = Vec::new();
                for r in rows {
                    let id: String = r.get(0);
                    let req: crate::runtime::prospective::LifecycleJobRequest = serde_json::from_value(r.get(1))?;
                    let due: i64 = r.get(4);
                    let created: i64 = r.get(5);
                    let updated: i64 = r.get(6);
                    let fence: i64 = r.get(9);
                    let job = crate::runtime::prospective::LifecycleScheduledJob {
                        id: id.clone(),
                        request: req,
                        status: r.get(2),
                        attempts: r.get::<_, i32>(3) as u32,
                        next_attempt_at: due.to_string(),
                        created_at: created.to_string(),
                        updated_at: updated.to_string(),
                        completed_memory_ids: r.get::<_, Option<Vec<String>>>(7).unwrap_or_default(),
                        last_error: r.get(8),
                    };
                    let claim = crate::runtime::lifecycle::LeaseClaim {
                        tenant_id: tenant.to_string(),
                        user_id: Some(user.to_string()),
                        resource: crate::runtime::lifecycle::LeaseResource::LifecycleJob,
                        key: id,
                        owner: owner.to_string(),
                        fence: fence as u64,
                    };
                    results.push((job, claim));
                }
                Ok(results)
            })
        }

        pub fn job_save_guarded(
            &self,
            claim: &crate::runtime::lifecycle::LeaseClaim,
            job: &crate::runtime::prospective::LifecycleScheduledJob,
        ) -> CoreResult<()> {
            self.scoped(&claim.tenant_id, claim.user_id.as_deref().unwrap_or(""), |tx| {
                let due = timestamp_seconds(&job.next_attempt_at)?;
                let rows = tx.execute(
                    "UPDATE nextral_lifecycle_jobs SET status=$4, next_attempt_at=to_timestamp($5), attempts=$6, updated_at=clock_timestamp(), completed_memory_ids=$7, last_error=$8, locked_by=CASE WHEN $4 IN ('completed', 'terminal_failed', 'cancelled', 'retry_pending', 'queued') THEN NULL ELSE locked_by END, locked_until=CASE WHEN $4 IN ('completed', 'terminal_failed', 'cancelled', 'retry_pending', 'queued') THEN NULL ELSE locked_until END WHERE tenant_id=$1 AND user_id=$2 AND id=$3 AND locked_by=$9 AND lease_fence=$10 AND locked_until > clock_timestamp()",
                    &[
                        &claim.tenant_id, &claim.user_id.as_deref().unwrap_or(""), &claim.key,
                        &job.status, &due, &(job.attempts as i32), &job.completed_memory_ids,
                        &job.last_error, &claim.owner, &(claim.fence as i64),
                    ],
                ).map_err(db)?;
                if rows == 0 {
                    return Err(CoreError::LeaseLost("lifecycle job lease expired or lost".into()));
                }
                Ok(())
            })
        }
        pub fn cutover(
            &self,
            tenant: &str,
            user: &str,
            logical: &str,
            target: &str,
        ) -> CoreResult<()> {
            self.scoped(tenant,user,|tx|{
   tx.query_one("SELECT pg_advisory_xact_lock(hashtextextended($1,0))",&[&tenant]).map_err(db)?;
   let prefix=format!("collection-record/{target}/{user}/");
   let staged=tx.query("SELECT value FROM nextral_runtime_state WHERE tenant_id=$1 AND starts_with(key,$2)",&[&tenant,&prefix]).map_err(db)?.into_iter().map(|r|serde_json::from_value::<MemoryRecord>(r.get(0)).map_err(CoreError::from)).collect::<CoreResult<Vec<_>>>()?;
   let first=staged.first().ok_or_else(||CoreError::Conflict("cutover has no staged source snapshot".into()))?;
   if staged.iter().any(|r|r.tenant_id!=tenant||r.user_id!=user){return Err(CoreError::Conflict("cutover snapshot scope mismatch".into()));}
   let current=tx.query("SELECT record_json FROM nextral_memories WHERE tenant_id=$1 AND user_id=$2 AND status='active' ORDER BY id FOR UPDATE",&[&tenant,&user]).map_err(db)?.into_iter().map(|r|serde_json::from_value::<MemoryRecord>(r.get(0)).map_err(CoreError::from)).collect::<CoreResult<Vec<_>>>()?;
   if current.len()!=staged.len(){return Err(CoreError::Conflict("active source set changed during backfill".into()));}
   for r in current {
    let next=staged.iter().find(|n|n.id==r.id).ok_or_else(||CoreError::Conflict("source IDs changed during backfill".into()))?;
    if r.content!=next.content||r.source_message_ids!=next.source_message_ids||r.privacy_level!=next.privacy_level||r.updated_at!=next.updated_at||r.entities!=next.entities||r.tags!=next.tags{return Err(CoreError::Conflict("source version changed during backfill".into()));}
    let mut updated=r;updated.embedding=next.embedding.clone();updated.embedding_provider=next.embedding_provider.clone();updated.embedding_model=next.embedding_model.clone();updated.embedding_dim=next.embedding_dim;
    Self::memory_write(tx,&updated)?;
   }
   let route=json!({"collection":target,"dimension":first.embedding_dim,"provider":first.embedding_provider,"model":first.embedding_model});
   let key=format!("alias/{logical}/{user}");
   tx.execute("INSERT INTO nextral_runtime_state(tenant_id,key,value) VALUES($1,$2,$3) ON CONFLICT(tenant_id,key) DO UPDATE SET value=excluded.value,updated_at=now()",&[&tenant,&key,&route]).map_err(db)?;
   Ok(())
  })
        }
        pub fn scrub_sources(&self, s: &TenantUserScope, memory: &MemoryRecord) -> CoreResult<()> {
            self.scoped(&s.tenant_id,&s.user_id,|tx|{
   tx.query_one("SELECT pg_advisory_xact_lock(hashtextextended($1,0))",&[&s.tenant_id]).map_err(db)?;
   tx.execute("DELETE FROM nextral_session_messages WHERE tenant_id=$1 AND user_id=$2 AND id=ANY($3)",&[&s.tenant_id,&s.user_id,&memory.source_message_ids]).map_err(db)?;
   tx.execute("DELETE FROM nextral_session_summaries WHERE tenant_id=$1 AND user_id=$2 AND session_id=$3",&[&s.tenant_id,&s.user_id,&memory.session_id]).map_err(db)?;
   let reminders=tx.query("SELECT record_json FROM nextral_reminders WHERE tenant_id=$1 AND user_id=$2 AND source_memory_id=$3",&[&s.tenant_id,&s.user_id,&memory.id]).map_err(db)?;
   for row in reminders{
    let mut reminder:ReminderRecord=serde_json::from_value(row.get(0))?;
    reminder.title="[redacted]".into();reminder.details.clear();
    if !matches!(reminder.status,crate::prospective::ReminderStatus::Completed|crate::prospective::ReminderStatus::Cancelled|crate::prospective::ReminderStatus::Expired){reminder.status=crate::prospective::ReminderStatus::Cancelled;reminder.terminal_reason=Some(crate::domain::reminder::ReminderTerminalReason::SourceUnavailable);}
    let payload=serde_json::to_value(&reminder)?;
    tx.execute("UPDATE nextral_reminders SET title=$4,details='',status=$5,record_json=$6,updated_at=now() WHERE tenant_id=$1 AND user_id=$2 AND id=$3",&[&s.tenant_id,&s.user_id,&reminder.id,&reminder.title,&text(&reminder.status)?,&payload]).map_err(db)?;
    Self::audit_write(tx,&AuditWrite{id:Some(deterministic_id(&[&memory.id,&reminder.id,"forget_reminder"])),tenant_id:s.tenant_id.clone(),user_id:Some(s.user_id.clone()),actor_id:s.user_id.clone(),action:"reminder_source_forgotten".into(),target_type:"reminder".into(),target_id:Some(reminder.id),reason:"source forgotten; delivery payload scrubbed".into(),request_id:None,trace_id:None,metadata_json:"{}".into()})?;
   }
   let states=tx.query("SELECT key,value FROM nextral_runtime_state WHERE tenant_id=$1 ORDER BY key FOR UPDATE",&[&s.tenant_id]).map_err(db)?;
   for row in states {
    let key:String=row.get(0);let value:Value=row.get(1);
    if crate::runtime::storage::lifecycle_state_references(&key,&value,memory) {
     match crate::runtime::storage::scrub_lifecycle_state(&key,value,memory){
      Some(replacement)=>{tx.execute("UPDATE nextral_runtime_state SET value=$3,updated_at=now() WHERE tenant_id=$1 AND key=$2",&[&s.tenant_id,&key,&replacement]).map_err(db)?;},
      None=>{tx.execute("DELETE FROM nextral_runtime_state WHERE tenant_id=$1 AND key=$2",&[&s.tenant_id,&key]).map_err(db)?;}
     }
    }
   }
   let payload=json!({"record":memory,"nodes":[],"edges":[]});
   tx.execute("UPDATE nextral_outbox_events SET payload=$3 WHERE tenant_id=$1 AND aggregate_id=$2 AND event_type IN ('memory.sync', 'graph.replay')",&[&s.tenant_id,&memory.id,&payload]).map_err(db)?;
   Ok(())
  })
        }
    }
    pub fn audit_write(event: &AuditEvent) -> CoreResult<AuditWrite> {
        Ok(AuditWrite {
            id: Some(event.id.clone()),
            tenant_id: event.tenant_id.clone(),
            user_id: event.user_id.clone(),
            actor_id: event.actor.clone(),
            action: text(&event.action)?,
            target_type: event.target_type.clone(),
            target_id: event.target_id.clone(),
            reason: event.reason.clone(),
            request_id: event.request_id.clone(),
            trace_id: event.trace_id.clone(),
            metadata_json: event.metadata_json.clone(),
        })
    }
    impl PostgresPort for PostgresAdapter {
        fn upsert_memory(&self, r: &MemoryRecord) -> CoreResult<()> {
            self.scoped(&r.tenant_id, &r.user_id, |tx| Self::memory_write(tx, r))
        }
        fn get_memory(&self, s: &TenantUserScope, id: &str) -> CoreResult<Option<MemoryRecord>> {
            self.scoped(&s.tenant_id,&s.user_id,|tx|tx.query_opt("SELECT record_json FROM nextral_memories WHERE tenant_id=$1 AND user_id=$2 AND id=$3",&[&s.tenant_id,&s.user_id,&id]).map_err(db)?.map(|r|serde_json::from_value(r.get(0)).map_err(CoreError::from)).transpose())
        }
        fn append_session_message(
            &self,
            s: &TenantUserScope,
            session: &str,
            role: &str,
            content: &str,
        ) -> CoreResult<String> {
            let m = SessionMessage::new(
                &s.tenant_id,
                &s.user_id,
                session,
                role,
                content,
                deterministic_id(&[session, role, content]),
            );
            let id = m.id.clone();
            self.append_message(&m)?;
            Ok(id)
        }
        fn upsert_reminder(&self, r: &ReminderRecord) -> CoreResult<()> {
            self.scoped(&r.tenant_id,&r.user_id,|tx|{
   let value=serde_json::to_value(r)?;let due=timestamp_seconds(&r.due_at)?;
   let last=r.last_attempt_at.as_deref().map(timestamp_seconds).transpose()?;
   let next=r.next_attempt_at.as_deref().map(timestamp_seconds).transpose()?;
   let created=timestamp_seconds(&r.created_at)?;let updated=timestamp_seconds(&r.updated_at)?;
   let count=tx.execute("INSERT INTO nextral_reminders(id,tenant_id,user_id,source_memory_id,kind,title,details,due_at,timezone,priority,status,attempt_count,dedupe_key,created_at,updated_at,record_json,last_attempt_at,next_attempt_at) VALUES($1,$2,$3,$4,$5,$6,$7,to_timestamp($8),$9,$10,$11,$12,$13,to_timestamp($14),to_timestamp($15),$16,to_timestamp($17),to_timestamp($18)) ON CONFLICT(id) DO UPDATE SET title=excluded.title,details=excluded.details,due_at=excluded.due_at,timezone=excluded.timezone,priority=excluded.priority,status=excluded.status,attempt_count=excluded.attempt_count,last_attempt_at=excluded.last_attempt_at,next_attempt_at=excluded.next_attempt_at,updated_at=excluded.updated_at,record_json=excluded.record_json WHERE nextral_reminders.tenant_id=excluded.tenant_id AND nextral_reminders.user_id=excluded.user_id AND nextral_reminders.source_memory_id=excluded.source_memory_id",&[&r.id,&r.tenant_id,&r.user_id,&r.source_memory_id,&text(&r.kind)?,&r.title,&r.details,&due,&r.timezone,&text(&r.priority)?,&text(&r.status)?,&(r.attempt_count as i32),&r.dedupe_key,&created,&updated,&value,&last,&next]).map_err(db)?;
   if count==0{return Err(CoreError::Conflict("reminder scope/source conflict".into()));}Ok(())
  })
        }
        fn write_audit(&self, e: &AuditWrite) -> CoreResult<()> {
            self.scoped(&e.tenant_id, e.user_id.as_deref().unwrap_or(""), |tx| {
                Self::audit_write(tx, e)
            })
        }
        fn enqueue_outbox(
            &self,
            tenant: &str,
            kind: &str,
            id: &str,
            payload: &str,
        ) -> CoreResult<String> {
            let v = serde_json::from_str(payload)?;
            self.scoped(tenant, "", |tx| {
                Self::outbox_write(tx, tenant, kind, id, &v)
            })
        }
    }
}
#[cfg(feature = "postgres")]
pub use enabled::*;

#[cfg(not(feature = "postgres"))]
mod disabled {
    use crate::{
        adapters::AdapterHealth,
        contracts::{CoreError, CoreResult},
        domain::{AuditEvent, SessionMessage},
        memory::MemoryRecord,
        ports::{AuditWrite, PostgresPort, TenantUserScope},
        prospective::ReminderRecord,
    };
    use serde_json::Value;
    #[derive(PartialEq, Eq)]
    pub struct PostgresAdapter {
        pub url: String,
    }
    fn unavailable() -> CoreError {
        CoreError::InvalidInput("production Postgres storage requires the postgres feature".into())
    }
    macro_rules! unavailable_methods {($($name:ident($($arg:ident:$ty:ty),*)->$out:ty;)*)=>{$(pub fn $name(&self,$($arg:$ty),*)->CoreResult<$out>{$(let _=$arg;)*Err(unavailable())})*};}
    impl PostgresAdapter {
        pub fn new(_url: impl Into<String>) -> CoreResult<Self> {
            Err(unavailable())
        }
        pub fn migration_sql(&self) -> &'static str {
            include_str!("../../migrations/postgres/0001_core_schema.sql")
        }
        pub fn health(&self) -> AdapterHealth {
            AdapterHealth {
                backend: "postgres".into(),
                configured: false,
                detail: "postgres feature disabled".into(),
            }
        }
        unavailable_methods! {
         readiness()->Value;migrate()->();
         commit_memory(r:&MemoryRecord,a:&AuditWrite,p:&Value,l:&str)->String;
         commit_graph_replay(r:&MemoryRecord,a:&AuditWrite,p:&Value)->String;
         acknowledge_graph(r:&MemoryRecord,v:&Value)->();
         list_memories(s:&TenantUserScope)->Vec<MemoryRecord>;
         record_accesses(s:&TenantUserScope,ids:&[&str])->();
         get_active_memories(s:&TenantUserScope,ids:&[&str])->Vec<MemoryRecord>;
         current_graph_memory_ids(s:&TenantUserScope,ids:&[String],p:&[crate::memory::PrivacyLevel])->Vec<String>;
         state_load(t:&str,k:&str)->Option<Value>;state_save(t:&str,k:&str,v:&Value)->();state_list(t:&str,p:&str)->Vec<(String,Value)>;
         state_save_claimed(t:&str,k:&str,o:&str,v:&Value)->Option<Value>;
         policy_mutate(s:&TenantUserScope,k:&str,v:Option<&Value>,a:&AuditWrite)->Option<Value>;
         claim(t:&str,k:&str,o:&str,n:u64,l:u64)->bool;release(t:&str,k:&str,o:&str)->();
         append_message(m:&SessionMessage)->();session_tail(s:&TenantUserScope,id:&str,l:usize)->Vec<SessionMessage>;session_head(s:&TenantUserScope,id:&str)->(Option<String>,usize);
         reminders(s:&TenantUserScope)->Vec<ReminderRecord>;
         claim_outbox(t:&str,o:&str)->Vec<(String,String,Value)>;renew_outbox(t:&str,id:&str,o:&str)->bool;finish_outbox(t:&str,id:&str,o:&str,e:Option<&str>)->();
         cutover(t:&str,u:&str,l:&str,target:&str)->();scrub_sources(s:&TenantUserScope,m:&MemoryRecord)->();
        }
    }
    impl PostgresPort for PostgresAdapter {
        fn upsert_memory(&self, _r: &MemoryRecord) -> CoreResult<()> {
            Err(unavailable())
        }
        fn get_memory(&self, _s: &TenantUserScope, _id: &str) -> CoreResult<Option<MemoryRecord>> {
            Err(unavailable())
        }
        fn append_session_message(
            &self,
            _s: &TenantUserScope,
            _id: &str,
            _role: &str,
            _content: &str,
        ) -> CoreResult<String> {
            Err(unavailable())
        }
        fn upsert_reminder(&self, _r: &ReminderRecord) -> CoreResult<()> {
            Err(unavailable())
        }
        fn write_audit(&self, _a: &AuditWrite) -> CoreResult<()> {
            Err(unavailable())
        }
        fn enqueue_outbox(&self, _t: &str, _kind: &str, _id: &str, _p: &str) -> CoreResult<String> {
            Err(unavailable())
        }
    }
    pub fn audit_write(e: &AuditEvent) -> CoreResult<AuditWrite> {
        Ok(AuditWrite {
            id: Some(e.id.clone()),
            tenant_id: e.tenant_id.clone(),
            user_id: e.user_id.clone(),
            actor_id: e.actor.clone(),
            action: serde_json::to_value(&e.action)?
                .as_str()
                .ok_or_else(|| CoreError::Serialization("audit action must be a string".into()))?
                .into(),
            target_type: e.target_type.clone(),
            target_id: e.target_id.clone(),
            reason: e.reason.clone(),
            request_id: e.request_id.clone(),
            trace_id: e.trace_id.clone(),
            metadata_json: e.metadata_json.clone(),
        })
    }
}
#[cfg(not(feature = "postgres"))]
pub use disabled::*;
