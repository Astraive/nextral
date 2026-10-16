'use strict';

const { loadNative } = require('./native-loader');

class NextralError extends Error {
  constructor(code, message, options) {
    super(message, options);
    this.name = 'NextralError';
    this.code = code;
    this.executionStatus = options && options.executionStatus ? options.executionStatus : undefined;
  }
}

function normalizeError(error) {
  if (error instanceof NextralError) return error;
  if (error && typeof error.message === 'string') {
    try {
      const details = JSON.parse(error.message);
      if (typeof details.code === 'string' && typeof details.message === 'string') {
        return new NextralError(details.code, details.message, {
          cause: error,
          executionStatus: details.execution_status || undefined,
        });
      }
    } catch { /* Native transport errors are not always JSON. */ }
  }
  return new NextralError(
    error instanceof TypeError ? 'invalid_input' : 'native_error',
    error instanceof Error ? error.message : String(error),
    { cause: error },
  );
}

function encode(value) {
  const json = JSON.stringify(value, (_key, item) => {
    if (typeof item === 'number' && !Number.isFinite(item)) {
      throw new NextralError('invalid_input', 'JSON numeric inputs must be finite.');
    }
    return item;
  });
  if (json === undefined) throw new NextralError('invalid_input', 'A JSON object is required.');
  return json;
}

function invoke(method, ...args) {
  try {
    return loadNative()[method](...args);
  } catch (error) {
    throw normalizeError(error);
  }
}

function validateConfig(config) {
  try {
    return JSON.parse(invoke('validateConfig', encode(config)));
  } catch (error) {
    throw normalizeError(error);
  }
}

class ServiceHandle {
  constructor(nativeHandle) {
    this._native = nativeHandle;
  }

  async shutdown(timeoutMs) {
    try {
      return await this._native.shutdown(timeoutMs);
    } catch (error) {
      throw normalizeError(error);
    }
  }

  get binds() {
    return this._native.binds();
  }
}
class WorkerHandle {
  constructor(nativeHandle) {
    this._native = nativeHandle;
  }

  async tick() {
    try {
      const raw = await this._native.tick();
      return JSON.parse(raw);
    } catch (error) {
      throw normalizeError(error);
    }
  }

  async shutdown(timeoutMs) {
    try {
      return await this._native.shutdown(timeoutMs);
    } catch (error) {
      throw normalizeError(error);
    }
  }
}


class Client {
  constructor(nativeClient) {
    this._native = nativeClient;
  }

  static async create(config) {
    try {
      const native = loadNative().Client.create(encode(config));
      return new Client(native);
    } catch (error) {
      throw normalizeError(error);
    }
  }

  async call(tool, payload, options = {}) {
    const { timeoutMs, signal } = options;
    const ticket = this._native.createTicket();
    let onAbort;
    if (signal) {
      if (signal.aborted) {
        ticket.cancel();
      } else {
        onAbort = () => ticket.cancel();
        signal.addEventListener('abort', onAbort, { once: true });
      }
    }
    try {
      const raw = await this._native.callJsonWithTicket(
        tool,
        encode(payload),
        timeoutMs !== undefined ? timeoutMs : null,
        ticket,
      );
      return JSON.parse(raw);
    } catch (error) {
      throw normalizeError(error);
    } finally {
      if (signal && onAbort) {
        signal.removeEventListener('abort', onAbort);
      }
    }
  }

  async close(timeoutMs) {
    try {
      const raw = await this._native.close(timeoutMs !== undefined ? timeoutMs : null);
      return JSON.parse(raw);
    } catch (error) {
      throw normalizeError(error);
    }
  }

  async serve() {
    try {
      const nativeHandle = await this._native.serve();
      return new ServiceHandle(nativeHandle);
    } catch (error) {
      throw normalizeError(error);
    }
  }

  async startWorker(config) {
    try {
      const nativeHandle = await this._native.startWorker(encode(config));
      return new WorkerHandle(nativeHandle);
    } catch (error) {
      throw normalizeError(error);
    }
  }

  async health(options) {
    return this.call('nextral.health', {}, options);
  }

  async ingest(request, options) {
    return this.call('nextral.memory.ingest', request, options);
  }

  async retrieve(request, options) {
    return this.call('nextral.memory.retrieve', request, options);
  }

  async forget(request, options) {
    return this.call('nextral.memory.forget', request, options);
  }

  async getMemory(request, options) {
    return this.call('nextral.memory.get', request, options);
  }

  async listMemories(request, options) {
    const payload = { ...request };
    if (payload.include_inactive && (!payload.filters || !payload.filters.statuses)) {
      payload.filters = { ...(payload.filters || {}), statuses: ['active', 'soft_deleted', 'redacted', 'archived'] };
    }
    const res = await this.call('nextral.memory.list', payload, options);
    const items = Array.isArray(res) ? res : (res && res.items ? res.items : []);
    return items.map(item => {
      const { preview, content_preview, ...rest } = item;
      return {
        embedding_provider: null,
        extraction_provider: null,
        extraction_model: null,
        schema_version: '1.0.0',
        ...rest,
      };
    });
  }

  async queryMemories(request, options) {
    return this.call('nextral.memory.list', request, options);
  }

  async repairIndex(request, options) {
    return this.call('nextral.index.repair', request, options);
  }

  async updateMemory(request, options) {
    return this.call('nextral.memory.update', request, options);
  }

  async memoryStats(request, options) {
    return this.call('nextral.memory.stats', request, options);
  }

  async searchByTag(request, options) {
    return this.call('nextral.memory.search_by_tag', request, options);
  }

  async searchByEntity(request, options) {
    return this.call('nextral.memory.search_by_entity', request, options);
  }

  async scoredSearch(request, options) {
    return this.call('nextral.runtime.scored_search', request, options);
  }

  async appendSessionMessage(request, options) {
    return this.call('nextral.session.append', request, options);
  }

  async eligibleSessions(request, options) {
    return this.call('nextral.session.eligible', request, options);
  }

  async workingContext(request, options) {
    return this.call('nextral.session.context', request, options);
  }

  async queryGraph(request, options) {
    return this.call('nextral.graph.query', request, options);
  }

  async graphify(request, options) {
    return this.call('nextral.graph.graphify', request, options);
  }

  async consolidate(request, options) {
    return this.call('nextral.consolidation.run', request, options);
  }

  async scheduleReminder(request, options) {
    return this.call('nextral.reminders.schedule', request, options);
  }

  async listReminders(request, options) {
    return this.call('nextral.reminders.list', request, options);
  }

  async executeDueReminders(request, options) {
    return this.call('nextral.reminders.due', request, options);
  }

  async planReembed(request, options) {
    return this.call('nextral.reembed.plan', request, options);
  }

  async executeReembed(request, options) {
    return this.call('nextral.jobs.reembed.execute', request, options);
  }

  async enqueueJob(request, options) {
    return this.call('nextral.jobs.enqueue', request, options);
  }

  async getJob(request, options) {
    return this.call('nextral.jobs.get', request, options);
  }

  async listJobs(request, options) {
    return this.call('nextral.jobs.list', request, options);
  }

  async runJobs(request, options) {
    return this.call('nextral.jobs.run', request, options);
  }

  async runOutbox(request, options) {
    return this.call('nextral.outbox.run', request, options);
  }

  async migrateDatabase(options) {
    return this.call('nextral.database.migrate', {}, options);
  }

  async upsertPolicy(request, options) {
    return this.call('nextral.policy.upsert', request, options);
  }

  async listPolicies(request, options) {
    return this.call('nextral.policy.list', request, options);
  }

  async removePolicy(request, options) {
    return this.call('nextral.policy.remove', request, options);
  }

  async createExperiment(request, options) {
    return this.call('experiments.create', request, options);
  }

  async promoteExperiment(request, options) {
    return this.call('experiments.promote', request, options);
  }

  async rollbackExperiment(request, options) {
    return this.call('experiments.rollback', request, options);
  }

  async experimentStatus(request = {}, options) {
    return this.call('experiments.status', request, options);
  }

  async safetyPolicy(options) {
    return this.call('safety.policy.get', {}, options);
  }

  async setSafetyPolicy(request, options) {
    return this.call('safety.policy.set', request, options);
  }

  async batchIngest(request, options) {
    return this.call('nextral.batch.ingest', request, options);
  }

  async batchRetrieve(request, options) {
    return this.call('nextral.batch.retrieve', request, options);
  }

  async batchForget(request, options) {
    return this.call('nextral.batch.forget', request, options);
  }
}
exports.WorkerHandle = WorkerHandle;

exports.Client = Client;
exports.ServiceHandle = ServiceHandle;
exports.NextralError = NextralError;
exports.validateConfig = validateConfig;
exports.lexicalScore = (text, query) => invoke('lexicalScore', text, query);
