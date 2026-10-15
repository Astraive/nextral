//! Run with `cargo run --release --example research_evaluation -- SUITE HOST REVISION POLICY RUN HOST_ID LATENCY_BUDGET_NS [BASELINE]`.
//! SUITE may be `representative`. HOST is a real executable receiving one request JSON
//! on stdin and returning a serialized ReplayOutcome on stdout. It must run isolated
//! fixture setup, real operations and audited event counting, not echo golden answers.
//! For the representative suite, delivery_transport describes a controlled HTTP server
//! response; the host must exercise its actual delivery adapter against that server.
//! Startup/setup are included in process-roundtrip latency. No performance SLO is implied.

use nextral::{
    contracts::{CoreError, CoreResult},
    runtime::evaluation::{
        promotion_replay_gate, run_golden_replay, EvaluationReport, GoldenSuite, PromotionEvidence,
        ReplayCase, ReplayOutcome, RunMetadata,
    },
};
use std::{
    fs,
    io::Write,
    process::{Command, Stdio},
};

fn execute_host(executable: &str, case: &ReplayCase) -> CoreResult<ReplayOutcome> {
    let mut process = Command::new(executable)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()?;
    let input = process
        .stdin
        .take()
        .ok_or_else(|| CoreError::Io("research host stdin unavailable".into()))?;
    let write_result = (|| -> CoreResult<()> {
        let mut input = input;
        serde_json::to_writer(&mut input, &case.request)?;
        input.write_all(b"\n")?;
        Ok(())
    })();
    if let Err(error) = write_result {
        let _ = process.kill();
        let _ = process.wait();
        return Err(error);
    }
    let output = process.wait_with_output()?;
    if !output.status.success() {
        return Err(CoreError::Conflict(
            "research host failed; no successful report emitted".into(),
        ));
    }
    // Host stderr and result contents are deliberately absent from diagnostics.
    serde_json::from_slice(&output.stdout).map_err(|_| {
        CoreError::InvalidInput("research host returned an invalid ReplayOutcome".into())
    })
}

fn run() -> CoreResult<()> {
    if cfg!(debug_assertions) {
        return Err(CoreError::InvalidInput(
            "measured performance runner requires --release".into(),
        ));
    }
    let args: Vec<String> = std::env::args().skip(1).collect();
    if !(7..=8).contains(&args.len()) {
        return Err(CoreError::InvalidInput(
            "usage: research_evaluation SUITE HOST REVISION POLICY RUN HOST_ID LATENCY_BUDGET_NS [BASELINE]".into(),
        ));
    }
    let suite: GoldenSuite = if args[0] == "representative" {
        GoldenSuite::representative()?
    } else {
        serde_json::from_slice(&fs::read(&args[0])?)?
    };
    let latency_budget_ns = args[6].parse::<u64>().map_err(|_| {
        CoreError::InvalidInput("latency budget must be positive nanoseconds".into())
    })?;
    let metadata = RunMetadata {
        run_id: args[4].clone(),
        implementation_revision: args[2].clone(),
        policy_version: args[3].clone(),
        host_id: args[5].clone(),
        latency_scope: "process-roundtrip".into(),
    };
    let report = run_golden_replay(&suite, metadata, latency_budget_ns, |case| {
        execute_host(&args[1], case)
    })?;
    let stdout = std::io::stdout();
    let mut stdout = stdout.lock();
    serde_json::to_writer_pretty(&mut stdout, &report)?;
    stdout.write_all(b"\n")?;
    if let Some(path) = args.get(7) {
        let baseline: EvaluationReport = serde_json::from_slice(&fs::read(path)?)?;
        promotion_replay_gate(&PromotionEvidence {
            baseline,
            candidate: report,
        })?;
    }
    Ok(())
}

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
