//! read_fcs_operator — Rust port of tercen/read_fcs_operator.
//!
//! * `fcs`: the FCS reader (header, TEXT, DATA, post-processing, seeded sampling).
//! * `input` / `download`: documentId → FileService download → FCS files.
//! * `output`: R-operator output semantics + streaming TSON `OperatorResult`.
//! * `upload`: streamed FileService upload; production task attach / dev task run + step link.
//! * `bin/fcsdump`: CLI used by the parity harness.
pub mod context;
pub mod download;
pub mod fcs;
pub mod input;
pub mod output;
pub mod pagecache;
pub mod progress;
pub mod tson;
pub mod upload;

pub use fcs::{
    DataType, Endian, FcsData, FcsError, Param, ReadOptions, read_bytes, read_file, sample_indices,
};

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use tercen_rs::context::ContextBase;
use tercen_rs::{DevContext, PropertyReader, TercenClient};

use output::Settings;
use progress::Reporter;

/// Production entry point (`--taskId`).
pub async fn run(task_id: &str) -> Result<()> {
    tracing::info!("read_fcs_operator starting (task_id={task_id})");
    let client = build_client().await?;
    // Deliberately not `ProductionContext::from_task_id`: see context.rs.
    let ctx = context::from_task_id(client, task_id).await?;
    execute(
        &ctx,
        Mode::Production {
            task_id: task_id.to_string(),
        },
    )
    .await
}

/// Dev entry point (`WORKFLOW_ID` / `STEP_ID`).
pub async fn run_dev(workflow_id: &str, step_id: &str) -> Result<()> {
    tracing::info!(
        "read_fcs_operator starting in dev mode (workflow_id={workflow_id}, step_id={step_id})"
    );
    let client = build_client().await?;
    let ctx = DevContext::from_workflow_step(client, workflow_id, step_id)
        .await
        .map_err(|e| anyhow::anyhow!("load workflow {workflow_id} / step {step_id}: {e}"))?;
    execute(
        &ctx,
        Mode::Dev {
            workflow_id: workflow_id.to_string(),
            step_id: step_id.to_string(),
        },
    )
    .await
}

enum Mode {
    Production {
        task_id: String,
    },
    Dev {
        workflow_id: String,
        step_id: String,
    },
}

async fn build_client() -> Result<Arc<TercenClient>> {
    let client = TercenClient::from_env()
        .await
        .map_err(|e| anyhow::anyhow!("connect to Tercen: {e}"))?;
    tracing::info!("connected to Tercen");
    Ok(Arc::new(client))
}

/// Read the operator properties (R `ctx$op.value` defaults).
pub fn settings_from_ctx(ctx: &ContextBase) -> Result<Settings> {
    let pr = PropertyReader::from_operator_settings(ctx.operator_settings());
    // Tercen serialises a DoubleProperty as e.g. "7.0", so every numeric property is parsed as
    // f64 and then cast. Parsing these as integers made a legitimate "7.0" fall back to the
    // default silently, which is worse than failing: the run succeeds with the wrong setting.
    let num = |name: &str, default: f64| -> Result<f64> {
        let raw = pr.get_string(name, &default.to_string());
        let v: f64 = raw
            .trim()
            .parse()
            .map_err(|_| anyhow::anyhow!("property '{name}' is not a number: '{raw}'"))?;
        Ok(v)
    };
    let wl = num("which.lines", -1.0)?;
    // R: -1 (or NA) means every event. 0 would make R sample zero rows; here it also means
    // "every event", which is the documented deviation.
    let which_lines = if wl > 0.0 { Some(wl as usize) } else { None };
    let seed = num("seed", 42.0)?;
    if !(0.0..=u64::MAX as f64).contains(&seed) {
        bail!("property 'seed' must be >= 0, got {seed}");
    }
    let threads = num("threads", 0.0)?;
    if threads < 0.0 {
        bail!("property 'threads' must be >= 0, got {threads}");
    }
    let s = Settings {
        which_lines,
        gather_channels: pr.get_bool("gather_channels", false),
        ungather_pattern: pr.get_string("ungather_pattern", "time|event"),
        truncate_max_range: pr.get_bool("truncate_max_range", true),
        seed: seed as u64,
        threads: threads as usize,
        ..Settings::default()
    };
    s.ungather_regex()?; // validate early
    Ok(s)
}

async fn execute(ctx: &ContextBase, mode: Mode) -> Result<()> {
    let t_start = Instant::now();
    tracing::info!(
        workflow = ctx.workflow_id(),
        step = ctx.step_id(),
        project = ctx.project_id(),
        namespace = ctx.namespace(),
        "context loaded"
    );
    // Progress reaches the user only in production, where there is a task to attach events to.
    let rep = match &mode {
        Mode::Production { task_id } => Reporter::spawn(Arc::clone(ctx.client()), task_id.clone()),
        Mode::Dev { .. } => Reporter::silent(),
    };
    let settings = settings_from_ctx(ctx)?;
    tracing::info!(?settings, "properties");
    rep.at(0, "Reading the input table");

    let doc_id = input::load_document_id(ctx)
        .await
        .map_err(|e| anyhow::anyhow!("load input table: {e:#}"))?;
    tracing::info!(doc_id, "input documentId resolved");

    let work_root = std::env::var("READ_FCS_WORKDIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            std::env::temp_dir().join(format!(
                "read_fcs_op_{}_{}",
                ctx.workflow_id(),
                ctx.step_id()
            ))
        });
    let _guard = TempDirGuard(work_root.clone());
    let t = Instant::now();
    rep.at(progress::DOWNLOAD.0, "Downloading the FCS document");
    let dl = download::fetch_fcs_files(ctx, &doc_id, &work_root)
        .await
        .map_err(|e| anyhow::anyhow!("file download: {e:#}"))?;
    rep.at(
        progress::DOWNLOAD.1,
        format!("{} FCS file(s) ready", dl.files.len()),
    );
    tracing::info!(
        n_files = dl.files.len(),
        bytes = dl.bytes,
        secs = format!("{:.1}", t.elapsed().as_secs_f64()),
        "FCS files ready"
    );

    let t = Instant::now();
    let plan =
        output::plan(&dl.files, &settings, &rep).map_err(|e| anyhow::anyhow!("plan: {e:#}"))?;
    tracing::info!(
        secs = format!("{:.1}", t.elapsed().as_secs_f64()),
        "{}",
        output::describe(&plan)
    );

    let result_path = work_root.join("result.tson");
    let t = Instant::now();
    let bytes = {
        let f = std::fs::File::create(&result_path)
            .with_context(|| format!("create {}", result_path.display()))?;
        // The result can be several GB; release it from the page cache as it goes, or the cgroup
        // counts it against the worker's --memory limit (see pagecache.rs).
        let w = std::io::BufWriter::with_capacity(8 << 20, pagecache::Releasing::new(f, 256 << 20));
        output::write_operator_result(w, &plan, &settings, &dl.doc_name, &rep)
            .map_err(|e| anyhow::anyhow!("write result: {e:#}"))?
    };
    let secs = t.elapsed().as_secs_f64();
    tracing::info!(
        bytes,
        secs = format!("{secs:.1}"),
        rows_per_s = format!("{:.0}", plan.measurement_rows() as f64 / secs.max(1e-9)),
        "OperatorResult written"
    );

    if let Ok(copy) = std::env::var("OUTPUT_TSON") {
        std::fs::copy(&result_path, &copy)?;
        tracing::info!(copy, "dev: OperatorResult TSON copied");
    }

    match mode {
        Mode::Production { task_id } => {
            upload::save_production(ctx, &task_id, &result_path, &rep)
                .await
                .map_err(|e| anyhow::anyhow!("upload: {e:#}"))?;
            rep.info("Import complete");
        }
        Mode::Dev {
            workflow_id,
            step_id,
        } => {
            if std::env::var("DEV_NO_UPLOAD").is_ok() {
                tracing::info!("DEV_NO_UPLOAD set: skipping upload");
            } else {
                let saved = upload::save_dev(ctx, &workflow_id, &step_id, &result_path)
                    .await
                    .map_err(|e| anyhow::anyhow!("dev save: {e:#}"))?;
                tracing::info!(
                    task_id = saved.task_id,
                    file_id = saved.file_id,
                    "dev result saved"
                );
            }
        }
    }
    tracing::info!(
        total_secs = format!("{:.1}", t_start.elapsed().as_secs_f64()),
        "done"
    );
    Ok(())
}

struct TempDirGuard(PathBuf);
impl Drop for TempDirGuard {
    fn drop(&mut self) {
        if std::env::var("READ_FCS_KEEP_WORKDIR").is_err() {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}

pub fn init_tracing() {
    use tracing_subscriber::{EnvFilter, fmt, prelude::*};
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env().add_directive(tracing::Level::INFO.into()))
        .init();
}

pub fn require_env(name: &str) -> Result<String> {
    std::env::var(name).with_context(|| format!("{name} environment variable not set"))
}
