//! Dev binary: `TERCEN_URI`, `TERCEN_TOKEN`, `WORKFLOW_ID`, `STEP_ID` from the environment.
//! Optional: `OUTPUT_TSON=<path>` (copy of the result), `DEV_NO_UPLOAD=1`, `DEV_NO_LINK=1`,
//! `READ_FCS_WORKDIR`, `READ_FCS_KEEP_WORKDIR=1`.
use anyhow::Result;

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;
use read_fcs_operator::{init_tracing, require_env, run_dev};

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    for v in ["TERCEN_URI", "TERCEN_TOKEN", "WORKFLOW_ID", "STEP_ID"] {
        require_env(v)?;
    }
    run_dev(&require_env("WORKFLOW_ID")?, &require_env("STEP_ID")?).await
}
