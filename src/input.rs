//! Input: resolve the FCS archive `documentId` from the column-facet table.
//!
//! Contract (R `main.R` / `utils.R::download_files`): the crosstab must have a `documentId`
//! column factor; the operator processes the **first** row's documentId. Several distinct
//! documentIds → we warn and use the first (R silently does the same).
use anyhow::{Context, Result, anyhow, bail};
use polars::prelude::*;
use tercen_rs::context::ContextBase;
use tercen_rs::tson_to_dataframe;

pub async fn load_document_id(ctx: &ContextBase) -> Result<String> {
    let col_table_id = ctx.cube_query().column_hash.clone();
    if col_table_id.is_empty() {
        bail!("Column factor documentId is required (the step has no column projection).");
    }
    let cnames = ctx
        .cnames()
        .await
        .map_err(|e| anyhow!("fetch column-facet schema: {e}"))?;
    let doc_col = cnames
        .iter()
        .find(|c| c.as_str() == "documentId" || c.ends_with(".documentId"))
        .cloned()
        .ok_or_else(|| {
            anyhow!("Column factor documentId is required (columns present: {cnames:?})")
        })?;

    let tson = ctx
        .streamer()
        .stream_tson(&col_table_id, Some(vec![doc_col.clone()]), 0, -1)
        .await
        .map_err(|e| anyhow!("stream column-facet table {col_table_id}: {e}"))?;
    let df = tson_to_dataframe(&tson).context("parse column-facet TSON")?;
    let s = df
        .column(&doc_col)
        .map_err(|e| anyhow!("missing column '{doc_col}': {e}"))?
        .cast(&DataType::String)
        .context("cast documentId to string")?;
    let ca = s.str().context("documentId is not a string column")?;
    let first = ca
        .get(0)
        .ok_or_else(|| anyhow!("column-facet table is empty — no documentId rows"))?
        .to_string();
    if first.is_empty() {
        bail!("first documentId value is empty");
    }
    let distinct: std::collections::BTreeSet<&str> = ca.into_iter().flatten().collect();
    if distinct.len() > 1 {
        tracing::warn!(n_distinct = distinct.len(), used = %first,
            "multiple documentIds projected — processing only the first (R operator behaviour)");
    }
    Ok(first)
}
