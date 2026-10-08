//! Output planning and TSON emission.
//!
//! Mirrors `tercen/read_fcs_operator` `main.R` + `utils.R::process_fcs`:
//!
//! * per file: `desc` (`$PnS`) that is missing or duplicated → replaced by the channel name;
//!   channels whose **name** matches `ungather_pattern` (case-insensitive regex, default
//!   `time|event`) are "un-gathered" (kept as observation columns); the other channels get
//!   `channel_id = 1..n` in file order;
//! * events are numbered `event_id = 1..total` across files in file order (after `which.lines`
//!   sampling);
//! * `gather_channels = true`: **Measurements** (`event_id`, `channel_id`, `value`) ⟕ **Observations**
//!   (un-gathered channels, `event_id`, `random_sequence`, `filename`) on `event_id` ⟕ **Variables** (`channel_name`,
//!   `channel_description`, `channel_name_description`, `channel_id`) on `channel_id`; when the
//!   same channel name carries different descriptions across files the description columns are
//!   dropped (R: "Different descriptions for the same channel name have been found");
//! * `gather_channels = false`: the wide table (all channels, `fileId`, `event_id`) ⟕ Observations;
//!   R additionally uploads the channel table as a project CSV, which this port does not (logged);
//! * plus a **Summary** relation (`FCS_summary.md`, base64 `.content`).
//!
//! Files with different channel counts are handled as R's `bind_rows` + `melt` would: missing
//! cells become NA (NaN here), so every event has `max(n_channels)` measurement rows.
//!
//! The result is written as one streaming TSON `OperatorResult` (`tson.rs`), so memory stays at
//! about one decoded file regardless of the cohort size; the data are decoded twice (once for the
//! plan and the observation columns, once for the values), which is far cheaper than holding them.
use std::collections::{BTreeSet, HashSet};
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use base64::Engine;
use rayon::prelude::*;
use regex::Regex;

use crate::fcs::{FcsData, Param, ReadOptions, Spillover, event_ranks, file_seed, read_file};
use crate::progress::{self, Reporter};
use crate::tson::TsonWriter;

/// Operator properties (see `operator.json`).
#[derive(Debug, Clone)]
pub struct Settings {
    /// `which.lines`: None = all events, Some(k) = a seeded random sample of k events per file.
    pub which_lines: Option<usize>,
    pub gather_channels: bool,
    pub ungather_pattern: String,
    pub truncate_max_range: bool,
    /// Seed for `which.lines` sampling (the R operator samples unseeded).
    pub seed: u64,
    /// Decode threads per file; 0 = rayon default.
    pub threads: usize,
    /// Files decoded concurrently in the planning pass. Peak memory is
    /// `in_flight × (events per file × channels × 8 B)`, so this — not the machine's core
    /// count — is what bounds it. The worker enforces a hard `--memory` limit taken from
    /// `memory_model.json`, so an unbounded pass would be killed on a large host.
    pub in_flight: usize,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            which_lines: None,
            gather_channels: false,
            ungather_pattern: "time|event".into(),
            truncate_max_range: true,
            seed: 42,
            threads: 0,
            in_flight: 4,
        }
    }
}

impl Settings {
    pub fn read_options(&self) -> ReadOptions {
        ReadOptions {
            truncate_max_range: self.truncate_max_range,
            dataset: 1,
            threads: if self.threads == 0 {
                None
            } else {
                Some(self.threads)
            },
            ..ReadOptions::default()
        }
    }
    pub fn ungather_regex(&self) -> Result<Regex> {
        let build = |p: &str| regex::RegexBuilder::new(p).case_insensitive(true).build();
        let p = self.ungather_pattern.as_str();
        match build(p) {
            Ok(r) => Ok(r),
            // R's `grepl()` (TRE, POSIX extended) reads a repetition operator with nothing before
            // it as a literal character; the regex crate refuses it. Workflows saved with 2.x
            // carry such patterns (the immunophenotyping template's `?!(...)`, a lookahead that
            // TRE never supported, so in R it matches nothing): read them the way R did.
            Err(e) if p.starts_with(['?', '*', '+', '{']) => build(&format!("\\{p}"))
                .map_err(|_| e)
                .with_context(|| format!("invalid ungather_pattern regex '{p}'")),
            Err(e) => Err(e).with_context(|| format!("invalid ungather_pattern regex '{p}'")),
        }
    }
}

/// What we keep per file after the planning pass.
#[derive(Debug)]
pub struct FilePlan {
    pub path: PathBuf,
    pub filename: String,
    pub params: Vec<Param>,
    /// Final description per parameter (R's NA/duplicate → name rule applied).
    pub desc: Vec<String>,
    pub n_events_in_file: usize,
    /// Sampled row indices (sorted) when `which.lines` applies.
    pub idx: Option<Vec<usize>>,
    /// Rows contributed by this file.
    pub n_rows: usize,
    /// `random_sequence` of each contributed row, in row order: the event's 1-based rank in the
    /// file's seeded random order (`fcs::event_ranks`). With `which.lines = k` these are exactly
    /// the values 1..=k, once each.
    pub ranks: Vec<u32>,
    /// Parameter indices matched by `ungather_pattern` (kept wide, in file order).
    pub condx: Vec<usize>,
    /// Parameter indices that are gathered (channel_id = position + 1).
    pub gathered: Vec<usize>,
    /// Values of the un-gathered channels (sampled rows), one Vec per `condx` entry.
    pub condx_values: Vec<Vec<f64>>,
    /// Parsed `$SPILLOVER`/`SPILL` matrix, when the file has a readable one.
    pub spill: Option<Spillover>,
}

/// One row of the Variables table.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NameMapRow {
    pub channel_name: String,
    pub channel_description: String,
    pub channel_name_description: String,
    pub channel_id: u32,
}

#[derive(Debug)]
pub struct Plan {
    pub files: Vec<FilePlan>,
    /// Union of un-gathered channel names, first-appearance order.
    pub condx_names: Vec<String>,
    /// Union of all channel names (wide output), first-appearance order.
    pub all_names: Vec<String>,
    /// Distinct name-map rows in first-appearance order (R `distinct()`).
    pub names_map: Vec<NameMapRow>,
    /// R: `duplicated(channel_name)` in the distinct map → drop description columns.
    pub bad_description: bool,
    pub total_events: usize,
    /// max over files of gathered channel count.
    pub n_channels: usize,
    /// Compensation rows (`comp_1`, `comp_2`, `comp_value`, `filename`), empty when the relation
    /// is not emitted. R emits it only when **every** file has a matrix, and so does this.
    pub compensation: Vec<CompRow>,
}

/// One cell of one file's compensation matrix, in the long form the R operator produces.
#[derive(Debug, Clone)]
pub struct CompRow {
    pub comp_1: String,
    pub comp_2: String,
    pub comp_value: f64,
    pub filename: String,
}

impl Plan {
    pub fn measurement_rows(&self) -> usize {
        self.total_events * self.n_channels
    }
}

fn apply_desc_rule(params: &[Param]) -> Vec<String> {
    // R: na_desc_idx <- is.na(desc); dup_desc_idx <- duplicated(desc) [on the original vector,
    // NA counting as a value]; desc[na | dup] <- name.
    let mut seen: HashSet<Option<&str>> = HashSet::new();
    let mut out = Vec::with_capacity(params.len());
    for p in params {
        let d = if p.desc.is_empty() {
            None
        } else {
            Some(p.desc.as_str())
        };
        let dup = !seen.insert(d);
        out.push(if d.is_none() || dup {
            p.name.clone()
        } else {
            p.desc.clone()
        });
    }
    out
}

fn plan_file(
    path: &Path,
    s: &Settings,
    opts: &ReadOptions,
    re: &Regex,
    file_index: usize,
) -> Result<FilePlan> {
    let d = read_file(path, opts).with_context(|| format!("read FCS {}", path.display()))?;
    let filename = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_string();
    // Ranked once per file; `which.lines` is a filter on that rank, so the same seed gives the
    // same cells whether the sampling happens here or downstream on `random_sequence`.
    let _ = file_index;
    let all_ranks = event_ranks(d.n_events, file_seed(s.seed, &filename));
    let idx = s.which_lines.map(|k| {
        (0..d.n_events)
            .filter(|&i| (all_ranks[i] as usize) <= k)
            .collect::<Vec<usize>>()
    });
    let n_rows = idx.as_ref().map(|v| v.len()).unwrap_or(d.n_events);
    let ranks: Vec<u32> = match &idx {
        Some(v) => v.iter().map(|&r| all_ranks[r]).collect(),
        None => all_ranks,
    };
    let mut condx = Vec::new();
    let mut gathered = Vec::new();
    for (i, p) in d.params.iter().enumerate() {
        if re.is_match(&p.name) {
            condx.push(i)
        } else {
            gathered.push(i)
        }
    }
    let condx_values = condx
        .iter()
        .map(|&i| match &idx {
            Some(v) => v.iter().map(|&r| d.columns[i][r]).collect(),
            None => d.columns[i].clone(),
        })
        .collect();
    let desc = apply_desc_rule(&d.params);
    let spill = crate::fcs::parse_spillover(&d.text, &d.params);
    if spill.is_none() && crate::fcs::spillover_keyword(&d.text).is_some() {
        tracing::warn!(file = %filename, "spillover keyword present but unreadable (size mismatch); ignored");
    }
    Ok(FilePlan {
        path: path.to_path_buf(),
        filename,
        params: d.params,
        desc,
        n_events_in_file: d.n_events,
        idx,
        n_rows,
        ranks,
        condx,
        gathered,
        condx_values,
        spill,
    })
}

/// Planning pass: read every file, keep metadata + observation columns.
///
/// Files are decoded `s.in_flight` at a time rather than all at once: a whole decoded file
/// is transient, so unbounded parallelism would make peak memory a function of the host's
/// core count and blow the worker's hard `--memory` limit on a large machine.
pub fn plan(files: &[PathBuf], s: &Settings, rep: &Reporter) -> Result<Plan> {
    if files.is_empty() {
        bail!("no FCS files to read");
    }
    let opts = s.read_options();
    let re = s.ungather_regex()?;
    let in_flight = s.in_flight.max(1);
    let mut plans: Vec<FilePlan> = Vec::with_capacity(files.len());
    for (chunk_no, chunk) in files.chunks(in_flight).enumerate() {
        let base = chunk_no * in_flight;
        let mut part: Vec<FilePlan> = chunk
            .par_iter()
            .enumerate()
            .map(|(i, p)| plan_file(p, s, &opts, &re, base + i))
            .collect::<Result<Vec<_>>>()?;
        plans.append(&mut part);
        rep.at(
            progress::band(progress::PLAN, plans.len(), files.len()),
            format!("Reading FCS files: {} of {}", plans.len(), files.len()),
        );
    }
    plans.sort_by(|a, b| a.path.cmp(&b.path));

    let mut condx_names: Vec<String> = Vec::new();
    let mut all_names: Vec<String> = Vec::new();
    let mut names_map: Vec<NameMapRow> = Vec::new();
    let mut seen_rows: HashSet<NameMapRow> = HashSet::new();
    let mut total_events = 0usize;
    let mut n_channels = 0usize;
    for f in &plans {
        for &i in &f.condx {
            if !condx_names.contains(&f.params[i].name) {
                condx_names.push(f.params[i].name.clone());
            }
        }
        for p in &f.params {
            if !all_names.contains(&p.name) {
                all_names.push(p.name.clone());
            }
        }
        for (k, &i) in f.gathered.iter().enumerate() {
            let name = f.params[i].name.clone();
            let desc = f.desc[i].clone();
            let nd = if name == desc {
                name.clone()
            } else {
                format!("{name} - {desc}")
            };
            let row = NameMapRow {
                channel_name: name,
                channel_description: desc,
                channel_name_description: nd,
                channel_id: (k + 1) as u32,
            };
            if seen_rows.insert(row.clone()) {
                names_map.push(row);
            }
        }
        total_events += f.n_rows;
        n_channels = n_channels.max(f.gathered.len());
    }
    let mut names_seen = HashSet::new();
    let bad_description = names_map
        .iter()
        .any(|r| !names_seen.insert(r.channel_name.clone()));
    if bad_description {
        rep.log(
            "Different descriptions for the same channel name have been found. \
             Description field will be ignored.",
        );
    }
    if plans.iter().any(|f| f.gathered.len() != n_channels) {
        // R assigns channel_id positionally *within each file* (`colnames(data)[!condx] <-
        // names_map$channel_id`), so when files disagree the same id means a different marker in
        // different files, and a join on channel_id fans out. We reproduce that rather than
        // renumber, but say so: silently wrong joins are worse than a noisy import.
        let mut by_id: std::collections::HashMap<u32, HashSet<&str>> =
            std::collections::HashMap::new();
        for r in &names_map {
            by_id
                .entry(r.channel_id)
                .or_default()
                .insert(r.channel_name.as_str());
        }
        let ambiguous = by_id.values().filter(|v| v.len() > 1).count();
        rep.log(format!(
            "Files do not share a channel set. Missing values are NA, and channel_id is \
             positional per file, so {ambiguous} channel_id(s) refer to different markers in \
             different files — join Measurements to Variables on channel_id with that in mind, \
             or import the differing files separately."
        ));
        tracing::warn!(
            channels_max = n_channels,
            ambiguous_channel_ids = ambiguous,
            "files do not share a channel set: missing cells become NA (as R's bind_rows/melt              produce) and channel_id is positional per file, so {ambiguous} id(s) refer to              different markers in different files — join Measurements to Variables on              channel_id with that in mind, or import the differing files separately"
        );
    }
    // R (`main.R`): the Compensation relation is emitted only if *every* file has a matrix
    // (`output.spill <- !any(is.na(...))`); otherwise it logs and emits nothing.
    let with_spill = plans.iter().filter(|f| f.spill.is_some()).count();
    let mut compensation = Vec::new();
    if with_spill == plans.len() {
        for f in &plans {
            let sp = f.spill.as_ref().expect("checked above");
            for (i, r) in sp.names.iter().enumerate() {
                for (j, c) in sp.names.iter().enumerate() {
                    compensation.push(CompRow {
                        comp_1: r.clone(),
                        comp_2: c.clone(),
                        comp_value: sp.get(i, j),
                        filename: f.filename.clone(),
                    });
                }
            }
        }
        tracing::info!(
            files = plans.len(),
            rows = compensation.len(),
            "built-in compensation matrices exported"
        );
    } else if with_spill > 0 {
        rep.log(format!(
            "Only {with_spill} of {} files carry a compensation matrix; no Compensation output.",
            plans.len()
        ));
    } else {
        tracing::info!("No built-in compensation matrices found.");
    }
    Ok(Plan {
        files: plans,
        condx_names,
        all_names,
        names_map,
        bad_description,
        total_events,
        n_channels,
        compensation,
    })
}

// ----------------------------------------------------------------------------------------------
// TSON emission
// ----------------------------------------------------------------------------------------------

const MEASUREMENTS: &str = "Measurements";
const OBSERVATIONS: &str = "Observations";
const VARIABLES: &str = "Variables";
const SUMMARY: &str = "Summary";
const COMPENSATION: &str = "Compensation";
const CHUNK: usize = 1 << 20; // f64s per write buffer (8 MB)
/// Memory the wide (`gather_channels = false`) path may hold for channel columns. Each group
/// of channels costs `total_events × 8 B` per channel, so this fixes peak memory and decides
/// how many decode passes the wide output takes.
const WIDE_BUDGET: usize = 256 << 20; // 256 MB

/// Rows in R's `distinct()` of the wide Observations table; here event_id is unique so it is the
/// row count. Column names as R produces them for the *test* golden (`Event #`, `Time`, ...).
struct ColSpec<'a> {
    name: &'a str,
    ty: &'a str, // "double" | "string" | "int32"
}

fn write_table_header<W: Write>(
    w: &mut TsonWriter<W>,
    name: &str,
    n_rows: usize,
    cols: &[ColSpec],
) -> Result<()> {
    w.map(4)?;
    w.key("kind")?;
    w.str("Table")?;
    w.key("nRows")?;
    w.i32(
        i32::try_from(n_rows).map_err(|_| anyhow!("table {name} has {n_rows} rows > i32::MAX"))?,
    )?;
    w.key("properties")?;
    w.map(4)?;
    w.key("kind")?;
    w.str("TableProperties")?;
    w.key("name")?;
    w.str(name)?;
    w.key("sortOrder")?;
    w.list(0)?;
    w.key("ascending")?;
    w.bool(false)?;
    w.key("columns")?;
    w.list(cols.len())?;
    Ok(())
}

fn write_column_header<W: Write>(w: &mut TsonWriter<W>, c: &ColSpec, n_rows: usize) -> Result<()> {
    w.map(6)?;
    w.key("kind")?;
    w.str("Column")?;
    w.key("name")?;
    w.str(c.name)?;
    w.key("type")?;
    w.str(c.ty)?;
    w.key("nRows")?;
    w.i32(n_rows as i32)?;
    w.key("size")?;
    w.i32(n_rows as i32)?;
    w.key("values")?;
    Ok(())
}

fn write_simple_relation<W: Write>(w: &mut TsonWriter<W>, id: &str) -> Result<()> {
    w.map(3)?;
    w.key("kind")?;
    w.str("SimpleRelation")?;
    w.key("id")?;
    w.str(id)?;
    w.key("index")?;
    w.i32(0)?;
    Ok(())
}

fn write_column_pair<W: Write>(w: &mut TsonWriter<W>, l: &[&str], r: &[&str]) -> Result<()> {
    w.map(3)?;
    w.key("kind")?;
    w.str("ColumnPair")?;
    w.key("lColumns")?;
    w.list(l.len())?;
    for s in l {
        w.str(s)?;
    }
    w.key("rColumns")?;
    w.list(r.len())?;
    for s in r {
        w.str(s)?;
    }
    Ok(())
}

/// `JoinOperator { leftPair, rightRelation: SimpleRelation(id) }`
fn write_join_simple<W: Write>(
    w: &mut TsonWriter<W>,
    l: &[&str],
    r: &[&str],
    id: &str,
) -> Result<()> {
    w.map(4)?;
    w.key("kind")?;
    w.str("JoinOperator")?;
    w.key("joinType")?;
    w.str("")?;
    w.key("leftPair")?;
    write_column_pair(w, l, r)?;
    w.key("rightRelation")?;
    write_simple_relation(w, id)?;
    Ok(())
}

/// The whole `OperatorResult`, streamed to `out`. Returns bytes written.
pub fn write_operator_result<W: Write>(
    out: W,
    plan: &Plan,
    s: &Settings,
    doc_name: &str,
    rep: &Reporter,
) -> Result<u64> {
    let opts = s.read_options();
    let mut w = TsonWriter::new(out)?;
    let gather = s.gather_channels;
    let with_variables = gather;
    let with_comp = !plan.compensation.is_empty();
    let n_tables = (if with_variables { 4 } else { 3 }) + usize::from(with_comp);

    w.map(3)?;
    w.key("kind")?;
    w.str("OperatorResult")?;
    w.key("tables")?;
    w.list(n_tables)?;

    // ---- main table -----------------------------------------------------------------------
    let main_name = if gather { MEASUREMENTS } else { "Wide" };
    if gather {
        let n = plan.measurement_rows();
        let cols = [
            ColSpec {
                name: "event_id",
                ty: "int32",
            },
            ColSpec {
                name: "channel_id",
                ty: "int32",
            },
            ColSpec {
                name: "value",
                ty: "double",
            },
        ];
        write_table_header(&mut w, main_name, n, &cols)?;
        let nc = plan.n_channels;
        // The ids are int32: an event index and a channel index. They were doubles, mirroring
        // the R operator's numeric columns, at 8 bytes each — a third of every gathered result
        // (24.7 B/value measured on a 312 M-value cohort, 7.7 GB). int32 is the platform's
        // ordinary integer type (`.ci`, `.ri`, `fileId` already use it) and every stage after
        // the operator — upload, read-back, parse — scales with these bytes. Ranges are safe:
        // `write_table_header` already refuses a table with more than i32::MAX rows, and the
        // ids never exceed that count.
        // event_id: each id repeated nc times
        write_column_header(&mut w, &cols[0], n)?;
        w.i32_list_header(n)?;
        {
            let mut buf: Vec<i32> = Vec::with_capacity(CHUNK);
            for e in 1..=plan.total_events {
                for _ in 0..nc {
                    buf.push(e as i32);
                }
                if buf.len() + nc > CHUNK {
                    w.i32_chunk(&buf)?;
                    buf.clear();
                }
            }
            w.i32_chunk(&buf)?;
        }
        // channel_id: 1..nc tiled
        write_column_header(&mut w, &cols[1], n)?;
        w.i32_list_header(n)?;
        {
            let mut buf: Vec<i32> = Vec::with_capacity(CHUNK);
            for _ in 0..plan.total_events {
                for c in 1..=nc {
                    buf.push(c as i32);
                }
                if buf.len() + nc > CHUNK {
                    w.i32_chunk(&buf)?;
                    buf.clear();
                }
            }
            w.i32_chunk(&buf)?;
        }
        // value: second decode pass, row-major over gathered channels
        write_column_header(&mut w, &cols[2], n)?;
        w.f64_list_header(n)?;
        let mut buf: Vec<f64> = Vec::with_capacity(CHUNK);
        let mut done = 0usize;
        for f in &plan.files {
            let d = read_file(&f.path, &opts)
                .with_context(|| format!("re-read {}", f.path.display()))?;
            check_same(&d, f)?;
            let rows: Box<dyn Iterator<Item = usize>> = match &f.idx {
                Some(v) => Box::new(v.iter().copied()),
                None => Box::new(0..d.n_events),
            };
            for r in rows {
                for c in 0..nc {
                    buf.push(match f.gathered.get(c) {
                        Some(&p) => d.columns[p][r],
                        None => f64::NAN,
                    });
                }
                if buf.len() + nc > CHUNK {
                    w.f64_chunk(&buf)?;
                    buf.clear();
                }
            }
            done += 1;
            rep.at(
                progress::band(progress::WRITE, done, plan.files.len()),
                format!("Writing values: {done} of {} files", plan.files.len()),
            );
        }
        w.f64_chunk(&buf)?;
    } else {
        // Wide: every channel name (union, first-appearance order), fileId, event_id.
        let n = plan.total_events;
        let mut cols: Vec<ColSpec> = plan
            .all_names
            .iter()
            .map(|nm| ColSpec {
                name: nm.as_str(),
                ty: "double",
            })
            .collect();
        cols.push(ColSpec {
            name: "fileId",
            ty: "int32",
        });
        cols.push(ColSpec {
            name: "event_id",
            ty: "int32",
        });
        write_table_header(&mut w, main_name, n, &cols)?;
        // TSON columns are contiguous, so a wide column needs that channel from every file at
        // once. Holding all decoded files (what R does) costs 8 B × events × channels — 1.9 GB
        // for a 93-file cohort — and grows without bound. Instead, take the channels in groups
        // sized to `WIDE_BUDGET`: each group re-reads the files and keeps only its own columns,
        // so peak memory is fixed and the cost is one extra decode pass per group.
        let per_chan = plan.total_events * 8;
        let group = (WIDE_BUDGET / per_chan.max(1)).clamp(1, plan.all_names.len().max(1));
        let n_groups = plan.all_names.len().div_ceil(group);
        tracing::info!(
            channels = plan.all_names.len(),
            group,
            passes = n_groups,
            budget_mb = WIDE_BUDGET / 1_000_000,
            "wide output: writing channels in groups to bound memory"
        );
        for (gi, names) in plan.all_names.chunks(group).enumerate() {
            // gather this group's columns for every file, then emit them one after another
            let mut acc: Vec<Vec<f64>> = names.iter().map(|_| Vec::with_capacity(n)).collect();
            for f in &plan.files {
                let d = read_file(&f.path, &opts)
                    .with_context(|| format!("re-read {}", f.path.display()))?;
                check_same(&d, f)?;
                for (k, nm) in names.iter().enumerate() {
                    let p = d.params.iter().position(|p| &p.name == nm);
                    match (&f.idx, p) {
                        (Some(v), Some(p)) => acc[k].extend(v.iter().map(|&r| d.columns[p][r])),
                        (None, Some(p)) => acc[k].extend_from_slice(&d.columns[p]),
                        (_, None) => acc[k].extend(std::iter::repeat_n(f64::NAN, f.n_rows)),
                    }
                }
            }
            for (k, col) in acc.iter().enumerate() {
                write_column_header(&mut w, &cols[gi * group + k], n)?;
                w.f64_list(col)?;
            }
            rep.at(
                progress::band(progress::WRITE, gi + 1, n_groups),
                format!("Writing channels: group {} of {n_groups}", gi + 1),
            );
        }
        write_column_header(&mut w, &cols[plan.all_names.len()], n)?;
        let mut file_ids: Vec<i32> = Vec::with_capacity(n);
        for (i, f) in plan.files.iter().enumerate() {
            file_ids.extend(std::iter::repeat_n((i + 1) as i32, f.n_rows));
        }
        w.i32_list(&file_ids)?;
        write_column_header(&mut w, &cols[plan.all_names.len() + 1], n)?;
        w.i32_list_header(n)?;
        let mut buf: Vec<i32> = Vec::with_capacity(CHUNK);
        for e in 1..=n {
            buf.push(e as i32);
            if buf.len() == CHUNK {
                w.i32_chunk(&buf)?;
                buf.clear();
            }
        }
        w.i32_chunk(&buf)?;
    }

    // ---- Observations ----------------------------------------------------------------------
    // R: gathered → select(matches("[a-zA-Z]")) keeps the un-gathered channels here; wide →
    // select(fileId, event_id), so Observations is just event_id + filename.
    {
        let n = plan.total_events;
        let condx_names: &[String] = if gather { &plan.condx_names } else { &[] };
        let mut cols: Vec<ColSpec> = condx_names
            .iter()
            .map(|nm| ColSpec {
                name: nm.as_str(),
                ty: "double",
            })
            .collect();
        cols.push(ColSpec {
            name: "event_id",
            ty: "int32",
        });
        cols.push(ColSpec {
            name: "random_sequence",
            ty: "int32",
        });
        cols.push(ColSpec {
            name: "filename",
            ty: "string",
        });
        write_table_header(&mut w, OBSERVATIONS, n, &cols)?;
        for (ci, nm) in condx_names.iter().enumerate() {
            write_column_header(&mut w, &cols[ci], n)?;
            w.f64_list_header(n)?;
            for f in &plan.files {
                match f.condx.iter().position(|&p| &f.params[p].name == nm) {
                    Some(k) => w.f64_chunk(&f.condx_values[k])?,
                    None => {
                        let nan = vec![f64::NAN; f.n_rows];
                        w.f64_chunk(&nan)?;
                    }
                }
            }
        }
        write_column_header(&mut w, &cols[condx_names.len()], n)?;
        w.i32_list_header(n)?;
        let mut buf: Vec<i32> = Vec::with_capacity(CHUNK.min(n));
        for e in 1..=n {
            buf.push(e as i32);
            if buf.len() == CHUNK {
                w.i32_chunk(&buf)?;
                buf.clear();
            }
        }
        w.i32_chunk(&buf)?;
        // random_sequence: the event's rank within its file (see `fcs::event_ranks`).
        write_column_header(&mut w, &cols[condx_names.len() + 1], n)?;
        w.i32_list_header(n)?;
        for f in &plan.files {
            let r: Vec<i32> = f.ranks.iter().map(|&v| v as i32).collect();
            w.i32_chunk(&r)?;
        }
        write_column_header(&mut w, &cols[condx_names.len() + 2], n)?;
        let n_bytes: usize = plan
            .files
            .iter()
            .map(|f| (f.filename.len() + 1) * f.n_rows)
            .sum();
        w.str_list_iter(
            n_bytes,
            plan.files
                .iter()
                .flat_map(|f| std::iter::repeat_n(f.filename.as_str(), f.n_rows)),
        )?;
    }

    // ---- Variables -------------------------------------------------------------------------
    if with_variables {
        let rows: Vec<NameMapRow> = if plan.bad_description {
            // R: select(channel_name, channel_id) %>% distinct()
            let mut seen = BTreeSet::new();
            plan.names_map
                .iter()
                .filter(|r| seen.insert((r.channel_name.clone(), r.channel_id)))
                .cloned()
                .collect()
        } else {
            plan.names_map.clone()
        };
        let n = rows.len();
        let cols: Vec<ColSpec> = if plan.bad_description {
            vec![
                ColSpec {
                    name: "channel_name",
                    ty: "string",
                },
                ColSpec {
                    name: "channel_id",
                    ty: "int32",
                },
            ]
        } else {
            vec![
                ColSpec {
                    name: "channel_name",
                    ty: "string",
                },
                ColSpec {
                    name: "channel_description",
                    ty: "string",
                },
                ColSpec {
                    name: "channel_name_description",
                    ty: "string",
                },
                ColSpec {
                    name: "channel_id",
                    ty: "int32",
                },
            ]
        };
        write_table_header(&mut w, VARIABLES, n, &cols)?;
        let mut ci = 0;
        write_column_header(&mut w, &cols[ci], n)?;
        ci += 1;
        w.str_list(
            &rows
                .iter()
                .map(|r| r.channel_name.as_str())
                .collect::<Vec<_>>(),
        )?;
        if !plan.bad_description {
            write_column_header(&mut w, &cols[ci], n)?;
            ci += 1;
            w.str_list(
                &rows
                    .iter()
                    .map(|r| r.channel_description.as_str())
                    .collect::<Vec<_>>(),
            )?;
            write_column_header(&mut w, &cols[ci], n)?;
            ci += 1;
            w.str_list(
                &rows
                    .iter()
                    .map(|r| r.channel_name_description.as_str())
                    .collect::<Vec<_>>(),
            )?;
        }
        write_column_header(&mut w, &cols[ci], n)?;
        w.i32_list(&rows.iter().map(|r| r.channel_id as i32).collect::<Vec<_>>())?;
    } else {
        tracing::info!(
            "gather_channels = false: the channel table is not uploaded as a project CSV in v0.1 (R uploads 'Channel-Descriptions-…')"
        );
    }

    // ---- Compensation ----------------------------------------------------------------------
    // R (`utils.R::get_spill_matrix`): the matrix is pivoted long to comp_1 (row detector),
    // comp_2 (column detector), comp_value, and the filename appended in main.R.
    if with_comp {
        let rows = &plan.compensation;
        let cols = [
            ColSpec {
                name: "comp_1",
                ty: "string",
            },
            ColSpec {
                name: "comp_2",
                ty: "string",
            },
            ColSpec {
                name: "comp_value",
                ty: "double",
            },
            ColSpec {
                name: "filename",
                ty: "string",
            },
        ];
        write_table_header(&mut w, COMPENSATION, rows.len(), &cols)?;
        write_column_header(&mut w, &cols[0], rows.len())?;
        w.str_list(&rows.iter().map(|r| r.comp_1.as_str()).collect::<Vec<_>>())?;
        write_column_header(&mut w, &cols[1], rows.len())?;
        w.str_list(&rows.iter().map(|r| r.comp_2.as_str()).collect::<Vec<_>>())?;
        write_column_header(&mut w, &cols[2], rows.len())?;
        w.f64_list(&rows.iter().map(|r| r.comp_value).collect::<Vec<_>>())?;
        write_column_header(&mut w, &cols[3], rows.len())?;
        w.str_list(&rows.iter().map(|r| r.filename.as_str()).collect::<Vec<_>>())?;
    }

    // ---- Summary ---------------------------------------------------------------------------
    {
        let md = summary_markdown(plan);
        let content = base64::engine::general_purpose::STANDARD.encode(md.as_bytes());
        let cols = [
            ColSpec {
                name: "filename",
                ty: "string",
            },
            ColSpec {
                name: "mimetype",
                ty: "string",
            },
            ColSpec {
                name: ".content",
                ty: "string",
            },
        ];
        write_table_header(&mut w, SUMMARY, 1, &cols)?;
        write_column_header(&mut w, &cols[0], 1)?;
        w.str_list(&["FCS_summary.md"])?;
        write_column_header(&mut w, &cols[1], 1)?;
        w.str_list(&["text/markdown"])?;
        write_column_header(&mut w, &cols[2], 1)?;
        w.str_list(&[content.as_str()])?;
    }

    // ---- joinOperators ---------------------------------------------------------------------
    w.key("joinOperators")?;
    w.list(2 + usize::from(with_comp))?;
    // 1. main ⟕ Observations (⟕ Variables), joined to the crosstab by nothing (empty pair)
    w.map(4)?;
    w.key("kind")?;
    w.str("JoinOperator")?;
    w.key("joinType")?;
    w.str("")?;
    w.key("leftPair")?;
    write_column_pair(&mut w, &[], &[])?;
    w.key("rightRelation")?;
    w.map(4)?;
    w.key("kind")?;
    w.str("CompositeRelation")?;
    w.key("id")?;
    w.str(&uuid::Uuid::new_v4().to_string())?;
    w.key("mainRelation")?;
    write_simple_relation(&mut w, main_name)?;
    w.key("joinOperators")?;
    w.list(if with_variables { 2 } else { 1 })?;
    write_join_simple(&mut w, &["event_id"], &["event_id"], OBSERVATIONS)?;
    if with_variables {
        write_join_simple(&mut w, &["channel_id"], &["channel_id"], VARIABLES)?;
    }
    // 2. Compensation, then Summary — the order R saves them in
    if with_comp {
        write_join_simple(&mut w, &[], &[], COMPENSATION)?;
    }
    write_join_simple(&mut w, &[], &[], SUMMARY)?;

    w.flush()?;
    let _ = doc_name;
    Ok(w.bytes)
}

fn check_same(d: &FcsData, f: &FilePlan) -> Result<()> {
    if d.n_events != f.n_events_in_file || d.params.len() != f.params.len() {
        bail!(
            "{} changed between passes ({} vs {} events)",
            f.path.display(),
            d.n_events,
            f.n_events_in_file
        );
    }
    Ok(())
}

/// R: `knitr::kable(df_summ)` under "### Uploaded Data Summary".
pub fn summary_markdown(plan: &Plan) -> String {
    let n_channels = plan.names_map.len();
    let mut lines = vec![
        "### Uploaded Data Summary".to_string(),
        String::new(),
        format!("\nNumber of files: {}", plan.files.len()),
        format!("\nNumber of channels: {}", n_channels),
        format!("\nTotal number of observations: {}", plan.total_events),
        String::new(),
        "### Summary table".to_string(),
        String::new(),
    ];
    let ev: Vec<String> = plan.files.iter().map(|f| f.n_rows.to_string()).collect();
    let w1 = ev
        .iter()
        .map(|s| s.len())
        .max()
        .unwrap_or(0)
        .max("Events".len())
        + 1;
    let w2 = plan
        .files
        .iter()
        .map(|f| f.filename.len())
        .max()
        .unwrap_or(0)
        .max("filename".len())
        + 1;
    lines.push(format!("|{:>w1$}|{:<w2$}|", "Events", "filename"));
    lines.push(format!("|{}:|:{}|", "-".repeat(w1 - 1), "-".repeat(w2 - 1)));
    for (f, e) in plan.files.iter().zip(&ev) {
        lines.push(format!("|{:>w1$}|{:<w2$}|", e, f.filename));
    }
    lines.join("\n") + "\n"
}

/// Human-readable plan summary for the log.
pub fn describe(plan: &Plan) -> String {
    format!(
        "{} file(s), {} events, {} gathered channel(s), {} un-gathered ({:?}), {} measurement rows{}",
        plan.files.len(),
        plan.total_events,
        plan.n_channels,
        plan.condx_names.len(),
        plan.condx_names,
        plan.measurement_rows(),
        if plan.bad_description {
            ", descriptions dropped"
        } else {
            ""
        }
    )
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_leading_quantifier_is_a_literal_as_in_r() {
        // R 4.x: grepl(p, x, ignore.case = TRUE) is TRUE only for "?!Time".
        let p = "?!(.ci|Time|FSC[-_]?[AWH]?|SSC[-_]?[AWH]??|SSC-B[-_]?[AWH]?|Width|Height|Area|Event[-_]?ID|Trigger[-_]?Pulse)";
        let o = super::Settings {
            ungather_pattern: p.into(),
            ..Default::default()
        };
        let re = o.ungather_regex().expect("R accepts this pattern");
        for (name, r) in [
            ("Time", false),
            ("FSC-A", false),
            ("SSC-A", false),
            ("CD3", false),
            ("Event_length", false),
            ("event_id", false),
            ("Width", false),
            ("?!Time", true),
        ] {
            assert_eq!(re.is_match(name), r, "{name}");
        }
        let bad = super::Settings {
            ungather_pattern: "(unclosed".into(),
            ..Default::default()
        };
        assert!(bad.ungather_regex().is_err());
    }

    use super::*;

    fn p(name: &str, desc: &str) -> Param {
        Param {
            index: 1,
            name: name.into(),
            desc: desc.into(),
            bits: 32,
            range: 1024.0,
            e: (0.0, 0.0),
            g: 1.0,
        }
    }

    #[test]
    fn description_rule_matches_r() {
        // NA → name; duplicated desc → name (second occurrence); duplicated NA also → name.
        let params = [
            p("A", "CD3"),
            p("B", ""),
            p("C", "CD3"),
            p("D", ""),
            p("E", "CD4"),
        ];
        assert_eq!(apply_desc_rule(&params), ["CD3", "B", "C", "D", "CD4"]);
    }

    /// Column names the writer emits that are deliberately **not** in `operator.json`:
    /// `fileId` is an internal join key, and the compensation columns exist only when a file
    /// carries a spillover matrix, so declaring them would promise a table that is often absent.
    const UNDECLARED_ON_PURPOSE: &[&str] = &["fileId", "comp_1", "comp_2", "comp_value"];
    /// Declared columns the writer drops when the data forces it: when two files give a channel
    /// different descriptions, the Variables table falls back to name + id (R does the same).
    const DECLARED_BUT_OPTIONAL: &[&str] = &["channel_description", "channel_name_description"];

    /// Every column name the writer produces for the fixture, and the data-dependent ones
    /// (wide mode emits one column per channel, named from the FCS file).
    fn emitted_columns(gathered: bool) -> (BTreeSet<String>, BTreeSet<String>) {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
        let dir = tempfile::tempdir().unwrap();
        crate::download::extract_fcs_entries(&root.join("fcs_test.zip"), dir.path()).unwrap();
        let files: Vec<std::path::PathBuf> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().path())
            .collect();
        let s = Settings {
            gather_channels: gathered,
            ..Settings::default()
        };
        let plan = plan(&files, &s, &Reporter::silent()).unwrap();
        // Both modes name columns from the data. Wide emits one column per channel; gathered
        // keeps the channels matching `ungather_pattern` (Time, Event #, …) as columns on
        // Observations. Neither set can be written into operator.json, which is why both
        // alternatives must allow additional attributes.
        let dynamic: BTreeSet<String> = if gathered {
            plan.condx_names.iter().cloned().collect()
        } else {
            plan.all_names.iter().cloned().collect()
        };
        let mut buf = Vec::new();
        write_operator_result(&mut buf, &plan, &s, "fcs_test.zip", &Reporter::silent()).unwrap();
        let rustson::Value::MAP(m) = rustson::decode_bytes(&buf).unwrap() else {
            panic!("result is not a map")
        };
        let rustson::Value::LST(tables) = &m["tables"] else {
            panic!("no tables")
        };
        let mut emitted = BTreeSet::new();
        for t in tables {
            let rustson::Value::MAP(t) = t else { panic!() };
            let rustson::Value::LST(cols) = &t["columns"] else {
                panic!()
            };
            for c in cols {
                let rustson::Value::MAP(c) = c else { panic!() };
                let rustson::Value::STR(n) = &c["name"] else {
                    panic!()
                };
                emitted.insert(n.clone());
            }
        }
        (emitted, dynamic)
    }

    /// `operator.json`'s `operatorSpec` is hand-written and says what this module will produce.
    /// The platform answers "what columns will this step have?" from it whenever the step has not
    /// run (`getPredictedAttributes`), and `allowAdditionalAttributes` is what tells a caller the
    /// answer is incomplete and the step must be run. Both are easy to get wrong by copying:
    /// 0.1.1's wide alternative had lost the flag, so an agent was told the wide output is
    /// `event_id` + `filename` when it is one column per channel. Check the file against reality.
    #[test]
    fn operator_spec_matches_what_the_writer_emits() {
        let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("operator.json");
        let spec: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&manifest).unwrap()).unwrap();
        let alternatives = spec["operatorSpec"]["outputSpecsV2"][0]["alternatives"]
            .as_array()
            .expect("outputSpecsV2[0].alternatives");

        for gathered in [true, false] {
            let want = if gathered {
                "gather_channels = true"
            } else {
                "gather_channels = false"
            };
            let alt = alternatives
                .iter()
                .find(|a| a["condition"].as_str().unwrap_or("").starts_with(want))
                .unwrap_or_else(|| panic!("no alternative for `{want}`"));

            // The condition string is matched by the platform against the step's property values
            // (`DataStep._matchesCondition`), so it has to name the property and the value.
            let cond = alt["condition"].as_str().unwrap();
            assert!(
                cond.contains("gather_channels")
                    && cond.contains(if gathered { "true" } else { "false" }),
                "condition `{cond}` will not match a step's properties"
            );

            let relations = alt["joinSpec"]["joinOperators"].as_array().unwrap();
            let dynamic_declared = relations.iter().any(|r| {
                r["rightRelation"]["meta_data"].as_array().is_some_and(
                    |md: &Vec<serde_json::Value>| {
                        md.iter().any(|p| {
                            p["key"] == "allowAdditionalAttributes" && p["value"] == "true"
                        })
                    },
                )
            });
            assert!(
                dynamic_declared,
                "the `{want}` alternative must set allowAdditionalAttributes: this operator always \
                 emits tables or columns the spec cannot name (per-channel columns in wide mode, \
                 the compensation table when a file has a spillover matrix), and without the flag \
                 the platform reports the predicted column list as complete"
            );

            let declared: BTreeSet<String> = relations
                .iter()
                .flat_map(|r| r["rightRelation"]["attributes"].as_array().unwrap())
                .map(|a| a["name"].as_str().unwrap().to_string())
                .collect();

            let (emitted, dynamic) = emitted_columns(gathered);

            let undeclared: Vec<&String> = emitted
                .difference(&declared)
                .filter(|c: &&String| !dynamic.contains(*c))
                .filter(|c: &&String| !UNDECLARED_ON_PURPOSE.contains(&c.as_str()))
                .collect();
            assert!(
                undeclared.is_empty(),
                "`{want}`: the writer emits {undeclared:?}, which operator.json does not declare \
                 — add them to the spec, or to UNDECLARED_ON_PURPOSE with the reason"
            );

            let never_produced: Vec<&String> = declared
                .difference(&emitted)
                .filter(|c: &&String| !DECLARED_BUT_OPTIONAL.contains(&c.as_str()))
                .collect();
            assert!(
                never_produced.is_empty(),
                "`{want}`: operator.json promises {never_produced:?}, which the fixture run does \
                 not produce — the spec is stale, or the column is data-conditional and belongs \
                 in DECLARED_BUT_OPTIONAL"
            );
        }
    }

    /// Layer-2 parity: the R operator's golden (`tests/fcs_test.zip` → `test_1_out_{1,2,3}.csv`) must be
    /// reproduced exactly (absTol 1e-6, the R test's tolerance) by `plan` + `write_operator_result`.
    #[test]
    fn r_operator_golden_parity() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
        let dir = tempfile::tempdir().unwrap();
        let n =
            crate::download::extract_fcs_entries(&root.join("fcs_test.zip"), dir.path()).unwrap();
        assert_eq!(n.fcs, 1);
        let mut files = Vec::new();
        for e in std::fs::read_dir(dir.path()).unwrap() {
            files.push(e.unwrap().path());
        }
        let s = Settings {
            gather_channels: true,
            ..Settings::default()
        };
        let plan = plan(&files, &s, &Reporter::silent()).unwrap();
        let mut buf = Vec::new();
        write_operator_result(&mut buf, &plan, &s, "fcs_test.zip", &Reporter::silent()).unwrap();
        let v = rustson::decode_bytes(&buf).unwrap();
        let rustson::Value::MAP(m) = v else {
            panic!("not a map")
        };
        let rustson::Value::LST(tables) = &m["tables"] else {
            panic!()
        };
        let table = |name: &str| -> std::collections::HashMap<String, rustson::Value> {
            for t in tables {
                let rustson::Value::MAP(t) = t else { panic!() };
                let rustson::Value::MAP(p) = &t["properties"] else {
                    panic!()
                };
                if p["name"] == rustson::Value::STR(name.to_string()) {
                    let rustson::Value::LST(cols) = &t["columns"] else {
                        panic!()
                    };
                    return cols
                        .iter()
                        .map(|c| {
                            let rustson::Value::MAP(c) = c else { panic!() };
                            let rustson::Value::STR(n) = &c["name"] else {
                                panic!()
                            };
                            (n.clone(), c["values"].clone())
                        })
                        .collect();
                }
            }
            panic!("table {name} missing");
        };
        let f64s = |v: &rustson::Value| -> Vec<f64> {
            match v {
                rustson::Value::LSTF64(x) => x.clone(),
                rustson::Value::LSTI32(x) => x.iter().map(|&v| v as f64).collect(),
                _ => panic!("not a numeric list"),
            }
        };
        let strs = |v: &rustson::Value| -> Vec<String> {
            match v {
                rustson::Value::LSTSTR(x) => String::from_utf8(x.bytes.clone())
                    .unwrap()
                    .split('\0')
                    .filter(|s| !s.is_empty())
                    .map(String::from)
                    .collect(),
                _ => panic!("not str list"),
            }
        };
        let csv = |name: &str| -> (Vec<String>, Vec<Vec<String>>) {
            let text = std::fs::read_to_string(root.join(name)).unwrap();
            let mut lines = text.lines();
            let parse = |l: &str| {
                l.split(',')
                    .map(|c| c.trim_matches('"').to_string())
                    .collect::<Vec<_>>()
            };
            let header = parse(lines.next().unwrap());
            (header, lines.map(parse).collect())
        };
        let close = |a: f64, b: &str| (a - b.parse::<f64>().unwrap()).abs() <= 1e-6;

        let (h, rows) = csv("test_1_out_1.csv");
        let t = table("Measurements");
        assert_eq!(h, ["event_id", "channel_id", "value"]);
        let (e, c, val) = (
            f64s(&t["event_id"]),
            f64s(&t["channel_id"]),
            f64s(&t["value"]),
        );
        assert_eq!(rows.len(), e.len());
        for (i, r) in rows.iter().enumerate() {
            assert!(
                close(e[i], &r[0]) && close(c[i], &r[1]) && close(val[i], &r[2]),
                "Measurements row {i}"
            );
        }
        let (h, rows) = csv("test_1_out_2.csv");
        let t = table("Variables");
        for (j, col) in h.iter().enumerate() {
            if col == "channel_id" {
                let x = f64s(&t[col]);
                for (i, r) in rows.iter().enumerate() {
                    assert!(close(x[i], &r[j]), "Variables.{col} row {i}");
                }
            } else {
                let x = strs(&t[col]);
                assert_eq!(x.len(), rows.len());
                for (i, r) in rows.iter().enumerate() {
                    assert_eq!(x[i], r[j], "Variables.{col} row {i}");
                }
            }
        }
        let (h, rows) = csv("test_1_out_3.csv");
        let t = table("Observations");
        assert_eq!(t.len(), h.len());
        for (j, col) in h.iter().enumerate() {
            if col == "filename" {
                let x = strs(&t[col]);
                for (i, r) in rows.iter().enumerate() {
                    assert_eq!(x[i], r[j], "Observations.filename row {i}");
                }
            } else {
                let x = f64s(&t[col]);
                for (i, r) in rows.iter().enumerate() {
                    assert!(close(x[i], &r[j]), "Observations.{col} row {i}");
                }
            }
        }
    }

    #[test]
    fn wide_mode_observations_has_no_channel_columns() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
        let dir = tempfile::tempdir().unwrap();
        crate::download::extract_fcs_entries(&root.join("fcs_test.zip"), dir.path()).unwrap();
        let files: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().path())
            .collect();
        let s = Settings::default();
        let plan = plan(&files, &s, &Reporter::silent()).unwrap();
        let mut buf = Vec::new();
        write_operator_result(&mut buf, &plan, &s, "x", &Reporter::silent()).unwrap();
        let rustson::Value::MAP(m) = rustson::decode_bytes(&buf).unwrap() else {
            panic!()
        };
        let rustson::Value::LST(tables) = &m["tables"] else {
            panic!()
        };
        let names: Vec<(String, Vec<String>)> = tables
            .iter()
            .map(|t| {
                let rustson::Value::MAP(t) = t else { panic!() };
                let rustson::Value::MAP(p) = &t["properties"] else {
                    panic!()
                };
                let rustson::Value::STR(n) = &p["name"] else {
                    panic!()
                };
                let rustson::Value::LST(cols) = &t["columns"] else {
                    panic!()
                };
                (
                    n.clone(),
                    cols.iter()
                        .map(|c| {
                            let rustson::Value::MAP(c) = c else { panic!() };
                            let rustson::Value::STR(n) = &c["name"] else {
                                panic!()
                            };
                            n.clone()
                        })
                        .collect(),
                )
            })
            .collect();
        let obs = &names.iter().find(|(n, _)| n == "Observations").unwrap().1;
        assert_eq!(obs, &["event_id", "random_sequence", "filename"]);
        let wide = &names.iter().find(|(n, _)| n == "Wide").unwrap().1;
        assert!(
            wide.contains(&"Time".to_string())
                && wide.ends_with(&["fileId".to_string(), "event_id".to_string()])
        );
        assert!(names.iter().all(|(n, _)| n != "Variables"));
    }

    /// Wide mode must emit the same values whatever `WIDE_BUDGET` allows in one pass: the
    /// grouping is a memory bound, never a change in output. Forcing one channel per group
    /// exercises the multi-pass path that a large cohort takes.
    #[test]
    fn wide_channel_grouping_does_not_change_values() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
        let dir = tempfile::tempdir().unwrap();
        crate::download::extract_fcs_entries(&root.join("fcs_test.zip"), dir.path()).unwrap();
        let files: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().path())
            .collect();
        let s = Settings::default();
        let p0 = plan(&files, &s, &Reporter::silent()).unwrap();
        let mut a = Vec::new();
        write_operator_result(&mut a, &p0, &s, "x", &Reporter::silent()).unwrap();
        // same input, but planned serially (in_flight = 1)
        let s1 = Settings {
            in_flight: 1,
            ..Settings::default()
        };
        let p1 = plan(&files, &s1, &Reporter::silent()).unwrap();
        let mut b = Vec::new();
        write_operator_result(&mut b, &p1, &s1, "x", &Reporter::silent()).unwrap();
        // TableProperties.name is a uuid per table, so compare the decoded columns instead
        let cols = |buf: &[u8]| -> Vec<(String, Vec<u8>)> {
            let rustson::Value::MAP(m) = rustson::decode_bytes(buf).unwrap() else {
                panic!()
            };
            let rustson::Value::LST(tables) = &m["tables"] else {
                panic!()
            };
            let mut out = Vec::new();
            for t in tables {
                let rustson::Value::MAP(t) = t else { panic!() };
                let rustson::Value::LST(cs) = &t["columns"] else {
                    panic!()
                };
                for c in cs {
                    let rustson::Value::MAP(c) = c else { panic!() };
                    let rustson::Value::STR(n) = &c["name"] else {
                        panic!()
                    };
                    out.push((n.clone(), rustson::encode(&c["values"]).unwrap()));
                }
            }
            out
        };
        assert_eq!(cols(&a), cols(&b), "in_flight changed the output");
    }

    #[test]
    fn summary_markdown_shape() {
        let plan = Plan {
            files: vec![],
            condx_names: vec![],
            all_names: vec![],
            names_map: vec![],
            bad_description: false,
            total_events: 0,
            n_channels: 0,
            compensation: vec![],
        };
        let md = summary_markdown(&plan);
        assert!(md.starts_with("### Uploaded Data Summary\n\n\nNumber of files: 0"));
        assert!(md.contains("| Events|filename |"));
    }

    /// Decode an OperatorResult written by `write_operator_result` into its tables, each as a
    /// map from column name to numeric values (int32 or double) — enough for the tests below.
    fn decode_tables(
        buf: &[u8],
    ) -> std::collections::HashMap<String, std::collections::HashMap<String, Vec<f64>>> {
        let v = rustson::decode_bytes(buf).unwrap();
        let rustson::Value::MAP(m) = v else {
            panic!("not a map")
        };
        let rustson::Value::LST(tables) = &m["tables"] else {
            panic!()
        };
        let mut out = std::collections::HashMap::new();
        for t in tables {
            let rustson::Value::MAP(t) = t else { panic!() };
            let rustson::Value::MAP(props) = &t["properties"] else {
                panic!()
            };
            let rustson::Value::STR(name) = &props["name"] else {
                panic!()
            };
            let rustson::Value::LST(cols) = &t["columns"] else {
                panic!()
            };
            let mut table = std::collections::HashMap::new();
            for c in cols {
                let rustson::Value::MAP(c) = c else { panic!() };
                let rustson::Value::STR(cname) = &c["name"] else {
                    panic!()
                };
                let vals: Option<Vec<f64>> = match &c["values"] {
                    rustson::Value::LSTF64(x) => Some(x.clone()),
                    rustson::Value::LSTI32(x) => Some(x.iter().map(|&v| v as f64).collect()),
                    _ => None,
                };
                if let Some(vals) = vals {
                    table.insert(cname.clone(), vals);
                }
            }
            out.insert(name.clone(), table);
        }
        out
    }

    fn plan_fixture(which_lines: Option<usize>) -> (Plan, Vec<u8>) {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
        let dir = tempfile::tempdir().unwrap();
        crate::download::extract_fcs_entries(&root.join("fcs_test.zip"), dir.path()).unwrap();
        let mut files: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().path())
            .collect();
        files.sort();
        let s = Settings {
            gather_channels: true,
            which_lines,
            ..Settings::default()
        };
        let plan = plan(&files, &s, &Reporter::silent()).unwrap();
        let mut buf = Vec::new();
        write_operator_result(&mut buf, &plan, &s, "fcs_test.zip", &Reporter::silent()).unwrap();
        (plan, buf)
    }

    /// `which.lines = k` and "import everything, keep random_sequence ≤ k" are the same draw:
    /// the same events, carrying the same ranks, with the same measurement values.
    #[test]
    fn which_lines_is_a_filter_on_random_sequence() {
        let (full_plan, full_buf) = plan_fixture(None);
        let full = decode_tables(&full_buf);
        let n = full_plan.files[0].n_events_in_file;
        assert_eq!(full_plan.files.len(), 1);
        let obs = &full["Observations"];
        let ranks = &obs["random_sequence"];
        assert_eq!(ranks.len(), n);
        // A permutation of 1..=n.
        let mut sorted: Vec<f64> = ranks.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert_eq!(sorted, (1..=n).map(|v| v as f64).collect::<Vec<_>>());
        // event_id is the original row + 1 in a full single-file import.
        assert_eq!(
            obs["event_id"],
            (1..=n).map(|v| v as f64).collect::<Vec<_>>()
        );

        let k = 50usize;
        let expected: Vec<usize> = (0..n).filter(|&i| ranks[i] <= k as f64).collect();
        let (sub_plan, sub_buf) = plan_fixture(Some(k));
        let sub = decode_tables(&sub_buf);
        assert_eq!(
            sub_plan.files[0].idx.as_ref().unwrap(),
            &expected,
            "which.lines kept other events"
        );
        assert_eq!(sub_plan.files[0].n_rows, k);
        // Ranks survive the subsample, and are exactly 1..=k.
        let sub_ranks = &sub["Observations"]["random_sequence"];
        for (j, &i) in expected.iter().enumerate() {
            assert_eq!(sub_ranks[j], ranks[i], "rank of kept event {i}");
        }
        let mut sr = sub_ranks.clone();
        sr.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert_eq!(sr, (1..=k).map(|v| v as f64).collect::<Vec<_>>());
        // Same measurement values: subsampled event j is full event expected[j].
        let nc = full_plan.n_channels;
        let fv = &full["Measurements"]["value"];
        let sv = &sub["Measurements"]["value"];
        assert_eq!(sv.len(), k * nc);
        for (j, &i) in expected.iter().enumerate() {
            for c in 0..nc {
                let (a, b) = (sv[j * nc + c], fv[i * nc + c]);
                assert!(
                    a == b || (a.is_nan() && b.is_nan()),
                    "value of event {i} channel {c}"
                );
            }
        }
    }

    /// Ranks depend on the seed and the file name only, not on the other files in the archive.
    #[test]
    fn ranks_are_a_function_of_seed_and_filename() {
        let a = crate::fcs::event_ranks(1000, crate::fcs::file_seed(42, "a.fcs"));
        let b = crate::fcs::event_ranks(1000, crate::fcs::file_seed(42, "a.fcs"));
        let c = crate::fcs::event_ranks(1000, crate::fcs::file_seed(43, "a.fcs"));
        let d = crate::fcs::event_ranks(1000, crate::fcs::file_seed(42, "b.fcs"));
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert_ne!(a, d);
        assert_eq!(
            crate::fcs::sample_indices(1000, 10, crate::fcs::file_seed(42, "a.fcs")).len(),
            10
        );
    }

    /// Regenerates the `random_sequence` column of the Observations golden from the writer.
    /// `REGEN_GOLDEN=1 cargo test regen_observations_golden -- --ignored`
    #[test]
    #[ignore]
    fn regen_observations_golden() {
        if std::env::var("REGEN_GOLDEN").is_err() {
            return;
        }
        let (_, buf) = plan_fixture(None);
        let t = decode_tables(&buf);
        let ranks = &t["Observations"]["random_sequence"];
        let ids = &t["Observations"]["event_id"];
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
        let path = root.join("test_1_out_3.csv");
        let text = std::fs::read_to_string(&path).unwrap();
        let mut lines = text.lines();
        let header = lines.next().unwrap();
        let hdr: Vec<&str> = header.split(',').collect();
        let eid_col = hdr
            .iter()
            .position(|h| h.trim_matches('"') == "event_id")
            .unwrap();
        let has = hdr.iter().any(|h| h.trim_matches('"') == "random_sequence");
        let mut out = String::new();
        out.push_str(header);
        if !has {
            out.push_str(",\"random_sequence\"");
        }
        out.push('\n');
        for line in lines {
            if line.trim().is_empty() {
                continue;
            }
            let cells: Vec<&str> = line.split(',').collect();
            let eid: f64 = cells[eid_col].parse().unwrap();
            let r = (0..ids.len())
                .find(|&i| ids[i] == eid)
                .map(|i| ranks[i])
                .unwrap();
            if has {
                let rs_col = hdr
                    .iter()
                    .position(|h| h.trim_matches('"') == "random_sequence")
                    .unwrap();
                let mut c: Vec<String> = cells.iter().map(|s| s.to_string()).collect();
                c[rs_col] = format!("{}", r as i64);
                out.push_str(&c.join(","));
            } else {
                out.push_str(line);
                out.push_str(&format!(",{}", r as i64));
            }
            out.push('\n');
        }
        std::fs::write(&path, out).unwrap();
    }
}
