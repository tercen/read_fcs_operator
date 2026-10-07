# read_fcs_operator (Rust, 3.x; developed as read_fcs_rust_operator) — maintenance guide

Rust port of `tercen/read_fcs_operator` (R, flowCore 2.x; last R release 2.6.0). Reads FCS 2.0/3.0/3.1
files (or a ZIP of them) into Tercen. Built with the `create-rust-operator` skill; first Rust operator of the
CYTOSHRINK port (`tercen-port-plan.md` §18).

## Status (2026-09-19, 01:30)

- `src/fcs.rs` reader: **done and validated** — 22 public fixtures (FlowIO + fcsparser test files, the
  Tercen test FCS) match fcsparser to ≤ 1e-14 relative (21 exact), and match flowCore with the operator's
  exact settings on every file flowCore can read; the reader also reads 6 files flowCore rejects
  (offset discrepancies, `$SPILLOVER` size mismatch, variable-width ints, cyflow) and the Cytek 24-bit files.
  FlowRepository FR-FCM-ZZZ4: 39/39 read (table below).
- Tercen plumbing: **done, runs end to end against local Studio in dev mode.**
  `tests/fcs_test.zip` (the R operator's golden input, `gather_channels = true`): Measurements 20,355 rows,
  Variables 69, Observations 295, Summary — **0 mismatches** against `tests/test_1_out_{1,2,3}.csv`
  (`parity/compare_result_tson.py`). The step in Studio shows the joined output factors
  (event_id, channel_id, value, Event #, Time, Event_length, filename, channel_* , mimetype).
- 93-file spectral cohort (1.28 GB zip, 7.44 M events × 42 gathered channels = 312.5 M measurement rows,
  private data, local Studio only), release build, 16 threads:

  | stage | time |
  |---|---|
  | download + unzip | 5.6 s |
  | plan pass (decode all files in parallel, observation columns) | 0.8 s |
  | write OperatorResult TSON (second decode pass, 7.72 GB) | 10.1 s = 31 M rows/s |
  | upload to Studio FileService (gRPC, 1 MiB chunks) | 180 s = 43 MB/s |
  | Studio RunComputationTask (server ingests the result) | 64 s |
  | **total** | **4 min 22 s**, peak RSS **860 MB** |

  The operator itself is ~17 s; the rest is the server side. The R operator holds the whole cohort in
  R data frames (tens of GB for this size) — this port never does.
- Reviewer pass (2026-09-19 01:45, 14 findings) applied: history rewritten to drop 2.8 GB of generated
  parity CSVs, `.dockerignore`, wide-mode Observations fixed (event_id + filename only, as R), CI smoke test
  made real, seed/threads parsed as doubles, golden parity as a `cargo test`, private ids untracked.
  jemalloc release build re-verified (tests + golden parity OK). Docker image build: see "Image" below.
- Not yet: compensation matrices, CSV channel table for `gather_channels = false`, a Studio golden for wide
  mode, memory model refit from real `stats_d_actual_ram_peak`.

## Tests and goldens

`tests/test.json` uses the **R operator's own goldens** (`test_1_out_{1,2,3,4}.csv` + `.schema`, `fcs_test.zip`,
`gather_channels = true`): a stronger claim than a self-generated golden. Verified 2026-09-19 with 0 mismatches
both by `parity/compare_result_tson.py` on the Studio dev run and by `cargo test output::tests::r_operator_golden_parity`
(absTol 1e-6, the R test's tolerance). `.content` of the summary is skipped (timestamp-free here, but kable
spacing is approximated). Wide mode (`gather_channels = false`, the default) is covered by a structural
`cargo test` only — a Studio golden is still to be generated.

## Progress and task log

`progress.rs`. The operator reports through four monotonic percent bands — download 0-8, planning
pass 8-45, result write 45-70, upload 70-100 — and writes the conditions a user should know about
to the task log, which is what R does with `ctx$log`: differing descriptions for one channel name,
a channel set that differs between files, and a compensation matrix on only some files.

Two things forced the implementation away from the obvious one:

- `TercenLogger::progress` in tercen-rs takes a percent and **drops it**, sending only the message,
  although `TaskProgressEvent` carries `actual` and `total` (`tercen_model.proto:2142`). The events
  are therefore built here, with `actual = percent, total = 100`, so the UI has numbers.
- The two long phases are synchronous and the planning pass is parallel, so reporting goes through
  an unbounded channel (non-blocking `send`, safe from rayon workers) drained by a background task.
  No phase ever waits on the network to report, and a dropped event is logged at debug, never
  fatal.

Events are throttled to percent changes. Without it the upload alone sent one event per MiB: 583
events for a 482 MB result, and would be 7,700 for the 7.7 GB one. Throttled, the same run sends
80.

## Input shapes verified end to end in Studio

| shape | result |
|---|---|
| zip of FCS files, gathered | the golden's four tables, exact parity with the R operator |
| zip of FCS files, wide (the default) | 43 columns incl. `fileId` as a true `int32`; Observations is `event_id` + `filename` only, as R |
| a single FCS document, not a zip | read directly, `filename` = the document name |
| zip whose files disagree on channels (12 vs 72 params) | union of channels, missing cells NaN, union of un-gathered columns |
| zip where every file has a spillover matrix | Compensation relation, 128 rows for 2 files x 8x8 |
| zip where only some files have one | no Compensation relation, logged (R behaviour) |

### Sharp edge: `channel_id` is positional per file

R assigns `channel_id` by position **within each file** (`colnames(data)[!condx] <-
names_map$channel_id`), so when a zip mixes files with different channel sets the same id means a
different marker in different files, and joining Measurements to Variables on `channel_id` fans
out. Measured on a 12-param + 72-param zip: 80 Variables rows over 69 distinct ids, 11 of them
ambiguous (`channel_id` 1 is both `FSC-A` and `7 FlowSOM`).

This port reproduces it rather than renumbering, because renumbering would silently disagree with
the R operator on the same input. It does warn, naming the number of ambiguous ids — R does not.
Importing files with differing panels separately is the way to avoid it.

## Known deviations from the R operator (deliberate unless marked TODO)

- `which.lines`: seeded (`seed`, ChaCha8), sampled rows kept in file order; R is unseeded and random-ordered.
  `which.lines = 0` reads all events (R would return 0 rows).
- Single non-zip document: `filename` = the document name (R: a random temp-file name).
- Zip file order: byte order of the entry path (R `list.files`: session-locale collation); mixed-case names
  can change `event_id`/`fileId` numbering between the two.
- `ungather_pattern` uses the `regex` crate (R: POSIX ERE via TRE) — identical for the default and simple
  alternations; back-references / POSIX classes differ.
- R's `select(matches("[0-9]+|event_id"))` would also melt un-gathered channels whose *names* contain digits
  (producing NA channel_ids); not replicated.
- Zip entries with escaping names abort only if they are FCS; other entries are skipped.
- No progress events (R `ctx$progress`); log lines instead. TODO: tercen-rs has no progress API yet.
- Wide mode writes channels in groups bounded by `WIDE_BUDGET` (R holds every file); gathered mode streams.
- Files flowCore rejects are read; `$PnR` parsed as f64.

## Memory model — why it has no features

The worker runs the operator container with a hard `--memory` limit taken from
`memory_model.json` (`exe_runner.dart`: `--memory $mem --memory-swap $mem`), computed as
`intercept x PROD(coefficient x feature^exponent) + 1.5 x offset` in MB. It also injects
`MALLOC_CONF`, which is why jemalloc is linked.

Every available feature (`n_main`, `n_cols`, `n_rows`) describes the **input** crosstab. For an
import operator that is a single documentId projection — `n_main` resolves to the `qt` schema's
row count, which is 0 here — so no feature says anything about the size of the import. The first
cut of this file declared `n_main` at 0.00001 MB/cell and therefore booked **75 MB** for a run
whose measured peak is 860 MB: an immediate exit 137.

The honest model is a constant, but the platform will not accept one written as such. Install
rejects an empty list with `task.git.estimate.model.file.features.empty`
(`git_operator.dart:478`), so a constant has to be expressed as a degenerate feature. The
evaluator computes `intercept × Π(coefficient × value^exponent) + 1.5 × offset`
(`task_service.dart:1098`), and `exponent: 0.0` makes the term exactly 1 whatever the value is,
including the 0 that `n_main` actually returns — Dart's `pow(0, 0)` is 1. Hence the entry below,
which is a constant wearing a feature's clothes. Do not "fix" it to a non-zero exponent.

Current booking: `1200 + 1.5 x 400` = **1.8 GB**. Measured on the 93-file spectral cohort
(7.44 M events x 43 channels, 16 threads, release build) with the `MALLOC_CONF` the worker
injects, which is the setting production actually runs under:

| mode | peak RSS | wall | result |
|---|---|---|---|
| gathered (`gather_channels = true`) | 695 MB | 14.0 s | 7.72 GB, 312 M rows |
| wide (default) | 854 MB | 16.1 s | 2.81 GB |
| gathered, `which.lines = 5000` | 654 MB | 6.8 s | 483 MB |
| 1 file, 295 events x 69 channels (the golden) | 6 MB | — | 518 KB |

Before the bounds below, the same gathered import peaked at 860 MB and the wide path would have
held every decoded file (about 1.9 GB). 1.8 GB leaves roughly 2x headroom on the worst mode.

What still scales with cohort size is the **Observations** columns, which are retained across the
run at `total_events x un-gathered channels x 8 B` — 60 MB here with one `Time` channel, and the
first thing to reconsider for a cohort several times larger. Everything else is fixed.

### Page cache counts against the limit

A cgroup charges **page cache** to the container, not just anonymous memory, and this operator is
I/O-heavy: importing the spectral cohort writes the 1.28 GB archive, the 1.2 GB of extracted FCS
files and the result, and maps every FCS file twice. The first container run under a 1800 MB limit
was **OOM-killed (exit 137) with an RSS of well under half of it** — the cache was the difference.

`pagecache.rs` therefore hands every large file back to the kernel (`fsync` +
`posix_fadvise(DONTNEED)`) once it is no longer needed: the archive as it downloads, each file as
it is extracted, each FCS file after it is decoded, and the result every 256 MB as it is written.
All of it is advisory and best-effort — without it the operator is still correct, just fatter than
the worker allows.

This is invisible outside a container: on the host the same run peaks at 854 MB and the cache is
simply the machine's to manage.

Verified in the container the worker would use (`--user 1000:1000 --memory 1800M --memory-swap
1800M`, Studio's own network, the production `--taskId/--serviceUri/--token` entry point):

| import | before the fix | after |
|---|---|---|
| 93 files, `which.lines = 5000` | — | exit 0, 19.3 s |
| 93 files, every event (7.44 M, 7.72 GB result) | exit 137 | exit 0, 171.5 s (14.7 s to write, 149 s to upload) |

Peak is kept **independent of the host** by two bounds, which is what makes a fixed booking
defensible at all:

- `Settings::in_flight` (4) caps how many files the planning pass decodes at once. Unbounded
  `par_iter` made peak scale with core count: 16 cores here, 4x that on a 64-core worker.
- `WIDE_BUDGET` (256 MB) caps the channel columns held by the wide (`gather_channels = false`)
  path, which otherwise holds every decoded file (1.9 GB for this cohort, unbounded in cohort
  size). It costs one extra decode pass per channel group: 11 passes and 2 s for this cohort.

Refit from `stats_d_actual_ram_peak` in the task meta once there are real runs.

## Limits

- Measurements rows must fit `i32` (Tercen `nRows`): ≈ 50 M events × 43 channels. Clean error.
- The `filename` string list must be < 4 GiB (`u32` TSON length): ≈ 100 M events × 40-byte names. Panic (abort).
- `FileDocument.size` is clamped to `i32::MAX` for results > 2 GB; the 7.72 GB run showed the server ignores it.
- Disk under `TMPDIR`: input zip + extracted files + 24 B × measurement rows (≈ 10 GB for the 93-file cohort).
- Planning pass runs `files.par_iter()` on the global rayon pool: peak RSS ≈ cores × largest decoded file
  (+ observation columns); the `threads` property bounds the per-file decode pool only.

## Image

Static tier (`scratch`, musl, jemalloc). Local `docker build` 2026-09-19: **6.9 MB compressed** (26.8 MB uncompressed;
budget 20 MB), builds cleanly with `musl-tools` and no extra `CC` setting; `docker run --user 1000:1000` starts and
exits 1 with `TERCEN_TASK_ID environment variable not set` (the CI smoke assertion).


## Dev loop against Studio (see `dev/README.md`)

- gRPC is **not** on the published port 5402; it is port 50051 of the `tercen_studio-tercen-1` container
  (`docker inspect` → 172.42.0.42): `TERCEN_URI=http://172.42.0.42:50051`.
- `DevContext` needs the DataStep to have `model.taskId` → a **done CubeQueryTask** (the UI creates one when
  you project a factor) and at least one `XYAxis`; `dev/make_cubequery.py` and `dev/set_axis.py` do that
  for hand-built steps. With the tercen-studio MCP agents (`agent_add_step`, `agent_configure_table_step`,
  `agent_configure_data_step`) the UI path is scriptable instead.
- Dev save = Python `OperatorContextDev.save`: upload, create `RunComputationTask` with the step's
  CubeQuery, run, wait, then link the step (`state`, `computedRelation`, `model.taskId`) so it shows in
  the workflow (`DEV_NO_LINK=1` to skip).

## Architecture

- `src/fcs.rs` — HEADER (6×8-char offsets, `$BEGINDATA/$ENDDATA` fallback, fcsparser's text_end==data_start
  quirk), TEXT (delimiter doubling; `empty_value` false = operator's `emptyValue = FALSE`), supplemental TEXT,
  `$NEXTDATA` chain (default: **second data set if present**, = the operator's `dataset = 2`), DATA
  (`$DATATYPE` F/D/I; `$BYTEORD` 1,2,3,4 / 4,3,2,1 and 2-byte forms; `$PnB` any 1–8 bytes for integers,
  masked to ⌈log2 $PnR⌉ bits like flowCore/fcsparser), post-processing (`truncate_max_range`: values > $PnR
  → $PnR; optional `linearize` = flowCore `transformation`, off in the operator), seeded `which.lines`
  sampling (`sample_indices`, ChaCha8 — the R operator samples unseeded).
  Output is **column-major** `Vec<Vec<f64>>` (per channel), decoded in 65,536-event row chunks with rayon.
- `src/tson.rs` — streaming TSON writer (byte-compatible with rustson, unit-tested), so the `OperatorResult`
  is written to disk column by column and never held in memory.
- Compensation: `$SPILLOVER`/`SPILL`/`$COMP` is parsed (`fcs::parse_spillover`) and emitted as the
  **Compensation** relation (`comp_1`, `comp_2`, `comp_value`, `filename`), pivoted long exactly as
  `utils.R::get_spill_matrix` does, including the IntelliCyt iQue3 case where the keyword carries
  channel numbers instead of names. As in R it is emitted only when **every** file has a readable
  matrix. A size mismatch (the same files flowCore rejects) is logged and ignored rather than
  failing the import. Verified against the raw keyword on all 26 compensated fixtures, and end to
  end in Studio.
- `src/output.rs` — R-operator output semantics (description rule, `ungather_pattern` regex, channel ids per
  file, `bind_rows`/`melt` NA semantics for unequal channel sets, Summary markdown) and the two-pass write:
  pass 1 decodes every file in parallel for metadata + observation columns; pass 2 re-decodes file by file
  while streaming `value` row-major. Joins: main ⟕ Observations(event_id) ⟕ Variables(channel_id) as a
  CompositeRelation of SimpleRelations named after the tables, plus the Summary join — the shape the R/Python
  clients produce.
- `src/input.rs`, `src/download.rs` — documentId from the column facet; FileService download streamed to disk;
  zip entries filtered to `.fcs|.lmd` (zip-slip refused).
- `src/upload.rs` — streamed `FileService.upload` (1 MiB `ReqUpload` messages); production attaches the file
  to the task (tercen-rs `save_table` semantics); dev creates/runs a `RunComputationTask` and links the step.
- `src/main.rs` (production `--taskId/--serviceUri/--token`), `src/bin/dev.rs`, `src/bin/fcsdump.rs`
  (`meta | csv | bench` CLI used by the parity harness). The skill's `props.rs`/`algorithm.rs` are folded into
  `lib.rs::settings_from_ctx` and `fcs.rs` respectively.
- `parity/` — `reference_fcsparser.py`, `reference_flowcore.R`, `compare.py`; generated CSVs are git-ignored.

## Reference semantics (what "same as the R operator" means)

`utils.R::get_fcs`: `read.FCS(transformation = FALSE, which.lines = NULL, dataset = 2, emptyValue = FALSE,
ignore.text.offset = TRUE, truncate_max_range = <property>)`, then an unseeded `sample()` of `which.lines`
rows. flowCore's `min.limit` is NULL in `read.FCS`, so no lower truncation. Values are raw (no `$PnE`).

Known, deliberate differences: seeded sampling (Seed property); files flowCore rejects are read (superset);
`$PnR` parsed as f64 (flowCore fails on ranges > 2^31 for integer data).

## Performance (this laptop, 16 threads, 2026-09-19)

Decode of one 80,000 × 43 float32 Cytek file (14 MB): 0.055–0.062 s (~60 M values/s, column-major output incl. transpose); threads 1→8 change little on one file.
flowCore `read.FCS` on the same file: 0.29 s. fcsparser: 0.02 s (numpy `fromfile`, a memcpy).
Whole 93-file private Cytek set decode: < 6 s single-threaded including process start-ups; 48 MB peak RSS.
Threads help little on a single 14 MB file (memory-bound); parallelism pays across files → the operator
decodes files in parallel (bounded pool) and pipelines the upload.

## Loading the task (0.1.3)

`src/context.rs` builds the context from the **task** and never fetches the workflow, and is
byte-identical with the copy in `asinh_rust_operator`.

`ProductionContext::from_task_id` does fetch the workflow, to read colour and palette settings
off the step, and fails the run when the step is not in the document it gets back:
`Step '…' not found in workflow`. That happens intermittently in ordinary use — change a
property in the interface and run, and the task can reference a step the saved workflow has not
caught up with. Faris hit it on tercen.com with the asinh operator on 2026-09-20; this operator
had the same failure waiting for it, since it never used a single colour field.

Verified by running the production binary against a task carrying schema ids with a `STEP_ID`
the workflow does not contain, which is the condition that fails: 93 files sampled at 5,000
lines, 482 MB uploaded, 17.9 s, no error.

Replace the module with `ProductionContext::from_task_id_data_only` once tercen-rs#1 is merged
and the lock is bumped.

## Next

Everything the R operator does is implemented and verified; what remains is one behaviour and the
release itself.

1. ~~Release~~ — **done on 2026-09-19.** Tag `0.1.0` at `fd8da1d`; the release workflow pushed
   `ghcr.io/tercen/read_fcs_rust_operator:0.1.0` (and `:latest`) and the install-check step passed.
   Installed on tercen.com from the private repo with a **classic** PAT (`repo` scope); a fine-grained
   `github_pat_…` is rejected by the zipball endpoint and surfaces as
   `task.git.operator.download.failed -- 404`.
2. ~~The GHCR package is private~~ — **made public on 2026-09-19.** The image is now anonymously
   pullable (`0.1.0`, `latest`, 7.1 MB compressed, digest `sha256:096859f5…`), matching how
   `ghcr.io/tercen/plot_operator` is published. The repository stays private. Note for the next
   operator: package visibility cannot be changed through the REST API — there is no such endpoint —
   only in the package's settings page. A private or absent package both answer `403 Forbidden`.
3. ~~The platform has not run the operator~~ — **first successful run on tercen.com, 2026-09-19.**
   Faris ran it on prod and it worked, which is the first time Tercen itself (rather than this repo)
   validated the result shape and the joins. Note the CI install-check does *not* do this: it
   installs from git and returns in two seconds. Broader testing is ongoing; the shapes most worth
   exercising are in `tests/` and in the skill's shape matrix (zip gathered, zip wide, a single
   non-archive file, files that disagree on channels, some-but-not-all with spillover).

### The operator spec (0.1.2)

`operator.json`'s `operatorSpec` is not documentation. When a step has not run, the platform answers
"what columns will this produce?" from it (`DataStep.getPredictedAttributes`), and
`allowAdditionalAttributes` on a relation sets `hasDynamicOutput`, which is what tells a caller the
predicted list is incomplete and the step must be run first. The MCP factor tool and
`workflow_operations` both act on that, so a stale spec misleads agent-built workflows. An **absent**
spec is handled honestly ("run this step to discover its output factors"); a **wrong** one is not.

0.1.1 had the wide alternative without the flag, lost when the file was copied from the R operator.
A caller was therefore told the wide output is `event_id` + `filename`, when it is one column per
channel. Fixed in 0.1.2, on **both** alternatives, because both name columns from the data:

- wide: one column per channel, from `$PnN`;
- gathered: the channels matching `ungather_pattern` stay as columns on Observations (`Time`,
  `Event #`, `Event_length` in the fixture).

Deliberately **not** declared, and why declaring them would be worse:

- the compensation table (`comp_1`, `comp_2`, `comp_value`, `filename`) is emitted only when a file
  carries a spillover matrix — promising it unconditionally trades a missing entry for a false one;
- `fileId` is an internal join key;
- `channel_description` / `channel_name_description` are declared but dropped when two files give a
  channel different descriptions (R does the same), so the spec over-promises those two by design.

`operator_spec_matches_what_the_writer_emits` (in `output.rs`) runs the writer on the fixture in both
modes and compares the emitted column names with the file, in both directions, with the exceptions
above as named lists. It is what stops the spec drifting again.

### Archive handling (0.1.1)

`download.rs` expands archives recursively. Two behaviours go beyond the R operator, both from
uploads that failed in testing on 2026-09-19:

- **A zip inside a zip is opened**, to `MAX_ARCHIVE_DEPTH` (4). Nesting is detected by the entry's
  magic bytes, not its extension, so an archive named anything still works. Each nested archive is
  streamed to disk, expanded into a sibling `<name>.d/` directory and then deleted; the `.d` suffix
  cannot collide with an extracted FCS file, and the `filename` factor uses the basename, so it
  never appears in output. R's `unzip` leaves the inner archive as a file and reports no FCS files.
- **`__MACOSX/…/._name.fcs` entries are skipped.** Finder's AppleDouble metadata keeps the original
  extension, so every extension test says FCS and the content is not. Without this they are read as
  data and fail the header check.

A zip with no FCS files now reports what it held instead (`ExtractReport::explain`): the extensions
found, or the count of archives left unopened at the depth limit.

Deliberately not done:

- The channel-table CSV upload R performs when `gather_channels = false` (`utils.R::upload_df`
  writes a file into a "FCS Annotations" project folder). Writing a project file as a side effect
  of an import is not worth reproducing; the operator logs instead.

Deferred until there are real runs:

- Refit `memory_model.json` from `stats_d_actual_ram_peak` in the task meta.
- A Studio-generated golden for wide mode; `tests/test.json` covers gathered only.

## Conformance: FlowRepository FR-FCM-ZZZ4 (39 files, 15 instrument families; 2026-09-19)

Rust reads all 39. 34 match both fcsparser and flowCore (operator settings) exactly or ≤ 1e-6 relative.
The other 5 are reference limitations, not reader defects:

| file | fcsparser | flowCore | Rust |
|---|---|---|---|
| `3215apc 100004.fcs` (24-bit ints, big-endian) | = Rust | garbage (cannot read 3-byte ints) | plausible values, = fcsparser |
| `…G710 Stained Control.fcs` (truncated file) | refuses | refuses | reads the complete rows, `truncated = true` |
| `Cytomics FC500.LMD`, `Gallios.LMD` (2 data sets) | reads set 1 | reads set 2 (`dataset = 2`) | reads set 2, = flowCore |
| `Stratedigm S1400` | = Rust | refuses | = fcsparser |

Defects this set found in the Rust reader (all fixed the same evening): TEXT `$BEGINDATA/$ENDDATA` must
**not** override a valid header (Accuri C6 files disagree with themselves — header wins, as flowCore's
`ignore.text.offset = TRUE`); `$ENDDATA` written as an exclusive end equal to the file size (Accuri, Partec)
→ clamp; header offset field `-1` (BD FACSAria II) → treat as 0; ±inf values in float data (MVa) → the
parity comparison had to treat equal infinities as equal.
Fixture set: `~/Downloads/FlowRepository_FR-FCM-ZZZ4_files.zip` (public; extract to `fixtures/flowrepo/`, git-ignored).


## Deviation from the R operator: int32 ids (0.1.4)

`event_id` and `channel_id` are `int32`, not the R operator's `double` — a third of every gathered result, and every stage after the operator scales with it (tercen/sci#1659). Both sides of each join carry the same type. Values are identical; the R-parity test reads either type.


## `random_sequence` and `which.lines` are one draw (0.1.5)

`fcs::event_ranks(n, file_seed(seed, filename))` ranks every event of a file once; `which.lines`
keeps rank ≤ k and Observations stores the rank as `random_sequence` (int32). Keep them coupled:
`which_lines_is_a_filter_on_random_sequence` asserts the same events, ranks 1..=k and values for
a subsampled import and a filtered full import. If the sampler changes, the column, the test and
`regen_observations_golden` (`REGEN_GOLDEN=1 cargo test regen_observations_golden -- --ignored`)
change together. The seed is keyed by file name on purpose: an event's rank must not depend on
which other files share the archive.


The platform test maps a golden CSV's columns to its `.schema` sidecar **by position**, not by name: keep the sidecar's column order identical to the CSV header (0.1.5 failed its install with `bad.value -- At column filename … refVal = 93` because the sidecar listed `random_sequence` before `filename` while the CSV had it after).
