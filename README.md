# read_fcs_operator

**Version 3 is a Rust implementation** that replaces the R one (2.x, flowCore, kept on the
[`r-legacy`](https://github.com/tercen/read_fcs_operator/tree/r-legacy) branch and the
`r-legacy-2.6.0` tag). It was developed as `tercen/read_fcs_rust_operator` and imported here as
one commit; its development history stays in that repository.

## Changes from 2.x (breaking)

- **`event_id` and `channel_id` are `int32`**, not `double`, in every table. Joins against a 2.x
  result, or a downstream step whose factor type was set from one, must be updated. The integer
  ids roughly halve the stored size of a cohort-scale result.
- **New column `random_sequence`** (int32) in the per-event table: a seeded rank per file that
  reproduces `which.lines` sampling, so a sample can be taken downstream by filtering on it.
- **Same values:** on 2.x's own unit test (`tests/test.json`, the same FCS zip) every value in
  all four tables equals 2.x's expected output; only the types above and the new column differ.
- **New properties:** `seed` (for `random_sequence` and `which.lines`) and `threads`.
- **gRPC operator**, static image; needs a Tercen server with gRPC operator support.

## What it reads

Reads FCS 2.0 / 3.0 / 3.1 files — one file or a zip of `.fcs` / `.lmd` files — into Tercen with the
same output tables as the R operator, decoded on all cores and uploaded as one streamed result. With
`gather_channels = true` (the cohort-scale path) memory stays at about one decoded file per core
regardless of cohort size; the wide default holds all decoded files, as R does.

## Input

A `documentId` column factor referencing the FCS file or zip (first row is used).

A zip may hold the files at any depth, and may hold **other zips** — a folder zipped up with a zip
of FCS files inside it is expanded to four levels. Entries macOS adds when compressing a folder
(`__MACOSX/…/._name.fcs`) are ignored rather than read as data. When a zip yields no FCS files the
error names what it did contain.

The operator declares its input and output shape in `operator.json` (`operatorSpec`), including that
it produces columns named from the data, so a downstream step can be planned before this one runs.

## Output (same as the R operator)

- `gather_channels = true`: **Measurements** `event_id (int32)`, `channel_id (int32)`, `value` ⟕ **Observations**
  (channels matching `ungather_pattern`, `event_id`, `filename`) ⟕ **Variables** (`channel_name`,
  `channel_description`, `channel_name_description`, `channel_id`), plus `FCS_summary.md`.
- `gather_channels = false` (default): the wide table (one column per channel, `fileId`, `event_id`)
  ⟕ Observations, plus `FCS_summary.md`.
- When every file carries a readable `$SPILLOVER`/`SPILL` matrix, a **Compensation** relation
  (`comp_1`, `comp_2`, `comp_value`, `filename`) is added, matching the R operator.

Reading matches flowCore `read.FCS(transformation = FALSE, dataset = 2, emptyValue = FALSE,
ignore.text.offset = TRUE, truncate_max_range = …)`: raw values, no `$PnE` linearisation, values above
`$PnR` clamped when `truncate_max_range` is on, the second data set when a file has several.

## Properties

| name | default | meaning |
|---|---|---|
| `which.lines` | -1 | events per file; -1 = all, otherwise the events whose `random_sequence` ≤ k (a seeded random sample) |
| `gather_channels` | false | long format with a Variables table |
| `ungather_pattern` | `time\|event` | case-insensitive regex of channel names kept as observation columns |
| `truncate_max_range` | true | clamp values above `$PnR` |
| `seed` | 42 | seed for the per-file event ranking (`random_sequence`, hence `which.lines`); ChaCha8, mixed with the file name; the R operator sampled unseeded |
| `threads` | 0 | decode threads per file, 0 = all cores |

## Differences from the R operator

- Sampling is seeded and sampled rows keep file order (R: unseeded, random order).
- Files flowCore rejects (offset disagreements, `$SPILLOVER` size mismatch, 24-bit integers,
  truncated files) are read; see `CLAUDE.md` for the conformance table.
- With `gather_channels = false` the channel table is not uploaded as a project CSV, as R does; it
  is logged instead.

## Development

```bash
# unit tests + TSON writer byte-compatibility
PROTOC=$(which protoc) cargo test

# dev run against a Tercen step (gRPC endpoint, e.g. Studio's tercen container port 50051)
export TERCEN_URI=http://<host>:50051 TERCEN_TOKEN=… WORKFLOW_ID=… STEP_ID=…
OUTPUT_TSON=/tmp/result.tson cargo run --release --bin dev
# DEV_NO_UPLOAD=1 skips the upload; DEV_NO_LINK=1 uploads but does not link the step.

# parity against the R operator's goldens
python parity/compare_result_tson.py /tmp/result.tson tests/
```

`fcsdump meta|csv|bench <file>` inspects a file or dumps it for the parity harness
(`parity/reference_fcsparser.py`, `parity/reference_flowcore.R`, `parity/compare.py`).
