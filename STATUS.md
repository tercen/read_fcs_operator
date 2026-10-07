# Status

## 0.1.5 (2026-09-27) — `random_sequence`: the sampling draw as a column

Observations gains `random_sequence` (int32): each event's 1-based rank in a seeded random order
of its file (`fcs::event_ranks`, ChaCha8 Fisher–Yates; the seed is the operator's `seed` mixed
with the file name, so a rank depends on the file and nothing else). `which.lines = k` is now
**defined** as "keep the events with `random_sequence ≤ k`", and the test
`which_lines_is_a_filter_on_random_sequence` proves it: a subsampled import and a full import
filtered on the column select the same events, carrying the same ranks (exactly 1..=k) and the
same measurement values. So one full import serves any number of sampling rates downstream — a
5,000-per-file training draw, a 50,000-per-file QC view, all events for clustering — from the
same cells, reproducibly, without re-importing; and a workflow built on `which.lines` today moves
to a filter on a full import later without changing which cells it analyses. This is the
`downsample_operator` idiom (a seeded rank per observation) applied at import for the one
grouping an FCS archive knows, the file; balancing by condition or cluster stays with that
operator downstream.

Deliberate change: `which.lines` selects different events than 0.1.4 did for the same seed —
the draw is a permutation prefix keyed by file name, where it was `rand::index::sample` keyed by
file position. Costs 4 bytes per event in the result (30 MB on the 7.4 M-event cohort).

Also fixed: `operator.json` still declared `event_id`/`channel_id` as `double` after 0.1.4 (the
spec test compared names only); it now says `int32`, and declares `random_sequence`.

## 0.1.4 (2026-09-27) — event_id and channel_id are int32

They were `double`, mirroring the R operator's numeric columns: 8 bytes each, a third of every
gathered result. Now `int32` — the platform's ordinary integer type (`.ci`, `.ri`, `fileId`
already are) — in every table that carries them: Measurements, Observations, the wide table and
Variables, so every join key changes on both sides at once. Values are unchanged (the R-parity
test passes as before; it reads int32 or double). Ranges are safe: `write_table_header` refuses
more than i32::MAX rows and the ids never exceed the row count.

Full 93-file cohort, all events, gathered: **5.19 GB (16.6 B/value, was 24.7)**. Everything after the
operator scales with those bytes (tercen/sci#1659: upload, read-back, parse).

Golden schemas (`tests/*.csv.schema`) now declare the two columns `int32`; the CSV values were
integers already. Downstream: a table imported before 0.1.4 carries `double` ids and will not join
on these — re-import or convert.

