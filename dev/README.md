# Dev-loop state (local Studio only)

- `studio.env` (git-ignored): `TERCEN_URI` = gRPC of the Studio tercen container (`docker inspect` IP, port 50051), `TERCEN_TOKEN` (30-day admin token, local instance only).
- A Studio project holding one workflow per case. The ids are per-machine and live in the
  git-ignored `spectral_ids.env`; recreate them with the scripts below rather than reusing mine.
  - `tests/fcs_test.zip` (public, the R operator's own golden) — **parity OK** vs the R goldens.
  - a large multi-file zip for throughput and memory work. Any cohort-scale FCS set does; keep
    real study data out of this repo and off any shared instance.
- Scripts (need a venv with `tercen-python-client` installed `--no-deps` + `pytson` from GitHub, numpy/pandas/polars/requests):
  `add_steps.py <wf> <fileId> <name> [prop=value…]` → TableStep(InMemoryRelation documentId) + DataStep;
  `make_cubequery.py <wf> <dataStep>` → runs a CubeQueryTask and sets `model.taskId` (what the UI does; DevContext needs it);
  `set_axis.py <wf> <dataStep> <taskId>` → default XYAxis (tercen-rs DevContext requires one);
  `make_run_task.py <wf> <dataStep> [--with-file-result]` → a RunComputationTask shaped like the
  platform's, so the **production** binary (`--taskId/--serviceUri/--token`) can be exercised
  without publishing anything.
- **Do not open a step holding a cohort-scale gathered result in the Studio UI**: a 312 M-row
  result hung the server (4 Dart threads at 100%, HTTP dead) until `docker compose restart tercen`.
  Measure throughput with `DEV_NO_UPLOAD=1`, or `DEV_NO_LINK=1` to upload without linking the step,
  or sample with `which.lines`.
- Server validation (`patch.validation.step.ports`): TableStep output port name `table`, DataStep ports `data`, linkType `relation` (fixed in `add_steps.py`).
- Run: `. dev/studio.env; WORKFLOW_ID=… STEP_ID=… OUTPUT_TSON=/tmp/r.tson target/release/dev`.
