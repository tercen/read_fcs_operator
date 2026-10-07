> History: written for `tercen/read_fcs_rust_operator` 0.1.x, before it became `read_fcs_operator` 3.0. The image is now `ghcr.io/tercen/read_fcs_operator`.

# Releasing this operator

**Status, 2026-09-19: released.** `0.1.0` is tagged at `fd8da1d`, the image is on GHCR as
`ghcr.io/tercen/read_fcs_rust_operator:0.1.0` and `:latest`, the GitHub release exists, and the
operator is installed on tercen.com. The GHCR package was made **public** the same day, so workers
pull it anonymously; the repository stays private. It ran successfully on tercen.com the same day,
which is the first validation of the output shape by the platform rather than by this repo.

Package visibility is **not** in the REST API. It is changed on the package's settings page, under
Danger Zone. Budget a manual step for it on the next operator.

What follows is the sequence that was used, kept for the next operator.

Originally written before anything had been pushed. This is the sequence from a local repo to an operator a user can
pick in the Tercen UI, and the decisions that belong to a human rather than to the checklist.

## Decisions to make first

| decision | options | note |
|---|---|---|
| Repository visibility | private, then public later | Private is enough to test the whole path. Studio's compose already passes `GITHUB_TOKEN` for private repos and `ghcr.io` pulls. |
| Image visibility | private package, then public | Same. A private package needs the token on every instance that runs the operator. |
| Which library team | `library` (Studio's "main library"), or a scratch team first | The six operators already in Studio's main library are all owned by `library`. |
| Publish to the tercen.com app library | later, separate step | `/publish-operator`, after the private path is proven. |

## Preconditions, all currently true

- `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test` (18) all clean.
- `operator.json` `container` pins `ghcr.io/tercen/read_fcs_rust_operator:0.1.0`, matching the tag
  to be created, and `Cargo.toml` is `0.1.0`. The release rule is: **pin and commit the version
  before tagging it**, never move a published tag.
- `tests/test.json` is an `OperatorUnitTest` referencing five files that all exist, with
  `gather_channels = true` and `absTol 1e-06`. Its goldens are the **R operator's own**, which is
  a stronger claim than a self-generated golden; `cargo test r_operator_golden_parity` reproduces
  them exactly on every build.
- `memory_model.json` books 1.8 GB against a measured worst case of 854 MB. See CLAUDE.md for why
  it declares no features.
- The image builds at **6.9 MB compressed**, well inside the 20 MB static-tier gate, and runs as
  `--user 1000:1000` under a hard `--memory 1800M` against a real task.
- Nothing dataset-specific is tracked: `git ls-files` is 45 files, fixtures and tokens are ignored.

## Sequence

```bash
# 1. create the repo (private first) and push
gh repo create tercen/read_fcs_rust_operator --private \
    --source . --remote origin --description "Read FCS files into Tercen (Rust)"
git push -u origin main

# 2. tag: the release workflow fires on X.Y.Z and does the rest
git tag 0.1.0 && git push origin 0.1.0
```

`.github/workflows/release.yml` then builds and pushes
`ghcr.io/tercen/read_fcs_rust_operator:0.1.0`, and runs `tercenctl operator install` against the
test instance as an install check. **That install step is the gate that matters**: it runs
`tests/test.json` through the platform, which is the first time Tercen itself validates the output
shape rather than this repo comparing tables. A red release burns a patch number, so expect to
tag `0.1.1` if it fails.

It needs three repository or organisation secrets, the same ones
`tercen/ps12image_rust_operator` uses:

```
TERCEN_TEST_OPERATOR_URI
TERCEN_TEST_OPERATOR_USERNAME
TERCEN_TEST_OPERATOR_PASSWORD
```

If they are not set on the new repo the workflow fails at that step with an authentication error,
not a test failure — check that before concluding the operator is broken.

```bash
# 3. install into a library team on the instance you want it on
tercenctl operator install \
    --repo https://github.com/tercen/read_fcs_rust_operator \
    --tag 0.1.0 --team library
```

## After installing

1. Pick **FCS (Rust)** on a data step with a `documentId` column factor and check the six
   properties appear with their defaults.
2. Run it on `tests/fcs_test.zip` and confirm four output tables.
3. Run it on a multi-file zip and watch the progress bar move through its four phases.
4. Compare against the R **FCS** operator on the same input: same four tables, same values.

## If it has to be withdrawn

Deleting a GHCR tag breaks every workflow already pointing at it. Prefer publishing `0.1.1` with
the fix and leaving `0.1.0` in place.
