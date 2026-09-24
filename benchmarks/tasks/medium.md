---
name: remote-name-validation
scope: medium
commit: 8c670aa1b9926cd23badb8a056d2ac68751ad520
expected_duration_band: 15-35 minutes
prompt: |
  In crates/swarmy-config, tighten validate_remote_name so a name cannot start
  or end with a hyphen or underscore. Keep interior hyphens and underscores
  valid. Add unit tests named remote_name_rejects_edge_separators and
  remote_name_accepts_interior_separators covering all four invalid edge cases
  and at least two valid examples. Update the validation error text. Change no
  other crate. Run cargo test -p swarmy-config --locked. Commit locally; do not
  push or open a pull request.
pass_criterion: |
  `git diff 8c670aa1b9926cd23badb8a056d2ac68751ad520 --name-only` contains
  only paths in crates/swarmy-config; both named tests exist and
  `cargo test -p swarmy-config --locked remote_name_` exits 0.
---
