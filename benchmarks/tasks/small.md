---
name: remote-docs
scope: small
commit: 8c670aa1b9926cd23badb8a056d2ac68751ad520
expected_duration_band: 5-15 minutes
prompt: |
  In this checkout, add a section headed "## Tunnel troubleshooting checklist" to docs/REMOTE.md.
  Include a numbered list with exactly these checks in order: verify the remote
  is running with `swarmy remote status`; connect with `swarmy remote connect NAME`;
  inspect health with `swarmy doctor --remote NAME`; disconnect another remote
  before connecting if FoundationDB's local port 4500 is occupied. Explain that
  none of these steps exposes database ports publicly. Do not change other files.
  Run make check. Commit locally; do not push or open a pull request.
pass_criterion: |
  `git diff 8c670aa1b9926cd23badb8a056d2ac68751ad520 --name-only` names only
  docs/REMOTE.md; the named heading and all four ordered checks occur under it;
  `make check` exits 0.
---
