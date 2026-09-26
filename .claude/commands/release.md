Cut a Pyronova release by running the checklist in `docs/release-pipeline.md`, steps
1–10, in order. That document is the spec: which machine, which command, what counts as
a pass. Don't add, skip or reorder steps here.

- Fail closed: any red step on either platform stops the release. Report a table
  (step × platform → pass/fail with the number); for a failure, the real output.
- Before editing an existing test, report why it failed (step 5) and wait for approval.
- The tag push publishes to PyPI and can't be undone: confirm with the owner right
  before step 10.2.
