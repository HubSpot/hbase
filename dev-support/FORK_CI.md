<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Run pull request CI in your fork

For pull requests to `apache/hbase:master`, enable the **Yetus** workflow in your
fork before pushing your PR branch. Tests run in the fork, using its owner's
GitHub Actions capacity. Standard GitHub-hosted runners are free for public repositories;
artifact storage has separate limits.

## Set up your fork

1. Rebase your branch onto `apache/hbase:master` so it contains
   `.github/workflows/yetus.yml` and its supporting scripts.
2. Open your fork's **Actions** tab and enable workflows. Under **Settings >
   Actions > General**, allow the actions used by the HBase workflows. No personal
   access token or upstream credentials are needed.
3. Push your branch to the fork. Open its **Yetus** run to see checks, job summaries,
   and the downloadable Yetus reports. Opening a PR alone does not start a fork run.

The workflow also has a **Run workflow** button. Select your PR branch when using
it. Manual dispatch requires the workflow file on the fork's default branch too;
otherwise push your PR branch to start a run. A push before the PR exists tests
against upstream `master`; once the PR exists,
the workflow reads its upstream target branch and labels. Use a separate source
branch for each PR.

The upstream **Fork CI monitor** publishes the aggregate `Yetus CI` status on your
PR and links to the run in your fork. Its schedule is about every five minutes;
GitHub scheduling delays and the monitor's API request budget can delay reporting.
The monitor never runs your code and never downloads fork artifacts.

## Re-run checks

Use **Re-run all jobs** on the run in your fork, dispatch **Yetus** on your PR branch,
or push another commit. Maintainers of `apache/hbase` do not automatically have
permission to re-run jobs in your fork.

- If Actions was disabled when you pushed, enable it and push again or dispatch
  **Yetus**.
- If upstream `master` changes, re-run all jobs. A green status requires testing
  against the target branch's exact current commit. The branch need not be rebased
  unless it conflicts with that commit or the CI definitions have changed upstream.
- If a maintainer changes the `test-yetus-prerelease` label, re-run all jobs so each
  job uses the selected Yetus version.
- If a run fails or is cancelled, inspect the linked run. Partial re-runs do not
  satisfy the monitor: it verifies a complete set of jobs from one run attempt.

Every build computes a patch that includes binary changes and represents the merge
of your branch into a pinned upstream commit. Yetus applies that patch to the base,
retaining its patch-based module selection and testing the merged result. All test partitions
use the same base; a conflict fails before Maven builds start. A push of an unchanged
upstream commit, such as syncing your fork's `master`, skips the expensive jobs.

## Ask a maintainer to run upstream CI

A maintainer can add the `use project ci` label to run a PR in `apache/hbase`.
Adding the label starts a PR workflow; GitHub may require approval before it runs.
If no run appears, close and reopen the PR or push again. Remove the label to return
to fork CI. PRs from branches inside `apache/hbase` also use upstream CI.

If the target branch moves during an upstream run, push or close and reopen the PR
to test the new base. Re-running an upstream PR event keeps the base associated
with that event's workflow definitions.

Changes under `.github/` or `dev-support/`, including renames, and changes to
`.asf.yaml` require upstream verification. A green fork run cannot verify changes
to the machinery that decides what its own tests do. Ask a maintainer to review
those changes and add `use project ci`. A file list that GitHub truncates also
requires this verification.

The monitor also compares the fork's `.github/` and `dev-support/` trees and
`.asf.yaml` with the tested upstream base. If upstream changed those files after
you branched, rebase to pick them up. Re-running an older workflow against a newer
base does not verify the updated CI definitions.

## Maintain the workflows

### Test the migration before it merges

If the pinned upstream base does not contain `dev-support/fork_ci.py`, the workflow
automatically uses bootstrap mode. It checks out the helper, Yetus launcher, and
summary formatter from the branch's exact head commit into `ci-tools`, separate
from `src`. The launcher remains available after patch preparation checks out the
upstream base in `src`, so Yetus still receives the local patch and pinned-base
arguments. The build token remains read-only.

Bootstrap runs have a ` [bootstrap]` suffix on their tested-base job. They are for
pre-merge feedback and cannot satisfy the upstream `Yetus CI` status. Once the
helper exists on the selected upstream base, a new run automatically uses pinned
upstream tooling. Permission errors, rate limits, and server errors do not enable
bootstrap mode.

### Update routing and reporting

[`yetus.yml`](../.github/workflows/yetus.yml) owns triggers, routing, and the pinned
test context. The three
`yetus-*-check.yml` workflows are reusable implementations. Keep `REQUIRED_JOBS` in
[`dev-support/fork_ci.py`](fork_ci.py) synchronized with their job names and matrix
entries.

[`fork-ci-monitor.yml`](../.github/workflows/fork-ci-monitor.yml) runs only in
`apache/hbase`, from trusted `master` code. Its
token can read the repository and write commit statuses; build jobs have read-only
tokens. The monitor checks the source repository, head commit, branch, workflow
path, event, successful jobs, tested base, and Yetus selection before reporting
success. It verifies matching CI definitions using immutable Git tree and blob
hashes. A newer push, changed labels, or retargeted PR invalidates an in-flight
decision. The monitor caps itself at 70 API calls per invocation and rotates the PR
it starts with, leaving remaining PRs for later runs.

This setup reports PRs targeting `master`. Release branches retain the CI defined
on those branches; migrate them separately before relying on fork reporting there.
Do not use `pull_request_target`, share an upstream token with forks, or execute
fork code in the monitor. A `workflow_run` event in upstream does not observe a
workflow that ran in another repository.

The workflows do not change merge policy. If the project makes CI mandatory,
require the upstream `Yetus CI` commit status, not the skipped upstream workflow
jobs or individual checks from a fork. Configure that requirement through the
project's reviewed [`.asf.yaml`](../.asf.yaml) settings. Existing open PRs must
rebase onto the workflows or use the upstream label before that requirement is enabled.

Run the offline tests after changing routing, patch preparation, or reporting:

```sh
python3 -m unittest discover -s dev-support -p test_fork_ci.py
```

*Written with AI assistance.*
