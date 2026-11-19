# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Offline tests for fork CI routing, result verification, and patch preparation."""

import copy
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch
from urllib.error import HTTPError

import fork_ci as ci

BASE = "a" * 40
HEAD = "b" * 40
FORK = "contributor/renamed-hbase"


def pull_request():
    return {
        "number": 123,
        "state": "open",
        "html_url": "https://github.com/apache/hbase/pull/123",
        "head": {"sha": HEAD, "ref": "feature/test", "repo": {"full_name": FORK}},
        "base": {"sha": BASE, "ref": "master"},
        "labels": [],
    }


def workflow_run():
    return {
        "id": 42,
        "head_sha": HEAD,
        "head_branch": "feature/test",
        "repository": {"full_name": FORK},
        "head_repository": {"full_name": FORK},
        "path": ci.WORKFLOW,
        "event": "push",
        "status": "completed",
        "conclusion": "success",
        "run_attempt": 1,
        "updated_at": "2026-11-18T12:00:00Z",
    }


def successful_jobs(pr):
    return [{"name": name, "conclusion": "success"}
            for name in sorted(ci.REQUIRED_JOBS | {ci.marker(pr)})]


class ContextTest(unittest.TestCase):
    def setUp(self):
        self.api = Mock()
        self.api.pages.return_value = []
        self.api.request.return_value = {"sha": BASE}

    def resolve(self, **kwargs):
        args = {"api": self.api, "event_name": "push", "event": {}, "repo": FORK,
                "head_sha": HEAD, "ref": "refs/heads/feature/test"}
        args.update(kwargs)
        return ci.resolve_context(**args)

    def test_push_before_pr_uses_upstream_master(self):
        self.assertEqual(self.resolve(), {
            "head_repository": FORK, "head_sha": HEAD, "base_sha": BASE,
            "base_ref": "master", "prerelease": "false",
        })
        self.api.request.assert_called_once_with("repos/apache/hbase/commits/master")

    def test_push_reads_upstream_labels(self):
        pr = pull_request()
        pr["labels"] = [{"name": ci.PRERELEASE_LABEL}]
        self.api.pages.return_value = [pr]
        self.assertEqual(self.resolve()["prerelease"], "true")
        self.api.pages.assert_called_once_with(
            "repos/apache/hbase/pulls", state="open", head="contributor:feature/test")

    def test_renamed_or_unrelated_forks_are_filtered(self):
        pr = pull_request()
        pr["head"]["repo"]["full_name"] = "contributor/other-fork"
        self.api.pages.return_value = [pr]
        self.assertEqual(self.resolve()["prerelease"], "false")
        self.api.request.assert_called_once()

    def test_stale_push_is_rejected(self):
        pr = pull_request()
        pr["head"]["sha"] = "c" * 40
        self.api.pages.return_value = [pr]
        with self.assertRaisesRegex(ValueError, "current head"):
            self.resolve()

    def test_ambiguous_target_is_rejected(self):
        self.api.pages.return_value = [pull_request(), pull_request()]
        with self.assertRaisesRegex(ValueError, "separate branch"):
            self.resolve()

    def test_upstream_push_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "branch in a fork"):
            self.resolve(repo=ci.UPSTREAM)

    def test_tag_is_rejected(self):
        with self.assertRaises(ValueError):
            self.resolve(ref="refs/tags/release")

    def test_external_pr_requires_project_label(self):
        pr = pull_request()
        self.api.request.return_value = pr
        with self.assertRaisesRegex(ValueError, "fork CI"):
            self.resolve(event_name="pull_request", event={"number": 123,
                         "pull_request": pr}, repo=ci.UPSTREAM)

    def test_explicit_project_ci(self):
        pr = pull_request()
        pr["labels"] = [{"name": ci.PROJECT_LABEL}]
        self.api.request.return_value = pr
        context = self.resolve(event_name="pull_request", event={"number": 123,
                               "pull_request": pr}, repo=ci.UPSTREAM)
        self.assertEqual(context["head_repository"], FORK)
        self.assertEqual(context["head_sha"], HEAD)

    def test_same_repository_pr_uses_project_ci(self):
        pr = pull_request()
        pr["head"]["repo"]["full_name"] = ci.UPSTREAM
        self.api.request.return_value = pr
        self.assertEqual(self.resolve(event_name="pull_request", event={"number": 123,
                         "pull_request": pr}, repo=ci.UPSTREAM)["head_repository"], ci.UPSTREAM)

    def test_upstream_rerun_keeps_the_base_of_its_workflow_definitions(self):
        pr = pull_request()
        pr["labels"] = [{"name": ci.PROJECT_LABEL}]
        event_pr = copy.deepcopy(pr)
        pr["base"]["sha"] = "c" * 40
        self.api.request.return_value = pr
        context = self.resolve(event_name="pull_request", event={"number": 123,
                               "pull_request": event_pr}, repo=ci.UPSTREAM)
        self.assertEqual(context["base_sha"], BASE)

    def test_upstream_rerun_after_retargeting_is_rejected(self):
        pr = pull_request()
        pr["labels"] = [{"name": ci.PROJECT_LABEL}]
        event_pr = copy.deepcopy(pr)
        pr["base"]["ref"] = "branch-2.6"
        self.api.request.return_value = pr
        with self.assertRaisesRegex(ValueError, "target changed"):
            self.resolve(event_name="pull_request", event={"number": 123,
                         "pull_request": event_pr}, repo=ci.UPSTREAM)

    def test_invalid_repository_and_sha_are_rejected(self):
        for changes in ({"repo": "owner/repo/../other"}, {"head_sha": "HEAD"}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                self.resolve(**changes)


class BootstrapTest(unittest.TestCase):
    def test_existing_helper_keeps_upstream_tooling(self):
        api = Mock()
        api.request.return_value = {"type": "file"}
        self.assertFalse(ci.bootstrap_required(api, BASE))
        api.request.assert_called_once_with(
            "repos/apache/hbase/contents/dev-support/fork_ci.py", ref=BASE)

    def test_absent_helper_enables_branch_tooling(self):
        api = Mock()
        error = HTTPError("url", 404, "Not Found", {}, None)
        self.addCleanup(error.close)
        api.request.side_effect = error
        self.assertTrue(ci.bootstrap_required(api, BASE))

    def test_permission_rate_limit_and_server_errors_do_not_enable_bootstrap(self):
        for code in (403, 429, 500):
            with self.subTest(code=code):
                api = Mock()
                error = HTTPError("url", code, "API error", {}, None)
                self.addCleanup(error.close)
                api.request.side_effect = error
                with self.assertRaises(HTTPError):
                    ci.bootstrap_required(api, BASE)

    def test_non_file_helper_and_invalid_commit_fail_closed(self):
        api = Mock()
        api.request.return_value = {"type": "symlink"}
        with self.assertRaisesRegex(ValueError, "regular file"):
            ci.bootstrap_required(api, BASE)
        with self.assertRaisesRegex(ValueError, "full commit"):
            ci.bootstrap_required(api, "master")


class EvaluationTest(unittest.TestCase):
    def setUp(self):
        self.pr = pull_request()
        self.run = workflow_run()
        self.jobs = successful_jobs(self.pr)
        self.files = [{"filename": "hbase-client/src/main/java/Example.java"}]
        self.api = Mock()
        self.api.request.side_effect = self.request
        self.api.pages.side_effect = self.pages
        self.api.tree.return_value = {
            "truncated": False,
            "tree": [{"path": name, "type": "tree", "mode": "040000", "sha": BASE}
                     for name in (".github", "dev-support", ".asf.yaml")],
        }

    def request(self, _path, **_params):
        return {"workflow_runs": [self.run]}

    def pages(self, path, _key=None, **_params):
        if path.endswith("/jobs"):
            return iter(self.jobs)
        if path.endswith("/files"):
            return iter(self.files)
        raise AssertionError(path)

    def evaluate(self, previous=None):
        return ci.evaluate(self.api, self.pr, previous)

    def test_complete_fork_run_passes(self):
        state, _, url = self.evaluate()
        self.assertEqual(state, "success")
        self.assertEqual(url, f"https://github.com/{FORK}/actions/runs/42")

    def test_bootstrap_run_cannot_satisfy_the_upstream_status(self):
        for job in self.jobs:
            if job["name"] == ci.marker(self.pr):
                job["name"] += " [bootstrap]"
        state, description, _ = self.evaluate()
        self.assertEqual(state, "pending")
        self.assertIn("Bootstrap", description)

    def test_wrong_workflow_event_head_repository_or_branch_is_ignored(self):
        for field, value in (
                ("path", ".github/workflows/fake.yml"),
                ("event", "pull_request"),
                ("head_sha", "c" * 40),
                ("repository", {"full_name": "attacker/hbase"}),
                ("head_repository", {"full_name": "attacker/hbase"}),
                ("head_branch", "other-branch")):
            with self.subTest(field=field):
                old = self.run[field]
                self.run[field] = value
                self.assertEqual(self.evaluate()[0], "pending")
                self.run[field] = old

    def test_dispatch_in_fork_is_accepted(self):
        self.run["event"] = "workflow_dispatch"
        self.assertEqual(self.evaluate()[0], "success")

    def test_missing_workflow_prompts_setup(self):
        error = HTTPError("url", 404, "Not Found", {}, None)
        self.addCleanup(error.close)
        self.api.request.side_effect = error
        state, description, _ = self.evaluate()
        self.assertEqual(state, "pending")
        self.assertIn("Enable Actions", description)

    def test_api_failure_is_not_treated_as_missing_workflow(self):
        error = HTTPError("url", 403, "Forbidden", {}, None)
        self.addCleanup(error.close)
        self.api.request.side_effect = error
        with self.assertRaises(HTTPError):
            self.evaluate()

    def test_deleted_repository_is_an_error(self):
        self.pr["head"]["repo"] = None
        self.assertEqual(self.evaluate()[0], "error")

    def test_running_run_is_pending(self):
        self.run["status"] = "in_progress"
        self.run["conclusion"] = None
        self.assertEqual(self.evaluate()[0], "pending")

    def test_unsuccessful_conclusions_fail(self):
        for conclusion in ("failure", "cancelled", "timed_out", "skipped", "neutral",
                           "action_required", "startup_failure", None):
            with self.subTest(conclusion=conclusion):
                self.run["conclusion"] = conclusion
                self.assertEqual(self.evaluate()[0], "failure")

    def test_missing_or_skipped_job_is_not_green(self):
        self.jobs.pop()
        self.assertEqual(self.evaluate()[0], "pending")
        self.jobs = successful_jobs(self.pr)
        self.jobs[0]["conclusion"] = "skipped"
        self.assertEqual(self.evaluate()[0], "pending")

    def test_stale_base_is_not_green(self):
        self.pr["base"]["sha"] = "c" * 40
        self.assertEqual(self.evaluate()[0], "pending")

    def test_changed_prerelease_label_requires_new_run(self):
        self.pr["labels"] = [{"name": ci.PRERELEASE_LABEL}]
        self.assertEqual(self.evaluate()[0], "pending")

    def test_ci_changes_require_project_ci(self):
        for name in (".github/workflows/yetus.yml", "dev-support/new_helper.py", ".asf.yaml"):
            with self.subTest(name=name):
                self.files = [{"filename": name}]
                self.assertIn("maintainer", self.evaluate()[1])

    def test_ci_rename_is_detected(self):
        self.files = [{"filename": "helper.py", "previous_filename": "dev-support/helper.py"}]
        self.assertEqual(self.evaluate()[0], "pending")

    def test_stale_ci_definitions_require_rebase_even_without_ci_files_in_pr(self):
        base_tree = self.api.tree.return_value
        head_tree = copy.deepcopy(base_tree)
        head_tree["tree"][0]["sha"] = "c" * 40
        self.api.tree.side_effect = [base_tree, head_tree]
        state, description, _ = self.evaluate()
        self.assertEqual(state, "pending")
        self.assertIn("rebase", description)

    def test_missing_truncated_or_symlinked_ci_definitions_are_not_trusted(self):
        for change in ("missing", "truncated", "symlink"):
            with self.subTest(change=change):
                base_tree = self.api.tree.return_value
                head_tree = copy.deepcopy(base_tree)
                if change == "missing":
                    head_tree["tree"].pop()
                elif change == "truncated":
                    head_tree["truncated"] = True
                else:
                    head_tree["tree"][0]["mode"] = "120000"
                self.api.tree.side_effect = [base_tree, head_tree]
                self.assertEqual(self.evaluate()[0], "pending")

    def test_truncated_file_list_is_not_trusted(self):
        self.files *= 3000
        self.assertEqual(self.evaluate()[0], "pending")

    def test_upstream_fallback_can_verify_ci_changes(self):
        self.pr["labels"] = [{"name": ci.PROJECT_LABEL}]
        self.run["event"] = "pull_request"
        self.run["repository"]["full_name"] = ci.UPSTREAM
        self.files = [{"filename": "dev-support/fork_ci.py"}]
        self.assertEqual(self.evaluate()[0], "success")
        self.api.request.assert_called_once_with(
            "repos/apache/hbase/actions/workflows/yetus.yml/runs", per_page=100,
            head_sha=HEAD, event="pull_request")

    def test_success_cache_only_trusts_upstream_bot_for_current_run_and_configuration(self):
        previous = {"state": "success", "description": ci.success_description(self.pr),
                    "target_url": f"https://github.com/{FORK}/actions/runs/42",
                    "creator": {"login": "github-actions[bot]"},
                    "created_at": "2026-11-18T12:01:00Z"}
        self.assertEqual(self.evaluate(previous)[0], "success")
        self.api.pages.assert_not_called()
        self.api.tree.assert_not_called()
        for field, value in (("creator", {"login": "contributor"}),
                             ("created_at", "2026-11-18T11:00:00Z"),
                             ("description", "some other configuration"),
                             ("target_url", "https://github.com/other/run")):
            with self.subTest(field=field):
                changed = dict(previous, **{field: value})
                self.api.pages.reset_mock()
                self.evaluate(changed)
                self.api.pages.assert_called()

    def test_latest_run_wins(self):
        newer = dict(self.run, id=43, conclusion="failure")
        self.api.request.side_effect = None
        self.api.request.return_value = {"workflow_runs": [self.run, newer]}
        self.assertEqual(self.evaluate()[0], "failure")


class MonitorTest(unittest.TestCase):
    def test_unchanged_status_is_not_reposted(self):
        api = Mock()
        pr = pull_request()
        previous = {"context": ci.STATUS, "state": "pending", "description": "running",
                    "target_url": "https://github.com/run"}
        ci.publish(api, pr, ("pending", "running", "https://github.com/run"), previous)
        api.request.assert_not_called()

    def test_monitor_rejects_forks(self):
        with patch.dict(os.environ, {"GITHUB_REPOSITORY": FORK}), self.assertRaises(ValueError):
            ci.monitor(Mock())

    def test_result_is_not_published_after_push_or_label_or_base_change(self):
        for change in ("head", "base", "labels", "state"):
            with self.subTest(change=change):
                pr = pull_request()
                latest = copy.deepcopy(pr)
                if change in ("head", "base"):
                    latest[change]["sha"] = "c" * 40
                elif change == "labels":
                    latest["labels"] = [{"name": ci.PROJECT_LABEL}]
                else:
                    latest["state"] = "closed"
                api = Mock(budget=70)
                api.pages.return_value = [pr]
                api.request.return_value = latest
                with patch.dict(os.environ, {"GITHUB_REPOSITORY": ci.UPSTREAM}), \
                        patch.object(ci, "current_status", return_value=None), \
                        patch.object(ci, "evaluate", return_value=("success", "passed", "url")), \
                        patch.object(ci, "publish") as publish:
                    ci.monitor(api)
                publish.assert_not_called()

    def test_budget_stops_before_starting_another_pr(self):
        api = Mock(budget=7)
        api.pages.return_value = [pull_request()]
        with patch.dict(os.environ, {"GITHUB_REPOSITORY": ci.UPSTREAM}), \
                patch.object(ci, "evaluate") as evaluate:
            ci.monitor(api)
        evaluate.assert_not_called()

    def test_client_does_not_exceed_its_request_budget(self):
        api = ci.GitHub("unused", budget=0)
        with self.assertRaises(ci.RequestBudgetExceeded):
            api.request("repos/apache/hbase/pulls")

    def test_client_paginates(self):
        api = ci.GitHub("unused")
        api.request = Mock(side_effect=[list(range(100)), [100]])
        self.assertEqual(list(api.pages("repos/apache/hbase/pulls")), list(range(101)))

    def test_client_caches_immutable_trees(self):
        api = ci.GitHub("unused")
        api.request = Mock(return_value={"tree": []})
        api.tree(ci.UPSTREAM, BASE)
        api.tree(ci.UPSTREAM, BASE)
        api.request.assert_called_once_with(f"repos/{ci.UPSTREAM}/git/trees/{BASE}")

    def test_rotation_reaches_every_pr_even_when_only_one_fits_the_budget(self):
        prs = [dict(pull_request(), number=number) for number in range(20)]
        first_prs = []
        for slot in range(len(prs)):
            api = Mock(budget=70)
            api.pages.return_value = prs
            api.request.side_effect = lambda path: copy.deepcopy(
                next(pr for pr in prs if path.endswith(f"/{pr['number']}")))

            def consume_budget(_api, pr, _previous):
                first_prs.append(pr["number"])
                _api.budget = 0
                return "pending", "waiting", "https://github.com/run"

            with patch.dict(os.environ, {"GITHUB_REPOSITORY": ci.UPSTREAM}), \
                    patch.object(ci.time, "time", return_value=slot * 300), \
                    patch.object(ci, "current_status", return_value=None), \
                    patch.object(ci, "evaluate", side_effect=consume_budget), \
                    patch.object(ci, "publish"), patch("builtins.print"):
                # Give each PR its own SHA so duplicate-head handling does not bypass evaluation.
                for number, pr in enumerate(prs):
                    pr["head"] = dict(pr["head"], sha=f"{number:040x}")
                ci.monitor(api)
        self.assertEqual(set(first_prs), set(range(20)))


class PatchTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.upstream = self.root / "upstream"
        self.upstream.mkdir()
        self.git(self.upstream, "init", "-q")
        self.git(self.upstream, "config", "user.email", "test@example.invalid")
        self.git(self.upstream, "config", "user.name", "CI test")
        (self.upstream / "data.txt").write_text("original\n", encoding="utf-8")
        self.save("initial")
        self.initial = ci.git(self.upstream, "rev-parse", "HEAD")
        (self.upstream / "data.txt").write_text("PR change\n", encoding="utf-8")
        (self.upstream / "binary").write_bytes(b"\0\xff\x01")
        self.save("head")
        self.head = ci.git(self.upstream, "rev-parse", "HEAD")
        self.git(self.upstream, "checkout", "--detach", self.initial)
        (self.upstream / "upstream-only").write_text("keep me\n", encoding="utf-8")
        self.save("upstream")
        self.base = ci.git(self.upstream, "rev-parse", "HEAD")
        self.source = self.root / "source"
        self.git(self.root, "clone", "-q", str(self.upstream), str(self.source))
        self.git(self.source, "checkout", "--detach", self.head)
        self.patch = self.root / "input.patch"

    @staticmethod
    def git(directory, *args):
        return subprocess.run(["git", "-C", str(directory), *args], check=True,
                              capture_output=True)

    def save(self, message):
        self.git(self.upstream, "add", ".")
        self.git(self.upstream, "commit", "-qm", message)

    def prepare(self):
        ci.prepare_patch(self.source, self.patch, self.base, self.head, str(self.upstream))

    def test_patch_applies_to_pinned_base_and_preserves_binary_and_upstream_changes(self):
        self.prepare()
        self.assertEqual(ci.git(self.source, "rev-parse", "HEAD"), self.base)
        self.git(self.source, "apply", "--check", str(self.patch))
        self.git(self.source, "apply", str(self.patch))
        self.assertEqual((self.source / "data.txt").read_text(), "PR change\n")
        self.assertEqual((self.source / "upstream-only").read_text(), "keep me\n")
        self.assertEqual((self.source / "binary").read_bytes(), b"\0\xff\x01")

    def test_conflict_fails_before_producing_patch(self):
        (self.upstream / "data.txt").write_text("conflicting upstream change\n")
        self.save("conflict")
        self.base = ci.git(self.upstream, "rev-parse", "HEAD")
        with self.assertRaises(subprocess.CalledProcessError):
            self.prepare()
        self.assertFalse(self.patch.exists())

    def test_wrong_checkout_is_rejected(self):
        self.git(self.source, "checkout", "--detach", self.base)
        with self.assertRaisesRegex(ValueError, "requested head"):
            self.prepare()

    def test_dirty_checkout_is_rejected(self):
        (self.source / "unexpected").write_text("untracked")
        with self.assertRaisesRegex(ValueError, "clean checkout"):
            self.prepare()


class YetusArgumentsTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        source = root / "src"
        support = source / "dev-support"
        support.mkdir(parents=True)
        self.wrapper = support / "jenkins_precommit_github_yetus.sh"
        shutil.copyfile(Path(__file__).with_name(self.wrapper.name), self.wrapper)
        gather = support / "gather_machine_environment.sh"
        gather.write_text("#!/usr/bin/env bash\nexit 0\n")
        gather.chmod(0o755)
        yetus_bin = root / "yetus" / "bin"
        yetus_bin.mkdir(parents=True)
        (yetus_bin / "test-patch").write_text('printf "%s\\n" "$@" > "$CAPTURE"\n')
        self.capture = root / "arguments"
        self.patch = root / "input.patch"
        self.patch.write_text("patch\n")
        self.env = {**os.environ,
            "ARCHIVE_PATTERN_LIST": "*.xml", "DOCKERFILE": str(root / "Dockerfile"),
            "GITHUB_PASSWORD": "unused", "GITHUB_USER": "test", "PATCHDIR": str(root / "output"),
            "PLUGINS": "github,unit", "SET_JAVA_HOME": "/jdk", "SOURCEDIR": str(source),
            "YETUSDIR": str(root / "yetus"), "AUTHOR_IGNORE_LIST": "ignore",
            "BLANKS_EOL_IGNORE_FILE": "ignore", "BLANKS_TABS_IGNORE_FILE": "ignore",
            "CAPTURE": str(self.capture), "GITHUB_ACTIONS": "true", "DEBUG": "false",
            "YETUS_LOCAL_PATCH": str(self.patch), "YETUS_BASE_SHA": BASE,
        }

    def invoke(self):
        return subprocess.run(["bash", str(self.wrapper)], env=self.env, capture_output=True,
                              check=False)

    def adapter_probe(self):
        # Upstream adapter fixture, with a terminating newline added:
        # apache/yetus rel/0.15.0, precommit/src/main/shell/robots.d/githubactions.sh.
        adapter = Path(__file__).parent / "test-data" / "yetus-0.15.0" / "githubactions.sh"
        probe = Path(self.env["YETUSDIR"]) / "bin" / "test-patch"
        probe.write_text('''
PATCH_BRANCH=""
BUILDMODE=patch
for arg in "$@"; do
  case "$arg" in --branch=*) PATCH_BRANCH="${arg#*=}" ;; esac
done
compile_cycle() { :; }
yetus_add_array_element() { :; }
add_docker_env() {
  local name
  for name in "$@"; do
    printf '%s=%s\\0' "$name" "${!name}" >> "$DOCKER_ENV"
  done
}
source "$YETUS_ADAPTER"
printf '%s\\n' "$PATCH_BRANCH" "$GITHUB_EVENT_NAME" "$ROBOTTYPE" "$BUILD_URL" \\
  "$BUILDMODE" > "$CAPTURE"
''')
        self.env.update(
            YETUS_ADAPTER=str(adapter.resolve()), DOCKER_ENV=str(self.capture) + ".docker",
            GITHUB_EVENT_NAME="push", GITHUB_REF="refs/heads/push_actions_test",
            GITHUB_BASE_REF="master", GITHUB_REPOSITORY="HubSpot/hbase",
            GITHUB_RUN_NUMBER="1", GITHUB_RUN_ID="42", GITHUB_SHA=HEAD,
            GITHUB_TOKEN="unused", GITHUB_WORKSPACE=str(self.wrapper.parents[2]),
        )
        return probe

    def test_actual_adapter_preserves_local_patch_base_for_push_and_pr_events(self):
        self.adapter_probe()
        for event in ("push", "pull_request"):
            with self.subTest(event=event):
                self.env["GITHUB_EVENT_NAME"] = event
                result = self.invoke()
                self.assertEqual(result.returncode, 0, result.stderr.decode())
                self.assertEqual(self.capture.read_text().splitlines(), [
                    BASE, "workflow_dispatch", "githubactions",
                    "https://github.com/HubSpot/hbase/actions/runs/42", "patch",
                ])

    def test_actual_adapter_hands_normalized_event_to_container_reexecution(self):
        probe = self.adapter_probe()
        result = self.invoke()
        self.assertEqual(result.returncode, 0, result.stderr.decode())
        docker_env = dict(entry.split("=", 1) for entry in
                          Path(self.env["DOCKER_ENV"]).read_text().rstrip("\0").split("\0"))
        self.assertEqual(docker_env["GITHUB_EVENT_NAME"], "workflow_dispatch")
        self.assertEqual(docker_env["GITHUB_REF"], "refs/heads/push_actions_test")
        self.assertEqual(docker_env["GITHUB_SHA"], HEAD)
        container_env = {**os.environ, **docker_env,
                         **{name: self.env[name] for name in
                            ("YETUS_ADAPTER", "DOCKER_ENV", "CAPTURE")}}
        result = subprocess.run(["bash", str(probe), "--branch=" + BASE, str(self.patch)],
                                env=container_env, capture_output=True, check=False)
        self.assertEqual(result.returncode, 0, result.stderr.decode())
        self.assertEqual(self.capture.read_text().splitlines()[0], BASE)

    def test_actual_adapter_keeps_push_behavior_without_a_local_patch(self):
        self.adapter_probe()
        self.env["YETUS_LOCAL_PATCH"] = ""
        result = self.invoke()
        self.assertEqual(result.returncode, 0, result.stderr.decode())
        self.assertEqual(self.capture.read_text().splitlines(), [
            "push_actions_test", "push", "githubactions",
            "https://github.com/HubSpot/hbase/actions/runs/42", "full",
        ])

    def test_local_patch_is_positional_and_base_is_pinned(self):
        result = self.invoke()
        self.assertEqual(result.returncode, 0, result.stderr.decode())
        args = self.capture.read_text().splitlines()
        self.assertEqual(args[-1], str(self.patch))
        self.assertIn("--git-offline", args)
        self.assertIn("--branch=" + BASE, args)
        self.assertIn("--branch-default=" + BASE, args)
        self.assertIn("--plugins=github,unit,-findbugs,-github", args)
        self.assertNotIn("--github-write-comment", args)

    def test_missing_patch_or_base_fails(self):
        self.env["YETUS_BASE_SHA"] = "HEAD"
        self.assertNotEqual(self.invoke().returncode, 0)
        self.env["YETUS_BASE_SHA"] = BASE
        self.patch.unlink()
        self.assertNotEqual(self.invoke().returncode, 0)

    def test_preserved_launcher_works_when_source_base_has_no_patch_aware_launcher(self):
        tools = self.wrapper.parents[2] / "ci-tools" / "dev-support"
        tools.mkdir(parents=True)
        launcher = tools / self.wrapper.name
        shutil.copyfile(self.wrapper, launcher)
        self.wrapper.write_text("exit 99\n")
        self.wrapper = launcher
        result = self.invoke()
        self.assertEqual(result.returncode, 0, result.stderr.decode())
        args = self.capture.read_text().splitlines()
        self.assertIn("--git-offline", args)
        self.assertEqual(args[-1], str(self.patch))

    def test_jenkins_arguments_are_unchanged(self):
        self.env.update(GITHUB_ACTIONS="false", YETUS_LOCAL_PATCH="", BUILD_URL_ARTIFACTS="url")
        result = self.invoke()
        self.assertEqual(result.returncode, 0, result.stderr.decode())
        args = self.capture.read_text().splitlines()
        self.assertIn("--github-write-comment", args)
        self.assertIn("--plugins=github,unit,-findbugs", args)
        self.assertNotIn("--git-offline", args)
        self.assertNotIn(str(self.patch), args)


if __name__ == "__main__":
    unittest.main()
