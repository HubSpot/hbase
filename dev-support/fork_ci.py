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

"""Prepare fork-side Yetus builds and report their results from trusted upstream CI."""

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

UPSTREAM = "apache/hbase"
WORKFLOW = ".github/workflows/yetus.yml"
STATUS = "Yetus CI"
PROJECT_LABEL = "use project ci"
PRERELEASE_LABEL = "test-yetus-prerelease"
REQUIRED_JOBS = {
    "Resolve CI context",
    "General / general-check",
    "General / zizmor",
    "Compile / jdk17-hadoop3-compile-check",
    "Unit / small",
    "Unit / medium",
    "Unit / large-wave-1",
    "Unit / large-wave-2",
    "Unit / large-wave-3",
}
SHA = re.compile(r"[0-9a-f]{40}\Z")
REPOSITORY = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+\Z")


class RequestBudgetExceeded(RuntimeError):
    """Leave remaining PRs for a subsequent scheduled run."""


class GitHub:
    """A bounded REST client; credentials are sent only to api.github.com."""

    def __init__(self, token, budget=None):
        self.token = token
        self.budget = budget
        self.trees = {}

    def request(self, path, data=None, **params):
        if not path.startswith("repos/"):
            raise ValueError("Expected a repository API path")
        if self.budget is not None:
            if self.budget <= 0:
                raise RequestBudgetExceeded("Monitor API request budget exhausted")
            self.budget -= 1
        url = "https://api.github.com/" + path
        if params:
            url += "?" + urlencode(params)
        request = Request(
            url,
            data=None if data is None else json.dumps(data).encode(),
            headers={
                "Authorization": "Bearer " + self.token,
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
                "Content-Type": "application/json",
            },
        )
        with urlopen(request, timeout=30) as response:
            return json.load(response)

    def pages(self, path, key=None, **params):
        page = 1
        while True:
            result = self.request(path, per_page=100, page=page, **params)
            items = result if key is None else result[key]
            yield from items
            if len(items) < 100:
                return
            page += 1

    def tree(self, repo, sha):
        key = repository(repo), commit(sha)
        if key not in self.trees:
            self.trees[key] = self.request(f"repos/{key[0]}/git/trees/{key[1]}")
        return self.trees[key]


def repository(value):
    if not REPOSITORY.fullmatch(value):
        raise ValueError("Invalid repository name")
    return value


def commit(value):
    if not SHA.fullmatch(value):
        raise ValueError("Expected a full commit SHA")
    return value


def labels(pr):
    return {label["name"] for label in pr["labels"]}


def project_ci(pr):
    head_repo = pr["head"]["repo"]
    return (head_repo is not None and head_repo["full_name"] == UPSTREAM
            or PROJECT_LABEL in labels(pr))


def resolve_context(api, event_name, event, repo, head_sha, ref):
    """Resolve a push against its upstream PR, or master before the PR exists."""
    repository(repo)
    commit(head_sha)
    if event_name == "pull_request":
        pr = api.request(f"repos/{UPSTREAM}/pulls/{int(event['number'])}")
        if repo != UPSTREAM or not project_ci(pr):
            raise ValueError("This pull request must use fork CI")
        if pr["head"]["sha"] != event["pull_request"]["head"]["sha"]:
            raise ValueError("Pull request changed; use the run of its latest push")
        if pr["base"]["ref"] != event["pull_request"]["base"]["ref"]:
            raise ValueError("Pull request target changed; push or reopen the PR")
    else:
        if repo == UPSTREAM or not ref.startswith("refs/heads/"):
            raise ValueError("Fork CI requires a branch in a fork")
        branch = ref.removeprefix("refs/heads/")
        prs = [pr for pr in api.pages(
            f"repos/{UPSTREAM}/pulls", state="open", head=repo.split("/")[0] + ":" + branch
        ) if pr["head"]["repo"] is not None
            and pr["head"]["repo"]["full_name"] == repo
            and pr["head"]["ref"] == branch]
        if len(prs) > 1:
            raise ValueError("Use a separate branch for each upstream pull request")
        pr = prs[0] if prs else None
        if pr is not None and pr["head"]["sha"] != head_sha:
            raise ValueError("Push is not the upstream PR's current head; push or re-run again")

    if pr is None:
        base_ref = "master"
        base_sha = api.request(f"repos/{UPSTREAM}/commits/master")["sha"]
        prerelease = False
    else:
        base_ref = pr["base"]["ref"]
        # A PR run executes workflow definitions from its event's merge commit. A re-run
        # must not claim to have executed workflow definitions from a newer upstream base.
        base_sha = (event["pull_request"]["base"]["sha"] if event_name == "pull_request"
                    else pr["base"]["sha"])
        head_sha = pr["head"]["sha"]
        repo = pr["head"]["repo"]["full_name"]
        prerelease = PRERELEASE_LABEL in labels(pr)
    return {
        "head_repository": repository(repo),
        "head_sha": commit(head_sha),
        "base_sha": commit(base_sha),
        "base_ref": base_ref,
        "prerelease": str(prerelease).lower(),
    }


def git(source, *args):
    return subprocess.check_output(["git", "-C", str(source), *args], text=True).strip()


def prepare_patch(source, patch, base_sha, head_sha, upstream_url=None):
    """Diff the clean merge against its pinned base, then let Yetus apply that patch."""
    commit(base_sha)
    commit(head_sha)
    if git(source, "rev-parse", "HEAD") != head_sha:
        raise ValueError("Checkout does not match the requested head")
    if git(source, "status", "--porcelain"):
        raise ValueError("Patch preparation requires a clean checkout")
    git(source, "fetch", "--no-tags", upstream_url or f"https://github.com/{UPSTREAM}.git",
        base_sha)
    # merge-tree exits nonzero on a conflict: do not spend hours testing an unmergeable PR.
    tree = git(source, "merge-tree", "--write-tree", base_sha, head_sha).splitlines()[0]
    with patch.open("wb") as output:
        subprocess.run(["git", "-C", str(source), "diff", "--binary", "--full-index",
                        base_sha, tree, "--"], stdout=output, check=True)
    git(source, "checkout", "--detach", base_sha)


def marker(pr):
    prerelease = str(PRERELEASE_LABEL in labels(pr)).lower()
    return (f"Tested base {commit(pr['base']['sha'])} for {pr['base']['ref']} "
            f"(prerelease={prerelease})")


def success_description(pr):
    signature = hashlib.sha256(marker(pr).encode()).hexdigest()[:32]
    return f"All checks passed (configuration {signature})"


def ci_file(path):
    # Include imported helpers and renamed paths, not just workflow YAML.
    return path.startswith((".github/", "dev-support/")) or path == ".asf.yaml"


def changed_ci(api, pr):
    files = list(api.pages(f"repos/{UPSTREAM}/pulls/{pr['number']}/files"))
    # GitHub truncates this endpoint at 3,000 files. An incomplete view cannot establish trust.
    return len(files) >= 3000 or any(
        ci_file(file["filename"]) or ci_file(file.get("previous_filename", ""))
        for file in files
    )


def trusted_ci(api, pr):
    """Require the fork to execute the CI definitions of the tested upstream base."""
    paths = {".github", "dev-support", ".asf.yaml"}

    def definitions(tree):
        if tree.get("truncated"):
            return None
        entries = {entry["path"]: (entry["type"], entry["mode"], entry["sha"])
                   for entry in tree["tree"] if entry["path"] in paths}
        return entries if entries.keys() == paths else None

    upstream = definitions(api.tree(UPSTREAM, pr["base"]["sha"]))
    fork = definitions(api.tree(pr["head"]["repo"]["full_name"], pr["head"]["sha"]))
    return upstream is not None and upstream == fork


def current_status(api, head_sha):
    for status in api.pages(f"repos/{UPSTREAM}/commits/{head_sha}/statuses"):
        if status["context"] == STATUS:
            return status
    return None


def evaluate(api, pr, previous=None):
    """Accept only complete, current runs of the expected workflow and event."""
    head = pr["head"]
    head_sha = commit(head["sha"])
    if head["repo"] is None:
        return "error", "The source repository is unavailable", pr["html_url"]
    repo = UPSTREAM if project_ci(pr) else repository(head["repo"]["full_name"])
    upstream = repo == UPSTREAM
    expected_event = "pull_request" if upstream else None
    actions_url = f"https://github.com/{repo}/actions/workflows/yetus.yml"
    params = {"head_sha": head_sha}
    if expected_event:
        params["event"] = expected_event
    else:
        params["branch"] = head["ref"]
    try:
        runs = api.request(f"repos/{repo}/actions/workflows/yetus.yml/runs",
                           per_page=100, **params)["workflow_runs"]
    except HTTPError as error:
        if error.code != 404:
            raise
        runs = []
    candidates = [run for run in runs
                  if run["head_sha"] == head_sha
                  and run["path"] == WORKFLOW
                  and run["repository"]["full_name"] == repo
                  and run["head_repository"] is not None
                  and run["head_repository"]["full_name"] == head["repo"]["full_name"]
                  and run["event"] in ({"pull_request"} if upstream
                                       else {"push", "workflow_dispatch"})
                  and run["head_branch"] == head["ref"]]
    if not candidates:
        message = ("Add 'use project ci', then push or reopen the PR" if upstream
                   else "Enable Actions in the fork, then push or dispatch Yetus")
        return "pending", message, actions_url
    run = max(candidates, key=lambda item: item["id"])
    url = f"https://github.com/{repo}/actions/runs/{int(run['id'])}"
    if run["status"] != "completed":
        return "pending", "Yetus CI is running or waiting for approval", url
    if run["conclusion"] != "success":
        return "failure", "Yetus CI did not pass; inspect the run and re-run or push", url

    description = success_description(pr)
    if (previous and previous["state"] == "success"
            and previous["description"] == description and previous["target_url"] == url
            and previous["creator"]["login"] == "github-actions[bot]"
            and previous["created_at"] >= run["updated_at"]):
        return "success", description, url

    jobs = list(api.pages(
        f"repos/{repo}/actions/runs/{run['id']}/attempts/{run['run_attempt']}/jobs", "jobs"
    ))
    required = REQUIRED_JOBS | {marker(pr)}
    passed = {job["name"] for job in jobs if job["conclusion"] == "success"}
    if not required <= passed:
        return "pending", "Missing checks or stale base/labels; re-run all Yetus jobs", url
    if not upstream and not trusted_ci(api, pr):
        return "pending", "CI differs from upstream; rebase or ask a maintainer for 'use project ci'", url
    if not upstream and changed_ci(api, pr):
        return "pending", "CI files changed; a maintainer must add 'use project ci'", url
    return "success", description, url


def publish(api, pr, result, previous):
    state, description, url = result
    desired = {"context": STATUS, "state": state, "description": description,
               "target_url": url}
    if previous is None or any(previous.get(key) != value for key, value in desired.items()):
        api.request(f"repos/{UPSTREAM}/statuses/{commit(pr['head']['sha'])}", data=desired)
    print(f"PR #{pr['number']}: {state}: {description}")


def monitor(api):
    if os.environ.get("GITHUB_REPOSITORY") != UPSTREAM:
        raise ValueError("The privileged monitor must run in apache/hbase")
    prs = list(api.pages(f"repos/{UPSTREAM}/pulls", state="open", base="master"))
    if not prs:
        return
    # Rotate independently of API search ordering so the request cap cannot starve older PRs.
    offset = (int(time.time()) // 300) % len(prs)
    prs = prs[offset:] + prs[:offset]
    duplicate_heads = {pr["head"]["sha"] for pr in prs
                       if sum(other["head"]["sha"] == pr["head"]["sha"]
                              for other in prs) > 1}
    errors = []
    for pr in prs:
        if api.budget is not None and api.budget < 8:
            print("Leaving remaining PRs for a later monitor run (API request budget)")
            break
        try:
            previous = current_status(api, commit(pr["head"]["sha"]))
            if pr["head"]["sha"] in duplicate_heads:
                result = ("pending", "Use a distinct head commit for each pull request",
                          f"https://github.com/{UPSTREAM}/pulls")
            else:
                result = evaluate(api, pr, previous)
            # File listings and runs may race a push, label change, or retargeting.
            latest = api.request(f"repos/{UPSTREAM}/pulls/{pr['number']}")
            if (latest["state"] != "open" or latest["head"]["sha"] != pr["head"]["sha"]
                    or marker(latest) != marker(pr) or project_ci(latest) != project_ci(pr)):
                continue
            publish(api, pr, result, previous)
        except RequestBudgetExceeded:
            print("Leaving remaining PRs for a later monitor run (API request budget)")
            break
        except (HTTPError, OSError, ValueError, KeyError) as error:
            # API failures must not turn into a green result or trigger an expensive fallback.
            errors.append(f"PR #{pr['number']}: {error}")
            publish(api, pr, ("error", "Unable to verify CI; the monitor will retry",
                              pr["html_url"]), None)
    if errors:
        raise RuntimeError("\n".join(errors))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("context", "prepare", "monitor"))
    args = parser.parse_args()
    if args.command == "prepare":
        prepare_patch(Path(os.environ["GITHUB_WORKSPACE"]) / "src",
                      Path(os.environ["RUNNER_TEMP"]) / "hbase-ci.patch",
                      os.environ["CI_BASE_SHA"], os.environ["CI_HEAD_SHA"])
        return
    api = GitHub(os.environ["GH_TOKEN"], budget=70 if args.command == "monitor" else None)
    if args.command == "monitor":
        monitor(api)
    else:
        with open(os.environ["GITHUB_EVENT_PATH"], encoding="utf-8") as event_file:
            event = json.load(event_file)
        context = resolve_context(api, os.environ["GITHUB_EVENT_NAME"], event,
                                  os.environ["GITHUB_REPOSITORY"], os.environ["GITHUB_SHA"],
                                  os.environ["GITHUB_REF"])
        with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as output:
            for key, value in context.items():
                if "\n" in value or "\r" in value:
                    raise ValueError("Invalid workflow output")
                print(f"{key}={value}", file=output)


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, ValueError, OSError, KeyError, subprocess.CalledProcessError) as exc:
        print(f"Fork CI: {exc}", file=sys.stderr)
        sys.exit(1)
