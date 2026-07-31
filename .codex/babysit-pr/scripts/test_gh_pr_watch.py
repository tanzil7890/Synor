#!/usr/bin/env python3
"""Regression tests for the PR watcher state and merge-safety decisions."""

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


SCRIPT_PATH = Path(__file__).with_name("gh_pr_watch.py")
MODULE_SPEC = importlib.util.spec_from_file_location("gh_pr_watch", SCRIPT_PATH)
gh_pr_watch = importlib.util.module_from_spec(MODULE_SPEC)
MODULE_SPEC.loader.exec_module(gh_pr_watch)


def workflow_run(run_id, run_number, conclusion, workflow_id=10, run_attempt=1):
    return {
        "id": run_id,
        "workflow_id": workflow_id,
        "run_number": run_number,
        "run_attempt": run_attempt,
        "event": "pull_request",
        "head_sha": "head-sha",
        "name": f"workflow-{workflow_id}",
        "status": "completed" if conclusion else "in_progress",
        "conclusion": conclusion,
        "html_url": f"https://example.test/runs/{run_id}",
    }


class MergeReadinessTests(unittest.TestCase):
    def test_unstable_merge_state_is_not_ready(self):
        pull_request = {
            "closed": False,
            "merged": False,
            "mergeable": "MERGEABLE",
            "merge_state_status": "UNSTABLE",
            "review_decision": "APPROVED",
        }
        checks = {
            "all_terminal": True,
            "failed_count": 0,
            "pending_count": 0,
        }

        self.assertFalse(gh_pr_watch.is_pr_ready_to_merge(pull_request, checks, []))


class ReviewBotDetectionTests(unittest.TestCase):
    def test_supported_ai_reviewer_bots_are_actionable(self):
        supported_logins = [
            "chatgpt-codex-connector[bot]",
            "devin-ai-integration[bot]",
            "greptile-apps[bot]",
        ]

        for login in supported_logins:
            with self.subTest(login=login):
                self.assertTrue(gh_pr_watch.is_actionable_review_bot_login(login))


class WorkflowRunSelectionTests(unittest.TestCase):
    def test_newer_success_supersedes_older_failure(self):
        runs = [
            workflow_run(100, 1, "failure"),
            workflow_run(101, 2, "success"),
        ]

        self.assertEqual(
            [], gh_pr_watch.failed_runs_from_workflow_runs(runs, "head-sha")
        )

    def test_newer_pending_run_supersedes_older_failure(self):
        runs = [
            workflow_run(100, 1, "failure"),
            workflow_run(101, 2, None),
        ]

        self.assertEqual(
            [], gh_pr_watch.failed_runs_from_workflow_runs(runs, "head-sha")
        )

    def test_only_latest_failed_attempt_is_retryable(self):
        runs = [
            workflow_run(100, 1, "failure", run_attempt=1),
            workflow_run(101, 2, "timed_out", run_attempt=2),
        ]

        failures = gh_pr_watch.failed_runs_from_workflow_runs(runs, "head-sha")

        self.assertEqual(1, len(failures))
        self.assertEqual(101, failures[0]["run_id"])
        self.assertEqual(2, failures[0]["run_attempt"])

    def test_failures_from_different_workflows_are_preserved(self):
        runs = [
            workflow_run(100, 1, "failure", workflow_id=10),
            workflow_run(200, 1, "timed_out", workflow_id=20),
        ]

        failures = gh_pr_watch.failed_runs_from_workflow_runs(runs, "head-sha")

        self.assertEqual([100, 200], sorted(item["run_id"] for item in failures))


class RetryDeduplicationTests(unittest.TestCase):
    def test_same_failed_attempt_is_not_retried_twice(self):
        pull_request = {
            "repo": "TriFetch/trifetch",
            "number": 2,
            "head_sha": "head-sha",
            "closed": False,
            "merged": False,
        }
        failed_run = {
            "run_id": 100,
            "run_attempt": 2,
            "workflow_name": "CI",
        }
        snapshot = {
            "pr": pull_request,
            "checks": {
                "all_terminal": True,
                "failed_count": 1,
                "pending_count": 0,
            },
            "failed_runs": [failed_run],
            "retry_state": {
                "current_sha_retries_used": 1,
                "max_flaky_retries": 3,
            },
        }
        state = {
            "retries_by_sha": {"head-sha": 1},
            "rerun_attempts_by_sha": {"head-sha": ["100:2"]},
        }

        with tempfile.TemporaryDirectory() as temporary_directory:
            state_path = Path(temporary_directory) / "watcher-state.json"
            state_path.write_text(json.dumps(state), encoding="utf-8")
            args = SimpleNamespace(max_flaky_retries=3)
            with (
                patch.object(
                    gh_pr_watch,
                    "collect_snapshot_unlocked",
                    return_value=(snapshot, state_path),
                ),
                patch.object(gh_pr_watch, "gh_text") as gh_text,
            ):
                result = gh_pr_watch.retry_failed_now_unlocked(
                    args,
                    pull_request,
                    state_path,
                )

        self.assertEqual("failed_run_attempts_already_retried", result["reason"])
        self.assertFalse(result["rerun_attempted"])
        gh_text.assert_not_called()


if __name__ == "__main__":
    unittest.main()
