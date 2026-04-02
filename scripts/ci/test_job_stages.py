#!/usr/bin/env python3
import json
import os
import re
import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.ci import job_stages


class JobStagesTest(unittest.TestCase):
    def _main_workflow_job_ids(self) -> set[str]:
        workflow_path = REPO_ROOT / ".github" / "workflows" / "Main.yml"
        workflow_text = workflow_path.read_text(encoding="utf-8")

        in_jobs = False
        job_ids: set[str] = set()
        for line in workflow_text.splitlines():
            if not in_jobs:
                if line.strip() == "jobs:":
                    in_jobs = True
                continue

            if line and not line.startswith(" "):
                break

            match = re.match(r"^ ([a-zA-Z0-9_-]+):\s*$", line)
            if match:
                job_ids.add(match.group(1))

        self.assertTrue(job_ids, "failed to parse any top-level jobs from .github/workflows/Main.yml")
        return job_ids

    def test_merge_group_minimal_jobs(self):
        selection = job_stages.compute_job_selection("merge_group", "gh-readonly-queue/main/pr-1-abc", "duckdb/duckdb")
        self.assertEqual(selection.enabled_jobs, ["linux-debug", "linux-release", "tidy-check"])
        self.assertTrue(selection.save_cache)

    def test_main_includes_main_only_jobs(self):
        selection = job_stages.compute_job_selection("push", "main", "duckdb/duckdb")
        self.assertIn("main_julia", selection.enabled_jobs)
        self.assertIn("valgrind", selection.enabled_jobs)
        self.assertTrue(selection.save_cache)

    def test_workflow_dispatch_matches_push_selection(self):
        for ref_name in ["feature/my-branch", "main"]:
            push_selection = job_stages.compute_job_selection("push", ref_name, "duckdb/duckdb")
            workflow_dispatch_selection = job_stages.compute_job_selection(
                "workflow_dispatch", ref_name, "duckdb/duckdb"
            )
            self.assertEqual(workflow_dispatch_selection.enabled_jobs, push_selection.enabled_jobs)
            self.assertEqual(workflow_dispatch_selection.save_cache, push_selection.save_cache)

    def test_regular_branch_excludes_main_only_jobs(self):
        selection = job_stages.compute_job_selection("pull_request", "feature/my-branch", "duckdb/duckdb")
        self.assertNotIn("main_julia", selection.enabled_jobs)
        self.assertNotIn("valgrind", selection.enabled_jobs)
        self.assertFalse(selection.save_cache)

    def test_fork_saves_cache(self):
        selection = job_stages.compute_job_selection("pull_request", "feature/my-branch", "somefork/duckdb")
        self.assertTrue(selection.save_cache)

    def test_writes_github_output(self):
        selection = job_stages.JobSelection(enabled_jobs=["linux-debug"], save_cache=False)
        with tempfile.NamedTemporaryFile(mode="w+", encoding="utf-8", delete=False) as tmp:
            output_path = tmp.name
        try:
            with open(output_path, "a", encoding="utf-8") as f:
                job_stages.write_outputs(selection, f)
            with open(output_path, "r", encoding="utf-8") as f:
                lines = [line.strip() for line in f.readlines() if line.strip()]
        finally:
            os.unlink(output_path)

        self.assertEqual(lines[0], "enabled_jobs=[\"linux-debug\"]")
        self.assertEqual(lines[1], "save_cache=false")

    def test_main_prints_and_writes_outputs(self):
        with tempfile.NamedTemporaryFile(mode="w+", encoding="utf-8", delete=False) as tmp:
            output_path = tmp.name

        old_env = os.environ.get("GITHUB_OUTPUT")
        old_argv = sys.argv
        try:
            os.environ["GITHUB_OUTPUT"] = output_path
            sys.argv = [
                "job_stages.py",
                "--event",
                "merge_group",
                "--ref_name",
                "gh-readonly-queue/main/pr-1-abc",
                "--repository",
                "duckdb/duckdb",
            ]
            rc = job_stages.main()
            self.assertEqual(rc, 0)
            with open(output_path, "r", encoding="utf-8") as f:
                out = f.read()
            self.assertIn("enabled_jobs=", out)
            self.assertIn("save_cache=true", out)
            payload = out.splitlines()[0].split("=", 1)[1]
            self.assertEqual(json.loads(payload), ["linux-debug", "linux-release", "tidy-check"])
        finally:
            sys.argv = old_argv
            if old_env is None:
                os.environ.pop("GITHUB_OUTPUT", None)
            else:
                os.environ["GITHUB_OUTPUT"] = old_env
            os.unlink(output_path)

    def test_all_jobs_matches_main_workflow(self):
        workflow_jobs = self._main_workflow_job_ids()
        missing_in_workflow = job_stages.ALL_JOBS - workflow_jobs
        missing_in_job_stages = workflow_jobs - job_stages.ALL_JOBS

        self.assertFalse(
            missing_in_workflow,
            f"jobs listed in job_stages.ALL_JOBS but missing from Main.yml: {sorted(missing_in_workflow)}",
        )
        self.assertFalse(
            missing_in_job_stages,
            f"jobs in Main.yml but missing from job_stages.ALL_JOBS: {sorted(missing_in_job_stages)}",
        )


if __name__ == "__main__":
    unittest.main()
