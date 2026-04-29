#!/usr/bin/env python3
import json
import os
import re
import shlex
import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.ci import job_stages


class JobStagesTest(unittest.TestCase):
    UNITTEST_TOKEN_RE = re.compile(r"(?:\./)?[\w./-]*unittest(?:\.exe)?$")

    def _workflow_paths(self) -> list[Path]:
        return sorted((REPO_ROOT / ".github" / "workflows").glob("*.yml"))

    def _extract_run_commands(self, workflow_text: str) -> list[tuple[int, str]]:
        commands: list[tuple[int, str]] = []
        lines = workflow_text.splitlines()
        i = 0
        while i < len(lines):
            line = lines[i]
            match = re.match(r"^(\s*)run:\s*(.*)$", line)
            if not match:
                i += 1
                continue

            indent = len(match.group(1))
            value = match.group(2).strip()
            line_no = i + 1

            if value in {"|", ">"}:
                block_lines: list[str] = []
                i += 1
                while i < len(lines):
                    block_line = lines[i]
                    if not block_line.strip():
                        block_lines.append("")
                        i += 1
                        continue

                    block_indent = len(block_line) - len(block_line.lstrip(" "))
                    if block_indent <= indent:
                        break
                    block_lines.append(block_line[indent + 2 :] if len(block_line) > indent + 2 else "")
                    i += 1

                commands.append((line_no, "\n".join(block_lines)))
                continue

            commands.append((line_no, value))
            i += 1

        return commands

    def _has_direct_unittest_invocation(self, command: str) -> bool:
        command_parts = re.split(r"\n|&&|\|\||;", command)
        for raw_part in command_parts:
            part = raw_part.strip()
            if not part:
                continue
            try:
                tokens = shlex.split(part)
            except ValueError:
                tokens = part.split()
            if not tokens:
                continue

            idx = 0
            while idx < len(tokens) and re.match(r"^[A-Za-z_][A-Za-z0-9_]*=.*$", tokens[idx]):
                idx += 1
            if idx >= len(tokens):
                continue

            if self.UNITTEST_TOKEN_RE.match(tokens[idx]):
                return True
        return False

    def _compute_job_selection(
        self,
        event_name: str,
        ref_name: str,
        repository: str,
        skip_tests: bool = False,
        changed_keys: set[str] | None = None,
    ) -> job_stages.JobSelection:
        return job_stages.compute_job_selection(
            job_stages.JobSelectionInput(
                event_name=event_name,
                ref_name=ref_name,
                repository=repository,
                skip_tests=skip_tests,
                changed_keys=changed_keys or set(),
            )
        )

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

    def test_merge_queue_push_minimal_jobs(self):
        selection = self._compute_job_selection("push", "gh-readonly-queue/main/pr-1-abc", "duckdb/duckdb")
        required_jobs = {"linux-debug", "linux-release", "linux-release-tests", "tidy-check"}
        self.assertTrue(required_jobs.issubset(set(selection.enabled_jobs)))
        self.assertTrue(selection.save_cache)

    def test_main_includes_main_only_jobs(self):
        selection = self._compute_job_selection("push", "main", "duckdb/duckdb")
        self.assertIn("main_julia", selection.enabled_jobs)
        self.assertIn("valgrind", selection.enabled_jobs)
        self.assertTrue(selection.save_cache)

    def test_workflow_dispatch_adds_release_jobs(self):
        for ref_name in ["feature/my-branch", "main"]:
            push_selection = self._compute_job_selection("push", ref_name, "duckdb/duckdb")
            workflow_dispatch_selection = self._compute_job_selection("workflow_dispatch", ref_name, "duckdb/duckdb")
            self.assertTrue(set(push_selection.enabled_jobs).issubset(set(workflow_dispatch_selection.enabled_jobs)))
            self.assertTrue(set(job_stages.RELEASE_JOBS).issubset(set(workflow_dispatch_selection.enabled_jobs)))
            self.assertEqual(workflow_dispatch_selection.save_cache, push_selection.save_cache)

    def test_repository_dispatch_adds_release_jobs(self):
        selection = self._compute_job_selection("repository_dispatch", "feature/my-branch", "duckdb/duckdb")
        self.assertTrue(set(job_stages.RELEASE_JOBS).issubset(set(selection.enabled_jobs)))

    def test_regular_branch_excludes_main_only_jobs(self):
        selection = self._compute_job_selection("pull_request", "feature/my-branch", "duckdb/duckdb")
        self.assertNotIn("main_julia", selection.enabled_jobs)
        self.assertNotIn("valgrind", selection.enabled_jobs)
        self.assertFalse(selection.save_cache)

    def test_fork_saves_cache(self):
        selection = self._compute_job_selection("pull_request", "feature/my-branch", "somefork/duckdb")
        self.assertTrue(selection.save_cache)

    def test_julia_changed_key_enables_main_julia_on_pr(self):
        selection = self._compute_job_selection(
            "pull_request", "feature/my-branch", "duckdb/duckdb", changed_keys={"julia"}
        )
        self.assertIn("main_julia", selection.enabled_jobs)

    def test_parse_changed_keys(self):
        parsed = job_stages.parse_changed_keys(" jUlia,tests_slow\nextensions julia ")
        self.assertEqual(parsed, {"julia", "tests_slow", "extensions"})

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
                "push",
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
            selected_jobs = json.loads(payload)
            required_jobs = {"linux-debug", "linux-release", "linux-release-tests", "tidy-check"}
            self.assertTrue(required_jobs.issubset(set(selected_jobs)))
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

    def test_summary_needs_all_other_jobs(self):
        workflow_path = REPO_ROOT / ".github" / "workflows" / "Main.yml"
        workflow_text = workflow_path.read_text(encoding="utf-8")
        workflow_jobs = self._main_workflow_job_ids()

        lines = workflow_text.splitlines()
        in_summary = False
        in_needs = False
        summary_needs: set[str] = set()

        for line in lines:
            if not in_summary:
                if re.match(r"^ summary:\s*$", line):
                    in_summary = True
                continue

            # Leave summary block on next top-level job.
            if re.match(r"^ [a-zA-Z0-9_-]+:\s*$", line):
                break

            if not in_needs:
                if re.match(r"^\s+needs:\s*$", line):
                    in_needs = True
                continue

            need_match = re.match(r"^\s+-\s+([a-zA-Z0-9_-]+)\s*$", line)
            if need_match:
                summary_needs.add(need_match.group(1))
                continue

            if re.match(r"^\s+steps:\s*$", line):
                break

        self.assertTrue(in_summary, "failed to locate 'summary' job in Main.yml")
        self.assertTrue(in_needs, "failed to locate summary.needs in Main.yml")

        expected_needs = workflow_jobs - {"summary"}
        missing_from_summary = expected_needs - summary_needs
        extra_in_summary = summary_needs - expected_needs

        self.assertFalse(
            missing_from_summary,
            f"summary.needs is missing jobs from Main.yml: {sorted(missing_from_summary)}",
        )
        self.assertFalse(
            extra_in_summary,
            f"summary.needs references unknown jobs: {sorted(extra_in_summary)}",
        )

    def test_workflow_unittest_commands_use_run_tests_py(self):
        violations: list[str] = []

        for workflow_path in self._workflow_paths():
            workflow_text = workflow_path.read_text(encoding="utf-8")
            for line_no, command in self._extract_run_commands(workflow_text):
                if not self._has_direct_unittest_invocation(command):
                    continue
                if "scripts/ci/run_tests.py" in command:
                    continue
                snippet = " ".join(command.strip().split())
                if len(snippet) > 180:
                    snippet = snippet[:177] + "..."
                violations.append(f"{workflow_path.relative_to(REPO_ROOT)}:{line_no}: {snippet}")

        if violations:
            formatted = "\n\n".join(f"- {entry}" for entry in violations)
            self.fail("workflow `run:` commands using `unittest` must use scripts/ci/run_tests.py:\n\n" + formatted)


if __name__ == "__main__":
    unittest.main()
