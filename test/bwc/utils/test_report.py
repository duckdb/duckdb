from datetime import datetime
import time
from utils.logger import make_logger
import os
import json
from difflib import unified_diff

logger = make_logger(__name__)


class TestReport:
    step_names = ["serialize_queries_plans", "execute_all_plans_from_file", "serialize_results", "compare_results"]

    def __init__(self, test_spec, old_version, new_version, exit_on_failure=False):
        self.test_filename = test_spec.test_absolute_filename
        self.test_relative_path = test_spec.test_spec_relative_path
        self.test_runtime_directory = test_spec.test_runtime_directory  # Test execution directory
        self.comparison_results_file_name = f"{test_spec.results_new_file_name}.report"
        self.comparison_results = None
        self.start_time = time.time()
        self.exit_on_failure = exit_on_failure
        self.last_timestamp = self.start_time
        self.durations = []
        self.outputs = []
        self.duckdb_old_version = old_version
        self.duckdb_new_version = new_version

    def end_cached_step(self, step_name, *args):
        str_args = ", ".join([f"'{arg}'" for arg in args])
        query = f"CALL {step_name}({str_args});"
        return self.end_step(step_name, {"success": True, "output": ["cached"], "error": None, "query": query})

    def end_step(self, step_name, step_output):
        if self.last_timestamp is None:
            raise ValueError("Test has not been started.")

        if step_name not in TestReport.step_names:
            raise ValueError(f"Unknown step name: {step_name}")

        self.durations.append(time.time() - self.last_timestamp)
        self.outputs.append(step_output)
        self.last_timestamp = time.time()
        success = step_output["success"]

        if self.exit_on_failure and not success:
            logger.error(f"Exiting following failure at step '{step_name}' failed for test '{self.test_filename}'")

            queries = '\n'.join([output['query'] for output in self.outputs])
            logger.info(f"Previous queries:\n{queries}")
            exit()

        return success

    def log_errors(self):
        for output in self.outputs:
            if 'error_log' in output:
                logger.error(output['error_log'])

    def log_steps_queries(self):
        steps = [f".cd {self.test_runtime_directory}\nLOAD 'test_utils';"]
        for output in self.outputs:
            if 'query' not in output:
                print(f"Output does not contain 'query': {output}")
            steps.append(output['query'])

        logger.info("Executed steps:\n" + "\n".join(steps))

    def find_exception_message(self):
        for output in self.outputs:
            if 'exception_message' in output:
                return output['exception_message']
        return '** No exception message found **'

    def load_comparison_results(self):
        # Check if `self.comparison_results_file_name` exists
        if not os.path.exists(self.comparison_results_file_name):
            return

        # Load as JSON
        with open(self.comparison_results_file_name, 'r') as f:
            self.comparison_results = json.load(f)

            error_count_map = {}
            for comparison_error in self.comparison_results:
                err_msg = comparison_error['error']
                if err_msg not in error_count_map:
                    error_count_map[err_msg] = 0
                error_count_map[err_msg] += 1

                expected = (
                    comparison_error['expected'].splitlines(keepends=True) if 'expected' in comparison_error else []
                )
                actual = comparison_error['actual'].splitlines(keepends=True) if 'actual' in comparison_error else []
                comparison_error['diff'] = ''.join(
                    unified_diff(expected, actual, fromfile='expected', tofile='actual', n=0)
                )

            for err_msg, count in error_count_map.items():
                if count == 1:
                    logger.warning(err_msg)
                else:
                    logger.warning(f"{err_msg} ({count})")

    def is_successful(self):
        return all(output["success"] for output in self.outputs)

    @staticmethod
    def report_schema():
        schema = """
    CREATE TABLE test_report (
      test_filename VARCHAR,
      test_runtime_directory VARCHAR,
      start_time TIMESTAMP,
      duckdb_old_version VARCHAR,
      duckdb_new_version VARCHAR,
    """
        for step_name in TestReport.step_names:
            schema += f"  {step_name}_duration DOUBLE,\n"
            schema += f"  {step_name}_outcome VARCHAR,\n"
            schema += f"  {step_name}_stdout VARCHAR,\n"
            schema += f"  {step_name}_stderr VARCHAR,\n"

        schema += f"  comparison_report VARCHAR,\n"
        schema += ");\n"
        return schema

    @staticmethod
    def report_insert():
        sql = f"INSERT INTO test_report (test_filename, test_runtime_directory, start_time, duckdb_old_version, duckdb_new_version, "
        sql += ", ".join(
            [
                f"{step_name}_duration, {step_name}_outcome, {step_name}_stdout, {step_name}_stderr"
                for step_name in TestReport.step_names
            ]
        )
        sql += ", comparison_report"
        sql += ") VALUES (?, ?, ?, ?, ?, " + ("?, ?, ?, ?, " * len(TestReport.step_names)) + "?);\n"
        return sql

    def report_sql_values(self):
        values = [
            self.test_filename,
            self.test_runtime_directory,
            datetime.fromtimestamp(self.start_time),
            self.duckdb_old_version,
            self.duckdb_new_version,
        ]
        for i in range(len(TestReport.step_names)):
            if i < len(self.durations):
                values.append(self.durations[i])

                output = self.outputs[i]
                values.append("success" if output["success"] else "failure")

                # Remove 'result' and 'true' if they are the last two lines
                if output["output"] and len(output["output"]) > 1 and output["output"][-2:] == ["result", "true"]:
                    output["output"] = output["output"][:-2]

                values.append(", ".join(output["output"]))
                values.append(output["error"])
            else:
                values.append(None)
                values.append("not_run")
                values.append("")
                values.append("")

        if self.comparison_results is not None:
            values.append(json.dumps(self.comparison_results, ensure_ascii=False))
        else:
            values.append(None)
        return values
