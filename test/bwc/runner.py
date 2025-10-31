import shutil
from utils.duckdb_cli import DuckDBCLI
from utils.duckdb_installer import install_assets, install_extensions, make_cli_path
from utils.test_files_parser import load_test_files
from utils.test_report import TestReport
from utils.logger import make_logger
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Lock, Event
from os.path import abspath, dirname
import os
import sys
import duckdb

logger = make_logger(__name__)
cancel_event = Event()

def run_multi_clis(clis, c):
  results = []
  for cli in clis:
    r = cli.execute_command(c)
    results.append(r)
  return results


TRANSIENT_ERROR_PATTERNS = [
  "Could not set lock on file",
  "HTTP",
  "Connection",
  "Unable to connect",
]


def is_transient_error(output):
  error = output.get("error") or ""
  exception = output.get("exception_message") or ""
  combined = error + exception
  return len(combined) == 0 or any(pattern in combined for pattern in TRANSIENT_ERROR_PATTERNS)


def run_step(cli, report, func_name, *args):
  str_args = ", ".join([f"'{arg}'" for arg in args])
  command = f"CALL {func_name}({str_args});"
  output = cli.execute_command(command)

  # Retry on transient errors (file locks, network issues)
  if not output["success"] and is_transient_error(output):
    for attempt in range(1, 4):
      logger.warning(f"[{cli.version}] Transient error, retrying in {attempt}s (attempt {attempt}/3)")
      time.sleep(attempt)
      output = cli.execute_command(command)
      if output["success"] or not is_transient_error(output):
        break

  # We had to name the functions `tu_compare_results_from_xxx`
  # in DuckDB 1.1.x to avoid collision and support overloading.
  step_name = "compare_results" if "compare_results" in func_name else func_name
  return report.end_step(step_name, output)

def get_extensions_versions(clis):
  results = run_multi_clis(clis, "SELECT extension_version FROM duckdb_extensions() WHERE extension_name='test_utils';")
  versions = []
  for r in results:
    if not r["success"]:
      raise RuntimeError(f"Failed to get extension version: {r['error']}")
    # Find the actual version value, skipping the CSV header and any stale output
    version = None
    for line in r["output"]:
      if line and line != "extension_version":
        version = line
        break
    if not version:
      raise RuntimeError(f"test-utils extension not loaded for CLI '{clis[len(versions)].version}' - output: {r['output']}")
    versions.append(version)
  return versions


def do_run_test(old_cli_path, new_cli_path, nb_tests, test_spec):
  logger.info(f"Running test {test_spec.test_idx}/{nb_tests}: {test_spec.test_spec_relative_path}")
  report = TestReport(test_spec)
  sanity_checks = False
  with DuckDBCLI(old_cli_path, unsigned=True) as old_cli:
    with DuckDBCLI(new_cli_path, unsigned=True) as new_cli:
      ext_path = 'test_utils'
      run_multi_clis([old_cli, new_cli], f".cd {test_spec.test_runtime_directory}\nLOAD '{ext_path}'")
      logger.debug(f"Loaded extension from '{ext_path}'")
      if sanity_checks:
        run_multi_clis([old_cli, new_cli], "pragma version;")
        run_multi_clis([old_cli, new_cli], "SELECT extension_name, loaded, extension_version FROM duckdb_extensions() WHERE extension_name='test_utils';")

      versions = get_extensions_versions([old_cli, new_cli])
      if versions[0] != versions[1]:
        # make release EXTENSION_CONFIGS=.github/config/extensions/test-utils.cmake to compile in this repo
        raise RuntimeError(f"test-utils extension version mismatch: {old_cli.version} is @ {versions[0]} and {new_cli.version} is @ {versions[1]}")

      # Cleanup ouptput directory in case previous runs left files there
      test_spec.reset_output_directory()

      # First serialize the queries & run them in the first version
      compare_func = "tu_compare_results_from_memory"
      compare_args = [test_spec.results_new_file_name]
      if test_spec.has_cached_serialized_plans:
        compare_args.append(test_spec.results_old_file_name) # Load old results for comparison
        compare_func = "tu_compare_results_from_file"
        report.end_cached_step("serialize_queries_plans", test_spec.queries_file_name, test_spec.serialized_plans_file_name)
        report.end_cached_step("serialize_results", test_spec.results_old_file_name)
      else:
        if not run_step(old_cli, report, "serialize_queries_plans", test_spec.queries_file_name, test_spec.serialized_plans_file_name):
          return report

        # TODO - merge results in serialized file?
        if not run_step(old_cli, report, "serialize_results", test_spec.results_old_file_name):
          return report

        # We need to provide a clean output folder for the other CLI
        test_spec.reset_output_directory()

      # Then execute the plans in the second version
      if not run_step(new_cli, report, "execute_all_plans_from_file", test_spec.serialized_plans_file_name, test_spec.results_new_file_name):
        return report

      # Now compare the results
      run_step(old_cli, report, compare_func, *compare_args)
      report.load_comparison_results()
      return report

def write_skip_file(report_lock, skip_file, to_write):
  with report_lock:
    skip_file.write(to_write)
    skip_file.flush()

def run_one_test_and_log(old_cli_path, new_cli_path, nb_tests, test, skip_file, report_con, report_lock):
  if cancel_event.is_set():
    return None

  try:
    report = do_run_test(old_cli_path, new_cli_path, nb_tests, test)
    if report.is_successful():
      write_skip_file(report_lock, skip_file, f"{test.test_spec_relative_path}\n")
      logger.info(f"Test {test.test_idx}/{nb_tests}: {test.test_spec_relative_path} - SUCCESS")
    else:
      em = report.find_exception_message()
      write_skip_file(report_lock, skip_file, f"{test.test_spec_relative_path} # FAILED: {em}\n")
      logger.error(f"Test {test.test_idx}/{nb_tests}: {test.test_spec_relative_path} - ❌ FAILED: {em}")

    with report_lock:
      report_con.execute(TestReport.report_insert(), report.report_sql_values())
      report_con.commit()

    return report
  except Exception as e:
    cancel_event.set()
    raise

def cleanup_runtime_dir(bwc_tests_base_dir):
  runtime_dir = f"{bwc_tests_base_dir}/runtime"
  logger.info(f"Cleaning up BWC directory '{runtime_dir}'")
  delete_list = []
  # Remove new result files, any 'output' dir, and 'data' & 'test' symlinks
  for root, dirs, files in os.walk(runtime_dir):
    for file in files:
      file_path = os.path.join(root, file)
      if file.endswith(".new.result.bin"):
        delete_list.append(file_path)
    for dir in dirs:
      dir_path = os.path.join(root, dir)
      if dir.startswith("output") or (os.path.islink(dir_path) and (dir == "data" or dir == "test")):
        delete_list.append(dir_path)

  dry_run = len(sys.argv) < 3 or sys.argv[2] != "no_dry_run"
  if dry_run:
    logger.info(f"{len(delete_list)} files would be deleted - re-run with `no_dry_run` argument to actually delete them")
    for file_path in delete_list:
      logger.debug(f"  {file_path}")
  else:
    removed = 0
    for file_path in delete_list:
      try:
        if os.path.isfile(file_path) or os.path.islink(file_path):
          os.remove(file_path)
          removed += 1
        elif os.path.isdir(file_path):
          shutil.rmtree(file_path)
          removed += 1
      except Exception as e:
        logger.error(f"Failed to delete '{file_path}': {e}")
    logger.info(f"Cleanup completed, deleted {removed}/{len(delete_list)} files/directories")

if __name__ == "__main__":
  supported_duckdb_versions = ["v1.4.4"] # ["v1.1.0", "v1.1.2", "v1.1.1", "v1.2.0", "v1.1.3", "v1.2.2", "v1.2.1", "v1.3.0", "v1.3.1", "v1.3.2", "1.4.0", "1.4.1", "1.4.2", "1.4.3"]

  duckdb_root_dir = dirname(dirname(dirname(abspath(__file__))))
  logger.info(f"DuckDB root dir: '{duckdb_root_dir}'")
  bwc_tests_base_dir = f"{duckdb_root_dir}/duckdb_unittest_tempdir/bwc"

  if len(sys.argv) > 1 and sys.argv[1] == "cleanup_bwc_directory":
    cleanup_runtime_dir(bwc_tests_base_dir)
    sys.exit(0)

  # Create the report database
  os.makedirs(f"{bwc_tests_base_dir}/reports", exist_ok=True)
  ts = time.strftime("%Y%m%d_%H%M%S")
  report_db_path = f"{bwc_tests_base_dir}/reports/test_report_{ts}.duckdb"
  con = duckdb.connect(report_db_path)
  con.execute(TestReport.report_schema())
  con.commit()

  logger.info(f"Report database created at '{report_db_path}'")

  # Install DuckDB CLIs and test suites for all supported versions
  with ThreadPoolExecutor(max_workers=10) as executor:
    list(executor.map(lambda version: install_assets(version, bwc_tests_base_dir), supported_duckdb_versions))

  nb_tests_run = 0
  nb_success = 0
  for old_duckdb_version in supported_duckdb_versions:
    logger.info(f"Running tests for DuckDB version '{old_duckdb_version}'")

    old_cli_p = make_cli_path(old_duckdb_version)
    new_cli_p = f"{duckdb_root_dir}/build/release/duckdb"

    # open skipfile
    skip_file_path = f"{bwc_tests_base_dir}/skip_tests_{old_duckdb_version}.txt"
    lock = Lock()

    with open(skip_file_path, 'a') as skip_file:
      test_pattern = sys.argv[1] if len(sys.argv) > 1 else None
      res = load_test_files(bwc_tests_base_dir, old_duckdb_version, skip_file_path, test_pattern)
      tests = res['tests']

      extensions = res['needed_extensions']
      extensions.add('test_utils')
      install_extensions(old_cli_p, extensions)
      install_extensions(new_cli_p, extensions)

      run_sequentially = test_pattern is not None
      stop_on_failure = False
      start_time = time.time()
      nb_tests_run = 0
      if run_sequentially:
        for test in tests:
          report = run_one_test_and_log(old_cli_p, new_cli_p, len(tests), test, skip_file, con, lock)
          nb_tests_run += 1

          if report.is_successful():
            nb_success += 1
          else:
            if stop_on_failure:
              break
            elif test_pattern is not None:
              report.log_errors()
              report.log_steps_queries()
      else:
        with ThreadPoolExecutor(max_workers=25) as executor:
          reports = executor.map(lambda test: run_one_test_and_log(old_cli_p, new_cli_p, len(tests), test, skip_file, con, lock), tests)

        reports_list = list(reports)
        nb_tests_run = len(reports_list)
        nb_success = sum(1 for r in reports_list if r.is_successful())

      elapsed = time.time() - start_time
      tps = nb_tests_run / elapsed if elapsed > 0 else 0
      nb_failed = nb_tests_run - nb_success
      logger.info(f"All tests completed in {elapsed:.2f}s - {nb_tests_run} tests run, {nb_success} successful, {nb_failed} failed - {tps:.2f} tests per second.")

