import argparse
import os
import duckdb_sqllogictest
from duckdb_sqllogictest import SQLParserException, SQLLogicParser, SQLLogicTest
import subprocess
import multiprocessing
import tempfile
import re

parser = argparse.ArgumentParser(description="Test serialization")
parser.add_argument("--shell", type=str, help="Shell binary to run", default=os.path.join('build', 'debug', 'duckdb'))
parser.add_argument("--offset", type=int, help="File offset", default=None)
parser.add_argument("--count", type=int, help="File count", default=None)
parser.add_argument('--no-exit', action='store_true', help='Do not exit after a test fails', default=False)
parser.add_argument('--print-failing-only', action='store_true', help='Print failing tests only', default=False)
parser.add_argument(
    '--include-extensions', action='store_true', help='Include test files of out-of-tree extensions', default=False
)
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument("--test-file", type=str, help="Path to the SQL logic file", default='')
group.add_argument(
    "--test-list", type=str, help="Path to the file that contains a newline separated list of test files", default=''
)
group.add_argument("--all-tests", action='store_true', help="Run all tests", default=False)
args = parser.parse_args()


def extract_git_urls(script: str):
    pattern = r'GIT_URL\s+(https?://\S+)'
    return re.findall(pattern, script)


import os
import requests
from urllib.parse import urlparse


def download_directory_contents(api_url, local_path, headers):
    response = requests.get(api_url, headers=headers)
    if response.status_code != 200:
        print(f"⚠️  Could not access {api_url}: {response.status_code}")
        return

    os.makedirs(local_path, exist_ok=True)

    for item in response.json():
        item_type = item.get("type")
        item_name = item.get("name")
        if item_type == "file":
            download_url = item.get("download_url")
            if not download_url:
                continue
            file_path = os.path.join(local_path, item_name)
            file_resp = requests.get(download_url)
            if file_resp.status_code == 200:
                with open(file_path, "wb") as f:
                    f.write(file_resp.content)
                print(f"  - Downloaded {file_path}")
            else:
                print(f"  - Failed to download {file_path}")
        elif item_type == "dir":
            subdir_api_url = item.get("url")
            subdir_local_path = os.path.join(local_path, item_name)
            download_directory_contents(subdir_api_url, subdir_local_path, headers)


def download_test_sql_folder(repo_url, base_folder="extension-test-files"):
    repo_name = urlparse(repo_url).path.strip("/").split("/")[-1]
    target_folder = os.path.join(base_folder, repo_name)

    if os.path.exists(target_folder):
        print(f"✓ Skipping {repo_name}, already exists.")
        return

    print(f"⬇️ Downloading test/sql from {repo_name}...")

    api_url = f"https://api.github.com/repos/duckdb/{repo_name}/contents/test/sql?ref=main"
    GITHUB_TOKEN = os.environ["GITHUB_TOKEN"]
    headers = {"Accept": "application/vnd.github.v3+json", "Authorization": f"Bearer {GITHUB_TOKEN}"}

    download_directory_contents(api_url, target_folder, headers)


def batch_download_all_test_sql():
    filename = ".github/config/out_of_tree_extensions.cmake"
    if not os.path.isfile(filename):
        raise Exception(f"File {filename} not found")
    with open(filename, "r") as f:
        content = f.read()
    urls = extract_git_urls(content)
    if urls == []:
        print("No URLs found.")
    for url in urls:
        download_test_sql_folder(url)


def find_tests_recursive(dir, excluded_paths):
    test_list = []
    for f in os.listdir(dir):
        path = os.path.join(dir, f)
        if path in excluded_paths:
            continue
        if os.path.isdir(path):
            test_list += find_tests_recursive(path, excluded_paths)
        elif path.endswith('.test') or path.endswith('.test_slow'):
            test_list.append(path)
    return test_list


def parse_test_file(filename):
    if not os.path.isfile(filename):
        raise Exception(f"File {filename} not found")
    parser = SQLLogicParser()
    try:
        out: Optional[SQLLogicTest] = parser.parse(filename)
        if not out:
            raise SQLParserException(f"Test {filename} could not be parsed")
    except:
        return []
    loop_count = 0
    statements = []
    for stmt in out.statements:
        if type(stmt) is duckdb_sqllogictest.statement.skip.Skip:
            # mode skip - just skip entire test
            break
        if (
            type(stmt) is duckdb_sqllogictest.statement.loop.Loop
            or type(stmt) is duckdb_sqllogictest.statement.foreach.Foreach
        ):
            loop_count += 1
        if type(stmt) is duckdb_sqllogictest.statement.endloop.Endloop:
            loop_count -= 1
        if loop_count > 0:
            # loops are ignored currently
            continue
        if not (
            type(stmt) is duckdb_sqllogictest.statement.query.Query
            or type(stmt) is duckdb_sqllogictest.statement.statement.Statement
        ):
            # only handle query and statement nodes for now
            continue
        if type(stmt) is duckdb_sqllogictest.statement.Statement:
            # skip expected errors
            if stmt.expected_result.type == duckdb_sqllogictest.ExpectedResult.Type.ERROR:
                if any(
                    "parser error" in line.lower() or "syntax error" in line.lower()
                    for line in stmt.expected_result.lines
                ):
                    continue
        query = ' '.join(stmt.lines)
        statements.append(query)
    return statements


def run_test_case(args_tuple):
    i, file, shell, print_failing_only = args_tuple
    results = []
    if not print_failing_only:
        print(f"Run test {i}: {file}")

    statements = parse_test_file(file)
    for statement in statements:
        with tempfile.TemporaryDirectory() as tmpdir:
            peg_sql_path = os.path.join(tmpdir, 'peg_test.sql')
            with open(peg_sql_path, 'w') as f:
                f.write(f'CALL check_peg_parser($TEST_PEG_PARSER${statement}$TEST_PEG_PARSER$);\n')

            proc = subprocess.run([shell, '-init', peg_sql_path, '-c', '.exit'], capture_output=True)
            stderr = proc.stderr.decode('utf8')

            if proc.returncode == 0 and ' Error:' not in stderr:
                continue

            if print_failing_only:
                print(f"Failed test {i}: {file}")
            else:
                print(f'Failed')
                print(f'-- STDOUT --')
                print(proc.stdout.decode('utf8'))
                print(f'-- STDERR --')
                print(stderr)

            results.append((file, statement))
            break
    return results


if __name__ == "__main__":
    files = []
    excluded_tests = {
        'test/sql/peg_parser',  # Fail for some reason
        'test/sql/prepared/parameter_variants.test',  # PostgreSQL parser bug with ?1
        'test/sql/copy/s3/download_config.test',  # Unknown why this passes in SQLLogicTest
        'test/sql/function/list/lambdas/arrow/lambda_scope_deprecated.test',  # Error in the tokenization of *+*
        'test/sql/catalog/function/test_simple_macro.test',  # Bug when mixing named parameters and non-named
    }
    if args.all_tests:
        # run all tests
        test_dir = os.path.join('test', 'sql')
        files = find_tests_recursive(test_dir, excluded_tests)
        if args.include_extensions:
            batch_download_all_test_sql()
            extension_files = find_tests_recursive('extension-test-files', {})
            files = files + extension_files
    elif len(args.test_list) > 0:
        with open(args.test_list, 'r') as f:
            files = [x.strip() for x in f.readlines() if x.strip() not in excluded_tests]
    else:
        # run a single test
        files.append(args.test_file)
    files.sort()

    start = args.offset if args.offset is not None else 0
    end = start + args.count if args.count is not None else len(files)
    work_items = [(i, files[i], args.shell, args.print_failing_only) for i in range(start, end)]

    if not args.no_exit:
        # Disable multiprocessing for --no-exit behavior
        failed_test_list = []
        for item in work_items:
            res = run_test_case(item)
            if res:
                failed_test_list.extend(res)
                exit(1)
    else:
        with multiprocessing.Pool() as pool:
            results = pool.map(run_test_case, work_items)
        failed_test_list = [item for sublist in results for item in sublist]

    failed_tests = len(failed_test_list)
    print("List of failed tests: ")
    for test, statement in failed_test_list:
        print(f"{test}\n{statement}\n\n")
    print(f"Total of {failed_tests} out of {len(files)} failed ({round(failed_tests/len(files) * 100,2)}%). ")
