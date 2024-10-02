import pytest
import os
import subprocess
import sys
from typing import List, NamedTuple, Union


def pytest_addoption(parser):
    parser.addoption(
        "--shell-binary", action="store", default=None, help="Provide the shell binary to use for the tests"
    )
    parser.addoption("--start-offset", action="store", type=int, help="Skip the first 'n' tests")


def pytest_collection_modifyitems(config, items):
    start_offset = config.getoption("--start-offset")
    if not start_offset:
        # --skiplist not given in cli, therefore move on
        return

    skipped = pytest.mark.skip(reason="included in --skiplist")
    skipped_items = items[:start_offset]
    for item in skipped_items:
        item.add_marker(skipped)


class TestResult:
    def __init__(self, stdout, stderr, status_code):
        self.stdout: Union[str, bytes] = stdout
        self.stderr: Union[str, bytes] = stderr
        self.status_code: int = status_code

    def check_stdout(self, expected: Union[str, List[str], bytes]):
        if isinstance(expected, list):
            expected = '\n'.join(expected)
        assert self.status_code == 0
        assert expected in self.stdout

    def check_not_exist(self, not_exist: Union[str, List[str], bytes]):
        if isinstance(not_exist, list):
            not_exist = '\n'.join(not_exist)
        assert self.status_code == 0
        assert not_exist not in self.stdout

    def check_stderr(self, expected: str):
        assert expected in self.stderr


class ShellTest:
    def __init__(self, shell):
        if not shell:
            raise ValueError("Please provide a shell binary")
        self.shell = shell
        self.arguments = [shell, '--batch', '--init', '/dev/null']
        self.statements: List[str] = []
        self.input = None
        self.output = None
        self.environment = {}

    def add_argument(self, *args):
        self.arguments.extend(args)
        return self

    def statement(self, stmt):
        self.statements.append(stmt)
        return self

    def query(self, *stmts):
        self.statements.extend(stmts)
        return self

    def input_file(self, file_path):
        self.input = file_path
        return self

    def output_file(self, file_path):
        self.output = file_path
        return self

    # Test Running methods

    def get_command(self, cmd: str) -> List[str]:
        command = self.arguments
        if self.input:
            command += [cmd]
        return command

    def get_input_data(self, cmd: str):
        if self.input:
            input_data = open(self.input, 'rb').read()
        else:
            input_data = bytearray(cmd, 'utf8')
        return input_data

    def get_output_pipe(self):
        output_pipe = subprocess.PIPE
        if self.output:
            output_pipe = open(self.output, 'w+')
        return output_pipe

    def get_statements(self):
        result = ""
        statements = []
        for statement in self.statements:
            if statement.startswith('.'):
                statements.append(statement)
            else:
                statements.append(statement + ';')
        return '\n'.join(statements)

    def get_output_data(self, res):
        if self.output:
            stdout = open(self.output, 'r').read()
        else:
            stdout = res.stdout.decode('utf8').strip()
        stderr = res.stderr.decode('utf8').strip()
        return stdout, stderr

    def run(self):
        statements = self.get_statements()
        command = self.get_command(statements)
        input_data = self.get_input_data(statements)
        output_pipe = self.get_output_pipe()

        my_env = os.environ.copy()
        for key, val in self.environment.items():
            my_env[key] = val

        res = subprocess.run(command, input=input_data, stdout=output_pipe, stderr=subprocess.PIPE, env=my_env)

        stdout, stderr = self.get_output_data(res)
        return TestResult(stdout, stderr, res.returncode)


@pytest.fixture()
def shell(request):
    custom_arg = request.config.getoption("--shell-binary")
    if not custom_arg:
        raise ValueError("Please provide a shell binary path to the tester, using '--shell-binary <path_to_cli>'")
    return custom_arg


@pytest.fixture()
def random_filepath(request, tmp_path):
    tmp_file = tmp_path / "random_import_file"
    return tmp_file


@pytest.fixture()
def generated_file(request, random_filepath):
    param = request.param
    tmp_file = random_filepath
    with open(tmp_file, 'w+') as f:
        f.write(param)
    return tmp_file


def check_load_status(shell, extension: str):
    binary = ShellTest(shell)
    binary.statement(f"select loaded from duckdb_extensions() where extension_name = '{extension}';")
    result = binary.run()
    return result.stdout


def assert_loaded(shell, extension: str):
    # TODO: add a command line argument to fail instead of skip if the extension is not loaded
    out = check_load_status(shell, extension)
    if 'true' not in out:
        pytest.skip(reason=f"'{extension}' extension is not loaded!")
    return


@pytest.fixture()
def autocomplete_extension(shell):
    assert_loaded(shell, 'autocomplete')


@pytest.fixture()
def json_extension(shell):
    assert_loaded(shell, 'json')
