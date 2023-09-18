import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--shell-binary", action="store", default=None, help="Provide the shell binary to use for the tests"
    )


import subprocess
import sys
from typing import List


class ShellTest:
    def __init__(self, shell):
        if not shell:
            raise ValueError("Please provide a shell binary")
        self.shell = shell
        self.arguments = [shell, '--batch', '--init', '/dev/null']
        self.statements: List[str] = []
        self.input = None
        self.output = None

    def add_argument(self, arg):
        self.arguments.append(arg)
        return self

    def statement(self, stmt):
        self.statements.append(stmt)
        return self

    def add_arguments(self, args: List):
        self.arguments.extend(args)
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

        res = subprocess.run(command, input=input_data, stdout=output_pipe, stderr=subprocess.PIPE)

        stdout, stderr = self.get_output_data(res)
        return stdout, stderr, res.returncode


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
