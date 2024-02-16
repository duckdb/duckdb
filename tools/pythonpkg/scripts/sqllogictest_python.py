import sys
import os
import glob
import json
from typing import Optional, List

script_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(script_path, '..', '..', '..', 'scripts'))
from sqllogictest import (
    SQLLogicParser,
    SQLLogicEncoder,
    SQLLogicTest,
    BaseStatement,
    Statement,
    Require,
    Mode,
    Halt,
    Set,
    Load,
    Query,
    HashThreshold,
    Loop,
    Foreach,
    Endloop,
    RequireEnv,
    Restart,
    Reconnect,
    Sleep,
    Skip,
    Unskip,
)

from enum import Enum, auto


class ExecuteResult:
    class Type(Enum):
        SUCCES = (auto(),)
        ERROR = (auto(),)
        SKIPPED = auto()

    def __init__(self, type: "ExecuteResult.Type"):
        self.type = type


class SQLLogicTestExecutor:
    def reset(self):
        self.skipped = False
        self.error: Optional[str] = None

    def __init__(self):
        self.reset()
        self.STATEMENTS = {Statement: self.execute_statement, RequireEnv: self.execute_require_env}

    def get_unsupported_statements(self, test: SQLLogicTest) -> List[BaseStatement]:
        unsupported_statements = [
            statement for statement in test.statements if statement.__class__ not in self.STATEMENTS
        ]
        return unsupported_statements

    def execute_statement(self, statement: BaseStatement):
        print('yes')

    def execute_require_env(self, statement: BaseStatement):
        self.skipped = True

    def execute_test(self, test: SQLLogicTest) -> ExecuteResult:
        self.reset()
        unsupported = self.get_unsupported_statements(test)
        if unsupported != []:
            error = 'Test skipped because the following statement types are not supported: '
            types = set([x.__class__ for x in unsupported])
            error += str(list([x.__name__ for x in types]))
            raise Exception(error)

        for statement in test.statements:
            method = self.STATEMENTS.get(statement.__class__)
            if not method:
                raise Exception(f"Not supported: {statement.__class__.__name__}")
            method(statement)
            if self.skipped:
                return ExecuteResult(ExecuteResult.Type.SKIPPED)
        return ExecuteResult(ExecuteResult.Type.SUCCES)


def main():
    parser = SQLLogicParser()
    executor = SQLLogicTestExecutor()
    test_directory = os.path.join(script_path, '..', '..', '..', 'test')
    for filename in glob.iglob(test_directory + '/**/*.test', recursive=True):
        print(filename)
        test = parser.parse(filename)

        result = executor.execute_test(test)
        if result.type == ExecuteResult.Type.SKIPPED:
            continue
        exit()
    print('test')
    pass


if __name__ == '__main__':
    main()
