from sqllogictest.base_statement import BaseStatement
from sqllogictest.expected_result import ExpectedResult
from sqllogictest.token import Token
from typing import List, Optional


class Statement(BaseStatement):
    def __init__(self, header: Token, line: int):
        super().__init__(header, line)
        self.lines: List[str] = []
        self.expected_result: Optional[ExpectedResult] = None
        self.connection_name: Optional[str] = None

    def add_lines(self, lines: List[str]):
        self.lines.extend(lines)

    def set_connection(self, connection: str):
        self.connection_name = connection

    def set_expected_result(self, expected_result: ExpectedResult):
        self.expected_result = expected_result
