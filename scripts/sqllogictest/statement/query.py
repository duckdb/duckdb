from sqllogictest.base_statement import BaseStatement
from sqllogictest.expected_result import ExpectedResult
from sqllogictest.token import Token
from typing import Optional, List
from enum import Enum


class SortStyle(Enum):
    NO_SORT = 0
    ROW_SORT = 1
    VALUE_SORT = 2
    UNKNOWN = 3


class Query(BaseStatement):
    def __init__(self, header: Token, line: int):
        super().__init__(header, line)
        self.label: Optional[str] = None
        self.lines: List[str] = []
        self.expected_result: Optional[ExpectedResult] = None
        self.connection_name: Optional[str] = None
        self.sortstyle: Optional[SortStyle] = None
        self.label: Optional[str] = None

    def add_lines(self, lines: List[str]):
        self.lines.extend(lines)

    def set_connection(self, connection: str):
        self.connection_name = connection

    def set_expected_result(self, expected_result: ExpectedResult):
        self.expected_result = expected_result

    def set_sortstyle(self, sortstyle: SortStyle):
        self.sortstyle = sortstyle

    def get_sortstyle(self) -> Optional[SortStyle]:
        return self.sortstyle

    def set_label(self, label: str):
        self.label = label

    def get_label(self) -> Optional[str]:
        return self.label
