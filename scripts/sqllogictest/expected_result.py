from enum import Enum, auto
from typing import Optional, List


class ExpectedResult:
    class Type(Enum):
        SUCCESS = auto()
        ERROR = auto()
        UNKNOWN = auto()

    def __init__(self, type: "ExpectedResult.Type"):
        self.type = type
        self.lines: Optional[List[str]] = None
        self.column_count: Optional[int] = None

    def add_lines(self, lines: List[str]):
        self.lines = lines

    def set_expected_column_count(self, column_count: int):
        self.column_count = column_count

    def get_expected_column_count(self) -> Optional[int]:
        return self.column_count
