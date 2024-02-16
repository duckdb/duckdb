from enum import Enum, auto
from typing import Optional, List


class ExpectedResult:
    class Type(Enum):
        SUCCES = (auto(),)
        ERROR = (auto(),)
        UNKNOWN = auto()

    def __init__(self, type: "ExpectedResult.Type"):
        self.type = type
        self.lines: Optional[List[str]] = None

    def add_lines(self, lines: List[str]):
        self.lines = lines
