from sqllogictest.base_statement import BaseStatement
from sqllogictest.token import Token
from typing import Optional, List


class Loop(BaseStatement):
    def __init__(self, header: Token, line: int, parallel: bool):
        super().__init__(header, line)
        self.parallel = parallel
        self.name: Optional[str] = None
        self.start: Optional[int] = None
        self.end: Optional[int] = None

    def set_name(self, name: str):
        self.name = name

    def set_start(self, start: List[str]):
        self.start = start

    def set_end(self, end: List[str]):
        self.end = end
