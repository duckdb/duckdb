from sqllogictest.base_statement import BaseStatement
from sqllogictest.token import Token
from typing import Optional, List


class Foreach(BaseStatement):
    def __init__(self, header: Token, line: int, parallel: bool):
        super().__init__(header, line)
        self.parallel = parallel
        self.values: List[str] = []
        self.name: Optional[str] = None

    def set_name(self, name: str):
        self.name = name

    def set_values(self, values: List[str]):
        self.values = values
