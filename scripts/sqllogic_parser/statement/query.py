from sqllogic_parser.base_statement import BaseStatement
from sqllogic_parser.token import Token
from typing import Optional


class Query(BaseStatement):
    def __init__(self, header: Token, line: int):
        super().__init__(header, line)
        self.label: Optional[str] = None

    def set_label(self, label: str):
        self.label = label
