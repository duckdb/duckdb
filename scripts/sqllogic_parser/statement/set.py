from sqllogic_parser.base_statement import BaseStatement
from sqllogic_parser.token import Token
from typing import List


class Set(BaseStatement):
    def __init__(self, header: Token, line: int):
        super().__init__(header, line)
        self.parameters = []

    def add_parameters(self, parameters: List[str]):
        self.parameters.extend(parameters)
