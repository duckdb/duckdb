from sqllogic_parser.base_statement import BaseStatement
from sqllogic_parser.token import Token


class Mode(BaseStatement):
    def __init__(self, header: Token, line: int, parameter: str):
        super().__init__(header, line)
        self.parameter = parameter
