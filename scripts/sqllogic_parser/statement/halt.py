from sqllogic_parser.base_statement import BaseStatement
from sqllogic_parser.token import Token


class Halt(BaseStatement):
    def __init__(self, header: Token, line: int):
        super().__init__(header, line)
