from sqllogic_parser.base_statement import BaseStatement
from sqllogic_parser.token import Token


class Query(BaseStatement):
    def __init__(self, header: Token, line: int):
        super().__init__(header, line)
