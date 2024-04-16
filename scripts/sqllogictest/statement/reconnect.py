from sqllogictest.base_statement import BaseStatement
from sqllogictest.token import Token


class Reconnect(BaseStatement):
    def __init__(self, header: Token, line: int):
        super().__init__(header, line)
