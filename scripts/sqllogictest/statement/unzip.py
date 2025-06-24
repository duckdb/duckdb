from sqllogictest.base_statement import BaseStatement
from sqllogictest.token import Token


class Unzip(BaseStatement):
    def __init__(self, header: Token, line: int, source: str, destination: str):
        super().__init__(header, line)
        self.source = source
        self.destination = destination
