from sqllogictest.base_statement import BaseStatement
from sqllogictest.token import Token


class HashThreshold(BaseStatement):
    def __init__(self, header: Token, line: int, threshold: int):
        super().__init__(header, line)
        self.threshold = threshold
