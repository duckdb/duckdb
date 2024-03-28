from sqllogictest.base_statement import BaseStatement
from sqllogictest.token import Token
from typing import List


class Set(BaseStatement):
    def __init__(self, header: Token, line: int):
        super().__init__(header, line)
        self.error_messages = []

    def add_error_messages(self, error_messages: List[str]):
        self.error_messages.extend(error_messages)
