from sqllogictest.token import Token, TokenType
from sqllogictest.base_decorator import BaseDecorator
from typing import List


class BaseStatement:
    def __init__(self, header: Token, line: int):
        self.header: Token = header
        self.query_line: int = line
        self.decorators: List[BaseDecorator] = []

    def add_decorators(self, decorators: List[BaseDecorator]):
        self.decorators = decorators

    def get_decorators(self) -> List[BaseDecorator]:
        return self.decorators

    def get_query_line(self) -> int:
        return self.query_line

    def get_type(self) -> TokenType:
        return self.header.type

    def get_parameters(self) -> List[str]:
        return self.header.parameters
