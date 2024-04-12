from sqllogictest.base_statement import BaseStatement
from sqllogictest.token import Token


class Load(BaseStatement):
    def __init__(self, header: Token, line: int):
        super().__init__(header, line)
        self.readonly: bool = False

    def set_readonly(self):
        self.readonly = True
