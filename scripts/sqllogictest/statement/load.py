from sqllogictest.base_statement import BaseStatement
from sqllogictest.token import Token


class Load(BaseStatement):
    def __init__(self, header: Token, line: int):
        super().__init__(header, line)
        self.readonly: bool = False
        self.version: Optional[int] = None

    def set_readonly(self):
        self.readonly = True

    def set_version(self, version: str):
        self.version = version
