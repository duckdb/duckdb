from typing import Optional, List
from sqllogic_parser.token import Token


class BaseStatement:
    def __init__(self, header: Token, line: int):
        self.header: Token = header
        self.query_line: int = line
        self.file_name: Optional[str] = None

        self.lines: List[str] = []
        self.expected_result: Optional[List[str]] = None
        self.connection_name: Optional[str] = None

    def add_lines(self, lines: List[str]):
        self.lines.extend(lines)

    def set_connection(self, connection: str):
        self.connection_name = connection

    def perform_rename(self):
        # TODO: handle renames:
        # environment variables
        # keywords
        pass
