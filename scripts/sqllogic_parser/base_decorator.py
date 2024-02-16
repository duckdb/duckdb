from sqllogic_parser.token import Token


class BaseDecorator:
    def __init__(self, token: Token):
        self.token: Token = token
