from sqllogic_parser.base_decorator import BaseDecorator
from sqllogic_parser.token import Token


class OnlyIf(BaseDecorator):
    def __init__(self, token: Token):
        super().__init__(token)
