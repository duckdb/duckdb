from sqllogictest.base_decorator import BaseDecorator
from sqllogictest.token import Token


class OnlyIf(BaseDecorator):
    def __init__(self, token: Token):
        super().__init__(token)
