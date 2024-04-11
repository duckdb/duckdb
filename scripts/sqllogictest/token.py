from enum import Enum, auto


class TokenType(Enum):
    SQLLOGIC_INVALID = auto()
    SQLLOGIC_SKIP_IF = auto()
    SQLLOGIC_ONLY_IF = auto()
    SQLLOGIC_STATEMENT = auto()
    SQLLOGIC_QUERY = auto()
    SQLLOGIC_HASH_THRESHOLD = auto()
    SQLLOGIC_HALT = auto()
    SQLLOGIC_MODE = auto()
    SQLLOGIC_SET = auto()
    SQLLOGIC_LOOP = auto()
    SQLLOGIC_CONCURRENT_LOOP = auto()
    SQLLOGIC_FOREACH = auto()
    SQLLOGIC_CONCURRENT_FOREACH = auto()
    SQLLOGIC_ENDLOOP = auto()
    SQLLOGIC_REQUIRE = auto()
    SQLLOGIC_REQUIRE_ENV = auto()
    SQLLOGIC_LOAD = auto()
    SQLLOGIC_RESTART = auto()
    SQLLOGIC_RECONNECT = auto()
    SQLLOGIC_SLEEP = auto()


class Token:
    def __init__(self):
        self.type = TokenType.SQLLOGIC_INVALID
        self.parameters = []
