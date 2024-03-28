from sqllogictest.base_statement import BaseStatement
from sqllogictest.token import Token
from enum import Enum, auto


class SleepUnit(Enum):
    SECOND = auto()
    MILLISECOND = auto()
    MICROSECOND = auto()
    NANOSECOND = auto()
    UNKNOWN = auto()


def get_sleep_unit(unit):
    seconds = ["second", "seconds", "sec"]
    miliseconds = ["millisecond", "milliseconds", "milli"]
    microseconds = ["microsecond", "microseconds", "micro"]
    nanoseconds = ["nanosecond", "nanoseconds", "nano"]
    if unit in seconds:
        return SleepUnit.SECOND
    elif unit in miliseconds:
        return SleepUnit.MILLISECOND
    elif unit in microseconds:
        return SleepUnit.MICROSECOND
    elif unit in nanoseconds:
        return SleepUnit.NANOSECOND
    else:
        return SleepUnit.UNKNOWN


class Sleep(BaseStatement):
    def __init__(self, header: Token, line: int, duration: int, unit: SleepUnit):
        super().__init__(header, line)
        self.duration = duration
        self.unit = unit

    def get_duration(self) -> int:
        return self.duration

    def get_unit(self) -> SleepUnit:
        return self.unit
