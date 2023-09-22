from typing import Optional, Union
from duckdb.experimental.spark._globals import _NoValueType, _NoValue
from duckdb import DuckDBPyConnection


class RuntimeConfig:
    def __init__(self, connection: DuckDBPyConnection):
        self._connection = connection

    def set(self, key: str, value: str) -> None:
        raise NotImplementedError

    def isModifiable(self, key: str) -> bool:
        raise NotImplementedError

    def unset(self, key: str) -> None:
        raise NotImplementedError

    def get(self, key: str, default: Union[Optional[str], _NoValueType] = _NoValue) -> str:
        raise NotImplementedError


__all__ = ["RuntimeConfig"]
