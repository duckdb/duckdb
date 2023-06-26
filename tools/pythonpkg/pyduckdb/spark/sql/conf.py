from typing import Optional, Union
from pyduckdb.spark._globals import _NoValueType, _NoValue
from duckdb import DuckDBPyConnection

class RuntimeConfig:
	def __init__(self, connection: DuckDBPyConnection):
		self._connection = connection
		self._config = {}

	def set(self, key: str, value: str) -> None:
		self._config[key] = value

	def isModifiable(self, key: str) -> bool:
		raise NotImplementedError

	def unset(self, key: str) -> None:
		raise NotImplementedError

	def get(self, key: str, default: Union[Optional[str], _NoValueType] = _NoValue) -> str:
		if key in self._config:
			return self._config[key]
		if default:
			return default
		raise KeyError(key)

__all__ = [
	"RuntimeConfig"
]
