import duckdb
from duckdb import DuckDBPyConnection

class SparkContext:
	def __init__(self, master: str, appName: str):
		self._connection = duckdb.connect(master)
		self._appName = appName

	@property
	def connection(self) -> DuckDBPyConnection:
		return self._connection

	def stop(self) -> None:
		self._connection.close()

__all__ = [
	"SparkContext"
]
