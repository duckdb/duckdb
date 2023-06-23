from typing_extensions import Self
from typing import Optional, List, Tuple, Any

from pyduckdb.spark.sql.dataframe import DataFrame
from pyduckdb.spark.sql.conf import RuntimeConfig
from pyduckdb.spark.sql.catalog import Catalog

class SparkSession:
	def __init__(self):
		self.name = "session"
		self._master = 'master'

	def newSession(self) -> "SparkSession":
		return SparkSession()

	def createDataFrame(self, tuples: List[Tuple[Any, ...]]) -> DataFrame:
		return DataFrame()

	def table(self, table_name: str) -> DataFrame:
		return DataFrame()

	def sql(self, query: str) -> DataFrame:
		"""
			TODO: this needs to query DuckDB
		"""
		return DataFrame()

	@property
	def catalog(self) -> Catalog:
		return Catalog()

	@property
	def conf(self) -> RuntimeConfig:
		return RuntimeConfig()

	class Builder:
		def __init__(self):
			self.name = "builder"
			self._config = {}

		def master(self, name: str) -> Self:
			self._master = name
			return self

		def appName(self, name: str) -> Self:
			self._appName = name
			return self

		def getOrCreate(self) -> "SparkSession":
			return SparkSession()
		
		def config(self, key: Optional[str] = None, value: Optional[str] = None) -> Self:
			if (key and value):
				self._config[key] = value
			return self
		
		def enableHiveSupport(self) -> Self:
			return self

	builder = Builder()

__all__ = [
	"SparkSession"
]
