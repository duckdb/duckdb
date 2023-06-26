from typing_extensions import Self
from typing import Optional, List, Tuple, Any, TYPE_CHECKING

if TYPE_CHECKING:
	from pyduckdb.spark.sql.catalog import Catalog

from pyduckdb.spark.sql.dataframe import DataFrame
from pyduckdb.spark.sql.conf import RuntimeConfig
from pyduckdb.spark.sql.readwriter import DataFrameReader
from pyduckdb.spark.context import SparkContext
from pyduckdb.spark.sql.udf import UDFRegistration
from pyduckdb.spark.sql.streaming import DataStreamReader
import duckdb

class SparkSession:
	def __init__(self, master: str = '', appName: str = ''):
		self.conn = duckdb.connect(master)
		self.context = SparkContext(master, appName)
		self._master = master
		self._conf = RuntimeConfig()
		self._appName = appName

	def newSession(self) -> "SparkSession":
		return SparkSession(self._master, self._appName)

	def createDataFrame(self, tuples: List[Tuple[Any, ...]]) -> DataFrame:
		parameter_count = len(tuples)
		parameters = [f'${x+1}' for x in range(parameter_count)]
		parameters = ', '.join(parameters)
		query = f"""
			select {parameters}
		"""
		# FIXME: we can't add prepared parameters to a relation
		# or extract the relation from a connection after 'execute'
		raise NotImplementedError()

	def table(self, table_name: str) -> DataFrame:
		relation = self.conn.table(table_name)
		return DataFrame(relation, self)

	def sql(self, query: str) -> DataFrame:
		relation = self.conn.sql(query)
		return DataFrame(relation, self)

	def getActiveSession(self) -> Self:
		return self

	@property
	def udf(self) -> UDFRegistration:
		return UDFRegistration()

	@property
	def sparkContext(self) -> SparkContext:
		return self.context

	def stop(self) -> None:
		self.context.stop()

	@property
	def read(self) -> DataFrameReader:
		return DataFrameReader(self)

	@property
	def readStream(self) -> DataStreamReader:
		return DataStreamReader(self)

	@property
	def version(self) -> str:
		return '1.0.0'

	@property
	def catalog(self) -> "Catalog":
		if not hasattr(self, "_catalog"):
			from pyduckdb.spark.sql.catalog import Catalog
			self._catalog = Catalog(self)
		return self._catalog

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
			return SparkSession(self._master, self._appName)
		
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
