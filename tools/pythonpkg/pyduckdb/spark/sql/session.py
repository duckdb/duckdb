from typing import Optional, List, Tuple, Any, TYPE_CHECKING

if TYPE_CHECKING:
	from pyduckdb.spark.sql.catalog import Catalog

from pyduckdb.spark.exception import ContributionsAcceptedError

from pyduckdb.spark.conf import SparkConf
from pyduckdb.spark.sql.dataframe import DataFrame
from pyduckdb.spark.sql.conf import RuntimeConfig
from pyduckdb.spark.sql.readwriter import DataFrameReader
from pyduckdb.spark.context import SparkContext
from pyduckdb.spark.sql.udf import UDFRegistration
from pyduckdb.spark.sql.streaming import DataStreamReader
import duckdb

# In spark:
# SparkSession holds a SparkContext
# SparkContext gets created from SparkConf
# At this level the check is made to determine whether the instance already exists and just needs to be retrieved or it needs to be created

# For us this is done inside of `duckdb.connect`, based on the passed in path + configuration
# SparkContext can be compared to our Connection class, and SparkConf to our ClientContext class

class SparkSession:
	def __init__(self, context : SparkContext):
		self.conn = context.connection
		self._context = context
		self._conf = RuntimeConfig(self.conn)

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

	def newSession(self) -> "SparkSession":
		return SparkSession(self._context)

	def range(self, start: int, end: Optional[int] = None, step: int = 1, numPartitions: Optional[int] = None) -> "DataFrame":
		raise ContributionsAcceptedError

	def sql(self, sqlQuery: str, **kwargs: Any) -> DataFrame:
		if kwargs:
			raise NotImplementedError
		relation = self.conn.sql(sqlQuery)
		return DataFrame(relation, self)

	def stop(self) -> None:
		self._context.stop()

	def table(self, tableName: str) -> DataFrame:
		relation = self.conn.table(tableName)
		return DataFrame(relation, self)

	def getActiveSession(self) -> "SparkSession":
		return self

	@property
	def catalog(self) -> "Catalog":
		if not hasattr(self, "_catalog"):
			from pyduckdb.spark.sql.catalog import Catalog
			self._catalog = Catalog(self)
		return self._catalog

	@property
	def conf(self) -> RuntimeConfig:
		return self._conf

	@property
	def read(self) -> DataFrameReader:
		return DataFrameReader(self)

	@property
	def readStream(self) -> DataStreamReader:
		return DataStreamReader(self)

	@property
	def sparkContext(self) -> SparkContext:
		return self._context

	@property
	def streams(self) -> Any:
		raise ContributionsAcceptedError

	@property
	def udf(self) -> UDFRegistration:
		return UDFRegistration()

	@property
	def version(self) -> str:
		return '1.0.0'

	class Builder:
		def __init__(self):
			self.name = "builder"
			self._master = ':memory:'
			self._config = {}

		def master(self, name: str) -> "SparkSession.Builder":
			self._master = name
			return self

		def appName(self, name: str) -> "SparkSession.Builder":
			# no-op
			return self

		def remote(self, url: str) -> "SparkSession.Builder":
			# no-op
			return self

		def getOrCreate(self) -> "SparkSession":
			# TODO: use the config to pass in methods to 'connect'
			context = SparkContext(self._master)
			return SparkSession(context)

		def config(self, key: Optional[str] = None, value: Optional[Any] = None, conf: Optional[SparkConf] = None) -> "SparkSession.Builder":
			if conf:
				raise NotImplementedError
			if (key and value):
				self._config[key] = value
			return self

		def enableHiveSupport(self) -> "SparkSession.Builder":
			# no-op
			return self

	builder = Builder()

__all__ = [
	"SparkSession"
]
