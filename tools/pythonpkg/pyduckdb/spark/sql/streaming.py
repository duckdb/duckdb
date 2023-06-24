from typing import TYPE_CHECKING

if TYPE_CHECKING:
	from pyduckdb.spark.sql.dataframe import DataFrame
	from pyduckdb.spark.sql.session import SparkSession

class DataStreamWriter:
	def __init__(self, dataframe: "DataFrame"):
		self.dataframe = dataframe
		pass

	def toTable(self, table_name: str) -> None:
		# register the dataframe or create a table from the contents?
		pass

class DataStreamReader:
	def __init__(self, session: "SparkSession"):
		self.session = session
		pass

	# TODO: Expand the parameters for this:
	# https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.DataStreamReader.load.html#pyspark.sql.streaming.DataStreamReader.load
	def load(self, path: str, format: str) -> "DataFrame":
		from pyduckdb.spark.sql.dataframe import DataFrame
		return DataFrame()

__all__ = [
	"DataStreamReader",
	"DataStreamWriter"
]
