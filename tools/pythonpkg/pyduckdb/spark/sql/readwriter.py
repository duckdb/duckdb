from typing import TYPE_CHECKING

if TYPE_CHECKING:
	from pyduckdb.spark.sql.dataframe import DataFrame
	from pyduckdb.spark.sql.session import SparkSession

class DataFrameWriter:
	def __init__(self, dataframe: "DataFrame"):
		self.dataframe = dataframe
		pass

	def saveAsTable(self, table_name: str) -> None:
		# register the dataframe or create a table from the contents?
		relation = self.dataframe.relation
		relation.create(table_name)

class DataFrameReader:
	def __init__(self, session: "SparkSession"):
		self.session = session
		pass

	# TODO: Expand the parameters for this:
	# https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.load.html#pyspark.sql.DataFrameReader.load
	def load(self, path: str, format: str) -> "DataFrame":
		from pyduckdb.spark.sql.dataframe import DataFrame
		raise NotImplementedError
		return DataFrame()

__all__ = [
	"DataFrameWriter",
	"DataFrameReader"
]
