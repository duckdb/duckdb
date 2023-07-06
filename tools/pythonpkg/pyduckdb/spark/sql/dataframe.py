from typing import TYPE_CHECKING

from pyduckdb.spark.sql.readwriter import DataFrameWriter
import duckdb

if TYPE_CHECKING:
	from pyduckdb.spark.sql.session import SparkSession

class DataFrame:
	def __init__(self, relation: duckdb.DuckDBPyRelation, session: "SparkSession"):
		self.relation = relation
		self.session = session

	def show(self) -> None:
		self.relation.show()

	def createOrReplaceTempView(self, name: str) -> None:
		raise NotImplementedError

	def createGlobalTempView(self, name: str) -> None:
		raise NotImplementedError

	@property
	def write(self) -> DataFrameWriter:
		return DataFrameWriter(self)

__all__ = [
	"DataFrame"
]
