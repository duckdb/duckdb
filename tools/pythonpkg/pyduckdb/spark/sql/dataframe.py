from typing_extensions import Self
from pyduckdb.spark.sql.readwriter import DataFrameWriter

class DataFrame:
	def __init__(self):
		pass
	
	def show(self) -> None:
		print("this is a dataframe")

	def createOrReplaceTempView(self, name: str) -> None:
		pass
	
	def createGlobalTempView(self, name: str) -> None:
		pass

	@property
	def write(self) -> DataFrameWriter:
		return DataFrameWriter(self)

__all__ = [
	"DataFrame"
]
