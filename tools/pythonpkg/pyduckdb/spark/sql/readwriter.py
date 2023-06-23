from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql.dataframe import DataFrame

class DataFrameWriter:
	def __init__(self, dataframe: "DataFrame"):
		self.dataframe = dataframe
		pass

	def saveAsTable(self, table_name: str) -> None:
		# register the dataframe or create a table from the contents?
		pass

__all__ = [
	"DataFrameWriter"
]
