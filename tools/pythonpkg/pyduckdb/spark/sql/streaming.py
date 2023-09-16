from typing import TYPE_CHECKING, Optional, Union
from pyduckdb.spark.sql.types import StructType

if TYPE_CHECKING:
    from pyduckdb.spark.sql.dataframe import DataFrame
    from pyduckdb.spark.sql.session import SparkSession

PrimitiveType = Union[bool, float, int, str]
OptionalPrimitiveType = Optional[PrimitiveType]


class DataStreamWriter:
    def __init__(self, dataframe: "DataFrame"):
        self.dataframe = dataframe

    def toTable(self, table_name: str) -> None:
        # Should we register the dataframe or create a table from the contents?
        raise NotImplementedError


class DataStreamReader:
    def __init__(self, session: "SparkSession"):
        self.session = session

    def load(
        self,
        path: Optional[str] = None,
        format: Optional[str] = None,
        schema: Union[StructType, str, None] = None,
        **options: OptionalPrimitiveType
    ) -> "DataFrame":
        from pyduckdb.spark.sql.dataframe import DataFrame

        raise NotImplementedError


__all__ = ["DataStreamReader", "DataStreamWriter"]
