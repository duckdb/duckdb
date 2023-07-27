from typing import TYPE_CHECKING, Union, List, Optional
from pyduckdb.spark.sql.types import StructType

PrimitiveType = Union[bool, float, int, str]
OptionalPrimitiveType = Optional[PrimitiveType]

if TYPE_CHECKING:
    from pyduckdb.spark.sql.dataframe import DataFrame
    from pyduckdb.spark.sql.session import SparkSession


class DataFrameWriter:
    def __init__(self, dataframe: "DataFrame"):
        self.dataframe = dataframe

    def saveAsTable(self, table_name: str) -> None:
        relation = self.dataframe.relation
        relation.create(table_name)


class DataFrameReader:
    def __init__(self, session: "SparkSession"):
        raise NotImplementedError
        self.session = session

    def load(
        self,
        path: Union[str, List[str], None] = None,
        format: Optional[str] = None,
        schema: Union[StructType, str, None] = None,
        **options: OptionalPrimitiveType
    ) -> "DataFrame":
        from pyduckdb.spark.sql.dataframe import DataFrame

        raise NotImplementedError


__all__ = ["DataFrameWriter", "DataFrameReader"]
