from ..exception import ContributionsAcceptedError

from typing import TYPE_CHECKING, List

from .readwriter import DataFrameWriter
from .types import Row, StructType
from .type_utils import duckdb_to_spark_schema
import duckdb

if TYPE_CHECKING:
    from .session import SparkSession


class DataFrame:
    def __init__(self, relation: duckdb.DuckDBPyRelation, session: "SparkSession"):
        self.relation = relation
        self.session = session
        self._schema = duckdb_to_spark_schema(self.relation.columns, self.relation.types) if self.relation else None

    def show(self) -> None:
        self.relation.show()

    def createOrReplaceTempView(self, name: str) -> None:
        self.relation.create_view(name, True)

    def createGlobalTempView(self, name: str) -> None:
        raise NotImplementedError

    @property
    def schema(self) -> StructType:
        """Returns the schema of this :class:`DataFrame` as a :class:`pyduckdb.spark.sql.types.StructType`.

        .. versionadded:: 1.3.0

        Examples
        --------
        >>> df.schema
        StructType([StructField('age', IntegerType(), True),
                    StructField('name', StringType(), True)])
        """
        return self._schema

    @property
    def write(self) -> DataFrameWriter:
        return DataFrameWriter(self)

    def printSchema(self):
        raise ContributionsAcceptedError

    def _cast_types(self, *types) -> "DataFrame":
        existing_columns = self.relation.columns
        types_count = len(types)
        assert types_count == len(existing_columns)
        cast_expressions = [f'"{existing}"::{target_type}' for existing, target_type in zip(existing_columns, types)]
        cast_expressions = ', '.join(cast_expressions)
        new_rel = self.relation.project(cast_expressions)
        return DataFrame(new_rel, self.session)

    def toDF(self, *cols) -> "DataFrame":
        existing_columns = self.relation.columns
        column_count = len(cols)
        assert column_count == len(existing_columns)
        projections = [f'"{existing}" as "{new}"' for existing, new in zip(existing_columns, cols)]
        projections = ', '.join(projections)
        new_rel = self.relation.project(projections)
        return DataFrame(new_rel, self.session)

    def collect(self) -> List[Row]:
        columns = self.relation.columns
        result = self.relation.fetchall()
        rows = [Row(**dict(zip(columns, x))) for x in result]
        return rows


__all__ = ["DataFrame"]
