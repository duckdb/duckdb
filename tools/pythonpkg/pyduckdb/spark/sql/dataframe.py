from pyduckdb.spark.exception import ContributionsAcceptedError

from typing import (
    TYPE_CHECKING,
    List,
    Optional,
    Union,
    Tuple,
    overload
)

from pyduckdb.spark.sql.readwriter import DataFrameWriter
from pyduckdb.spark.sql.types import Row, StructType
from pyduckdb.spark.sql.type_utils import duckdb_to_spark_schema
from pyduckdb.spark.sql.column import Column
import duckdb

if TYPE_CHECKING:
    from pyduckdb.spark.sql.session import SparkSession

class DataFrame:
    def __init__(self, relation: duckdb.DuckDBPyRelation, session: "SparkSession"):
        self.relation = relation
        self.session = session
        self._schema = duckdb_to_spark_schema(self.relation.columns, self.relation.types) if self.relation else None

    def show(self, **kwargs) -> None:
        self.relation.show()

    def createOrReplaceTempView(self, name: str) -> None:
        raise NotImplementedError

    def createGlobalTempView(self, name: str) -> None:
        raise NotImplementedError

    # select *, 5 from (VALUES (1)) tbl(a)
    def withColumn(self, columnName: str, col: Column) -> "DataFrame":
        cols = [duckdb.ColumnExpression(x) for x in self.relation.columns]
        cols.append(col.expr.alias(columnName))
        rel = self.relation.select(*cols)
        return DataFrame(rel, self.session)

    @property
    def schema(self) -> StructType:
        """Returns the schema of this :class:`DataFrame` as a :class:`pyspark.sql.types.StructType`.

        .. versionadded:: 1.3.0

        Examples
        --------
        >>> df.schema
        StructType([StructField('age', IntegerType(), True),
                    StructField('name', StringType(), True)])
        """
        return self._schema

    @overload
    def __getitem__(self, item: Union[int, str]) -> Column:
        ...

    @overload
    def __getitem__(self, item: Union[Column, List, Tuple]) -> "DataFrame":
        ...

    def __getitem__(self, item: Union[int, str, Column, List, Tuple]) -> Union[Column, "DataFrame"]:
        """Returns the column as a :class:`Column`.

        .. versionadded:: 1.3.0

        Examples
        --------
        >>> df.select(df['age']).collect()
        [Row(age=2), Row(age=5)]
        >>> df[ ["name", "age"]].collect()
        [Row(name='Alice', age=2), Row(name='Bob', age=5)]
        >>> df[ df.age > 3 ].collect()
        [Row(age=5, name='Bob')]
        >>> df[df[0] > 3].collect()
        [Row(age=5, name='Bob')]
        """
        if isinstance(item, str):
            return self.item
        #elif isinstance(item, Column):
        #    return self.filter(item)
        #elif isinstance(item, (list, tuple)):
        #    return self.select(*item)
        #elif isinstance(item, int):
        #    jc = self._jdf.apply(self.columns[item])
        #    return Column(jc)
        else:
            raise TypeError("unexpected item type: %s" % type(item))

    def __getattr__(self, name: str) -> Column:
        """Returns the :class:`Column` denoted by ``name``.

        .. versionadded:: 1.3.0

        Examples
        --------
        >>> df.select(df.age).collect()
        [Row(age=2), Row(age=5)]
        """
        if name not in self.relation.columns:
            raise AttributeError(
                "'%s' object has no attribute '%s'" % (self.__class__.__name__, name)
            )
        return Column(duckdb.ColumnExpression(name))

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

__all__ = [
    "DataFrame"
]
