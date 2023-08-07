from ..exception import ContributionsAcceptedError

from typing import TYPE_CHECKING, List, Optional, Union, Tuple, overload
from duckdb import StarExpression, ColumnExpression

from .readwriter import DataFrameWriter
from .types import Row, StructType
from .type_utils import duckdb_to_spark_schema
from .column import Column
import duckdb

if TYPE_CHECKING:
    from .session import SparkSession


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

    def withColumnRenamed(self, columnName: str, newName: str) -> "DataFrame":
        if columnName not in self.relation:
            raise ValueError(f"DataFrame does not contain a column named {columnName}")
        cols = []
        for x in self.relation.columns:
            col = ColumnExpression(x)
            if x.casefold() == columnName.casefold():
                col = col.alias(newName)
            cols.append(col)
        rel = self.relation.select(*cols)
        return DataFrame(rel, self.session)

    def withColumn(self, columnName: str, col: Column) -> "DataFrame":
        if columnName in self.relation:
            # We want to replace the existing column with this new expression
            cols = []
            for x in self.relation.columns:
                if x.casefold() == columnName.casefold():
                    cols.append(col.expr.alias(columnName))
                else:
                    cols.append(ColumnExpression(x))
        else:
            cols = [ColumnExpression(x) for x in self.relation.columns]
            cols.append(col.expr.alias(columnName))
        rel = self.relation.select(*cols)
        return DataFrame(rel, self.session)

    def select(self, *cols) -> "DataFrame":
        cols = list(cols)
        if len(cols) == 1:
            cols = cols[0]
        if isinstance(cols, list):
            projections = [x.expr if isinstance(x, Column) else ColumnExpression(x) for x in cols]
        else:
            projections = [cols.expr if isinstance(cols, Column) else ColumnExpression(cols)]
        rel = self.relation.select(*projections)
        return DataFrame(rel, self.session)

    @property
    def columns(self) -> List[str]:
        """Returns all column names as a list.

        .. versionadded:: 1.3.0

        Examples
        --------
        >>> df.columns
        ['age', 'name']
        """
        return [f.name for f in self.schema.fields]

    def drop(self, *cols: "ColumnOrName") -> "DataFrame":  # type: ignore[misc]
        if len(cols) == 1:
            col = cols[0]
            if isinstance(col, str):
                exclude = [col]
            elif isinstance(col, Column):
                exclude = [col.expr]
            else:
                raise TypeError("col should be a string or a Column")
        else:
            for col in cols:
                if not isinstance(col, str):
                    raise TypeError("each col in the param list should be a string")
            exclude = list(cols)
        # Filter out the columns that don't exist in the relation
        exclude = [x for x in exclude if x in self.relation.columns]
        expr = StarExpression(exclude=exclude)
        return DataFrame(self.relation.select(expr), self.session)

    def __contains__(self, item: str):
        """
        Check if the :class:`DataFrame` contains a column by the name of `item`
        """
        return item in self.relation

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
        # elif isinstance(item, Column):
        #    return self.filter(item)
        # elif isinstance(item, (list, tuple)):
        #    return self.select(*item)
        # elif isinstance(item, int):
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
            raise AttributeError("'%s' object has no attribute '%s'" % (self.__class__.__name__, name))
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


__all__ = ["DataFrame"]
