from .column import Column
from typing import Any

import duckdb

from duckdb import CaseExpression, ConstantExpression, ColumnExpression, FunctionExpression, Expression
from ._typing import ColumnOrName


def col(column: str):
    return Column(ColumnExpression(column))


def when(condition: "Column", value: Any) -> Column:
    if not isinstance(condition, Column):
        raise TypeError("condition should be a Column")
    v = value.expr if isinstance(value, Column) else value
    expr = CaseExpression(condition.expr, v)
    return Column(expr)


def _inner_expr_or_val(val):
    return val.expr if isinstance(val, Column) else val


def struct(*cols: Column) -> Column:
    return Column(FunctionExpression('struct_pack', *[_inner_expr_or_val(x) for x in cols]))


def lit(col: Any) -> Column:
    return col if isinstance(col, Column) else Column(ConstantExpression(col))


def _invoke_function(function: str, *arguments):
    return Column(FunctionExpression(function, *arguments))


def _to_column(col: ColumnOrName) -> Column:
    return col.expr if isinstance(col, Column) else ColumnExpression(col)


def regexp_replace(str: "ColumnOrName", pattern: str, replacement: str) -> Column:
    r"""Replace all substrings of the specified string value that match regexp with rep.

    .. versionadded:: 1.5.0

    Examples
    --------
    >>> df = spark.createDataFrame([('100-200',)], ['str'])
    >>> df.select(regexp_replace('str', r'(\d+)', '--').alias('d')).collect()
    [Row(d='-----')]
    """
    return _invoke_function("regexp_replace", _to_column(str), pattern, replacement)
