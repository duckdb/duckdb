from .column import Column
from typing import (
    Any
)

import duckdb

from duckdb import (
    CaseExpression,
    ConstantExpression,
    ColumnExpression,
    FunctionExpression,
    Expression
)

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
