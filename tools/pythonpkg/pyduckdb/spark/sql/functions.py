from .column import Column
from typing import (
    Any
)

import duckdb

from duckdb import (
    CaseExpression,
    ColumnExpression,
    FunctionExpression,
    Expression
)

def col(column: str):
    return Column(ColumnExpression(column))

def when(condition: "Column", value: Any) -> Column:
    if not isinstance(condition, Column):
        raise TypeError("condition should be a Column")
    if not isinstance(value, Column):
        raise NotImplementedError()
    expr = CaseExpression(condition.expr, value.expr)
    return Column(expr)

# rel.select(struct('a', 'b').alias("struct"))
# Is equivalent to:
# rel.project("{'a': a, 'b': b} as struct")
# Full query:
#    select {'a': a, 'b': b} from (VALUES (1, 2), (3, 4)) tbl(a, b);

# FIXME: only works with 2 columns currently
def struct(*cols: Column) -> Column:
    return Column(FunctionExpression('struct_pack', cols))
