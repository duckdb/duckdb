from .column import Column

import duckdb

def col(column: str):
	return Column(duckdb.ColumnExpression(column))

def when(x):
	pass

# rel.select(struct('a', 'b').alias("struct"))
# Is equivalent to:
# rel.project("{'a': a, 'b': b} as struct")
# Full query:
#    select {'a': a, 'b': b} from (VALUES (1, 2), (3, 4)) tbl(a, b);

# FIXME: only works with 2 columns currently
def struct(*cols: Column) -> Column:
	assert len(cols) == 2
	col1 = cols[0]
	col2 = cols[1]
	return Column(duckdb.BinaryFunctionExpression('struct_pack', col1.expr, col2.expr))
