from .column import Column

def col(x):
	pass

def when(x):
	pass

# rel.select(struct('a', 'b').alias("struct"))
# Is equivalent to:
# rel.project("{'a': a, 'b': b} as struct")
# Full query:
#    select {'a': a, 'b': b} from (VALUES (1, 2), (3, 4)) tbl(a, b);
def struct(*cols: Column) -> Column:
	pass
