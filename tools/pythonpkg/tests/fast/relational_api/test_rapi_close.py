import duckdb
from decimal import Decimal
import pytest

# A closed connection should invalidate all relation's methods

con = duckdb.default_connection
con.execute("CREATE TABLE items(item VARCHAR, value DECIMAL(10,2), count INTEGER)")
con.execute("INSERT INTO items VALUES ('jeans', 20.0, 1), ('hammer', 42.2, 2)")
rel = con.table("items")
print(rel.df())
con.close()
with pytest.raises(Exception, match='This relation\'s connection is closed.'):
	print(rel)
with pytest.raises(Exception, match='This relation\'s connection is closed.'):
	rel.type
with pytest.raises(Exception, match='This relation\'s connection is closed.'):
	rel.columns
with pytest.raises(Exception, match='This relation\'s connection is closed.'):
	rel.types
with pytest.raises(Exception, match='This relation\'s connection is closed.'): 
	rel.dtypes
with pytest.raises(Exception, match='This relation\'s connection is closed.'):
	rel.__len__
with pytest.raises(Exception, match='This relation\'s connection is closed.'):
	rel.filter("")
with pytest.raises(Exception, match='This relation\'s connection is closed.'):
	rel.project("")
with pytest.raises(Exception, match='This relation\'s connection is closed.'):
	rel.order("")
with pytest.raises(Exception, match='This relation\'s connection is closed.'):
	rel.aggregate("")
with pytest.raises(Exception, match='This relation\'s connection is closed.'):
	rel.sum("")
with pytest.raises(Exception, match='This relation\'s connection is closed.'):
	rel.count("")
with pytest.raises(Exception, match='This relation\'s connection is closed.'):
	rel.median("")
with pytest.raises(Exception, match='This relation\'s connection is closed.'):
	rel.quantile("","")
with pytest.raises(Exception, match='This relation\'s connection is closed.'):
	rel.apply("","")
with pytest.raises(Exception, match='This relation\'s connection is closed.'):
	rel.min("")
with pytest.raises(Exception, match='This relation\'s connection is closed.'):
	rel.max("")
with pytest.raises(Exception, match='This relation\'s connection is closed.'):
	rel.mean("")
with pytest.raises(Exception, match='This relation\'s connection is closed.'):
	rel.var("")
with pytest.raises(Exception, match='This relation\'s connection is closed.'):
	rel.std("")
with pytest.raises(Exception, match='This relation\'s connection is closed.'):
	rel.value_count("")
with pytest.raises(Exception, match='This relation\'s connection is closed.'):
	rel.unique("")
with pytest.raises(Exception, match='This relation\'s connection is closed.'):
	rel.union()
with pytest.raises(Exception, match='This relation\'s connection is closed.'):
	rel.except_("")
with pytest.raises(Exception, match='This relation\'s connection is closed.'):
	rel.intersect("")
with pytest.raises(Exception, match='This relation\'s connection is closed.'):
	rel.join("", "")
with pytest.raises(Exception, match='This relation\'s connection is closed.'):
	rel.distinct()
with pytest.raises(Exception, match='This relation\'s connection is closed.'):
	rel.limit("")
with pytest.raises(Exception, match='This relation\'s connection is closed.'):
	rel.query("","")
with pytest.raises(Exception, match='This relation\'s connection is closed.'):
	rel.execute()
with pytest.raises(Exception, match='This relation\'s connection is closed.'):
	rel.write_csv("")
with pytest.raises(Exception, match='This relation\'s connection is closed.'):
	rel.insert_into("")
with pytest.raises(Exception, match='This relation\'s connection is closed.'):
	rel.insert("")
with pytest.raises(Exception, match='This relation\'s connection is closed.'):
	rel.create("")
with pytest.raises(Exception, match='This relation\'s connection is closed.'):
	rel.create_view("")
with pytest.raises(Exception, match='This relation\'s connection is closed.'):
	rel.to_arrow_table()
with pytest.raises(Exception, match='This relation\'s connection is closed.'):
	rel.arrow()
with pytest.raises(Exception, match='This relation\'s connection is closed.'):
	rel.to_df()
with pytest.raises(Exception, match='This relation\'s connection is closed.'):
	rel.df()
with pytest.raises(Exception, match='This relation\'s connection is closed.'):
	rel.fetchone()
with pytest.raises(Exception, match='This relation\'s connection is closed.'):
	rel.fetchall()
with pytest.raises(Exception, match='This relation\'s connection is closed.'):
	rel.map("")
with pytest.raises(Exception, match='This relation\'s connection is closed.'):
	rel.__str__
with pytest.raises(Exception, match='This relation\'s connection is closed.'):
	rel.__repr__