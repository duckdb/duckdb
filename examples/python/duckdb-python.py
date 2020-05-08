import duckdb

# basic SQL API

# connect to an in-memory temporary database
conn = duckdb.connect()

# if you want, you can create a cursor() like described in PEP 249 but its fully redundant
cursor = conn.cursor()

# run arbitrary SQL commands
conn.execute("CREATE TABLE test_table (i INTEGER, j STRING)")

# add some data
conn.execute("INSERT INTO test_table VALUES (1, 'one')")

# we can use placeholders for parameters
conn.execute("INSERT INTO test_table VALUES (?, ?)", [2, 'two'])

# we can provide multiple sets of parameters to executemany()
conn.executemany("INSERT INTO test_table VALUES (?, ?)", [[3, 'three'], [4, 'four']])

# fetch as pandas data frame
print(conn.execute("SELECT * FROM test_table").fetchdf())

# fetch as list of masked numpy arrays, cleaner when handling NULLs
print(conn.execute("SELECT * FROM test_table").fetchnumpy())


# we can query pandas data frames as if they were SQL views
# create a sample pandas data frame
import pandas as pd
test_df = pd.DataFrame.from_dict({"i":[1, 2, 3, 4], "j":["one", "two", "three", "four"]})

# make this data frame available as a view in duckdb
conn.register("test_df", test_df)
print(conn.execute("SELECT j FROM test_df WHERE i > 1").fetchdf())


# relation API, programmatic querying. relations are lazily evaluated chains of relational operators

# create a "relation" from a pandas data frame with an existing connection
rel = conn.from_df(test_df)
print(rel)

# alternative shorthand, use a built-in default connection to create a relation from a pandas data frame
rel = duckdb.df(test_df)
print(rel)

# create a relation from a CSV file

# first create a CSV file from our pandas example 
import tempfile, os
temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
test_df.to_csv(temp_file_name, index=False)

# now create a relation from it
rel = duckdb.from_csv_auto(temp_file_name)
print(rel)

# create a relation from an existing table
rel = conn.table("test_table")
print(rel)

# a relation has an alias (like a table name)
print(rel.alias)

# we can change the alias, useful for (self)joins for example
rel2 = rel.set_alias('new_alias')
print(rel2.alias)

# we can inspect the type of a relation
print(rel.type)

# or the column names that are in it
print(rel.columns)

# or the types of those columns
print(rel.types)

# now we can apply some operators to the relation
# filter the relation
print(rel.filter('i > 1'))

# project the relation, get some columns
print(rel.project('i, j'))

# or transform them
print(rel.project('i + 1'))

# order the relation
print(rel.order('j'))

# limit the rows returned
print(rel.limit(2))

# of course these things can be chained
print(rel.filter('i > 1').project('i + 1').order('j').limit(2))

# aggregate the relation
print(rel.aggregate("sum(i)"))

# non-aggregated columns create implicit grouping
print(rel.aggregate("j, sum(i)"))

# we can also explicit group the relation before aggregating
print(rel.aggregate("sum(i)", "j"))

# distinct values
print(rel.distinct())


# multi-relation operators are also supported, e.g union
print(rel.union(rel))

# join rel with itself on i
rel2 = conn.from_df(test_df)
print(rel.join(rel2, 'i'))

# for explicit join conditions the relations can be named using alias()
print(rel.set_alias('a').join(rel.set_alias('b'), 'a.i=b.i'))


# there are also shorthand methods to directly create a relation and apply an operator from pandas data frame objects
print(duckdb.filter(test_df, 'i > 1'))
print(duckdb.project(test_df, 'i +1'))
print(duckdb.order(test_df, 'j'))
print(duckdb.limit(test_df, 2))

print(duckdb.aggregate(test_df, "sum(i)"))
print(duckdb.distinct(test_df))

# when chaining only the first call needs to include the data frame parameter
print(duckdb.filter(test_df, 'i > 1').project('i + 1').order('j').limit(2))

# turn the relation into something else again




# compute the query result from the relation 
res = rel.execute()
print(res)
# res is a query result, you can call fetchdf() or fetchnumpy() or fetchone() on it
print(res.fetchone())
print(res.fetchall())

# convert a relation back to a pandas data frame
print(rel.to_df())

# df() is shorthand for to_df() on relations
print(rel.df())

# create a table in duckdb from the relation
print(rel.create("test_table2"))

# insert the relation's data into an existing table
conn.execute("CREATE TABLE test_table3 (i INTEGER, j STRING)")
print(rel.insert_into("test_table3"))

# Inserting elements into table_3
print(conn.values([5, 'five']).insert_into("test_table3"))
rel_3 = conn.table("test_table3")
rel_3.insert([6,'six'])

# create a SQL-accessible view of the relation
print(rel.create_view('test_view'))


# we can also directly run SQL queries on relation objects without explicitly creating a view
# the first parameter gives the rel object a view name so we can refer to it in queries
res = rel.query('my_name_for_rel', 'SELECT * FROM my_name_for_rel')
print(res)
# res is a query result, we can fetch with the methods described above, e.g.
print(res.fetchone())
print(res.fetchdf())
# or just use df(), a shorthand for fetchdf() on query results
print(res.df())

# this also works directly on data frames
res = duckdb.query(test_df, 'my_name_for_test_df', 'SELECT * FROM my_name_for_test_df')
print(res.df())



