import duckdb
from duckdb.typing import *
from faker import Faker


def generate_random_name(str1, str2):
    #fake = Faker()
    #fake2 = Faker()
    return str1+ str2


duckdb.create_function("random_name", generate_random_name, [VARCHAR, VARCHAR], VARCHAR)
res = duckdb.sql("SELECT random_name('UW', 'final project')").fetchall()
print(res)

#duckdb.create_aggregate_function("test", generate_random_name, [], DOUBLE)


 # Connect to an in-memory DuckDB database
con = duckdb.connect(database=':memory:')

    # Create a temporary table
con.execute("""
        CREATE TEMP TABLE sales (
            id INTEGER,
            category TEXT,
            amount DOUBLE
        )
    """
    )

# Insert some sample data

con.execute("""
        INSERT INTO sales (id, category, amount) VALUES
        (1, 'Electronics', 250.50),
        (2, 'Clothing', 99.99),
        (3, 'Electronics', 399.99),
        (4, 'Books', 19.99),
        (5, 'Clothing', 49.99),
        (6, 'Books', 10.99)
    """
    )
#con.create_function("random_name", generate_random_name, [], VARCHAR)
#con.create_aggregate_function("udf_avg", generate_random_name, [], DOUBLE)
con.create_aggregate_function("udf_sum", generate_random_name, [], DOUBLE)
result = con.execute("""
        SELECT udf_sum_double(amount) FROM sales
        """
        ).fetchall()
print(result)
