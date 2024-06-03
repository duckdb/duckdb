# Example demonstrating slow performance from https://github.com/duckdb/duckdb/issues/10214
# Projection pushdown on DISTINCT ON doesn't work

import pandas as pd
import numpy as np
import duckdb
from time import perf_counter

duckdb.query("SET explain_output = 'all';")

n = 100_000
columns = 10
pd_df = pd.DataFrame({f'col{i}': 1000 * np.random.sample(n) for i in range(columns)})
query = """
SELECT col0
FROM 
(
    SELECT 
    DISTINCT ON (floor(col0))
    {columns}
    FROM pd_df
    ORDER by col0 DESC
)
"""
results = dict()
for columns in ['col0', '*',]:
    start = perf_counter()
    results[columns] = duckdb.query(query.format(columns=columns)).df()
    print(f'The query using `{columns}` took {perf_counter() - start} s')
    expl = duckdb.query("EXPLAIN " + query.format(columns=columns)).df()
    for row in range(2):
        print(expl.iloc[row, 0])
        print(expl.iloc[row, 1])
print(f"The results are equal: {results['*'].equals(results['col0'])}")