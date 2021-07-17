import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

df = pd.DataFrame({'önë': [1, 2, 3],
                   '': ['foo', 'bar', 'baz'],
                   '🦆': [True, False, True]})
table = pa.Table.from_pandas(df)
pq.write_table(table, 'data/parquet-testing/silly-names.parquet')