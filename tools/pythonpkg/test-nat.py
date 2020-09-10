import pandas
import numpy as np
import datetime
import duckdb
df = pandas.DataFrame([{"col1":"val1","col2":1.05},{"col1":"val3","col2":np.NaN}])
df["newcol1"] = np.where(df["col1"] == "val1",np.NaN,df["col1"])
current_time = datetime.datetime.now().replace(microsecond=0)
df['datetest'] = current_time
df.loc[0,'datetest'] = pandas.NaT
conn = duckdb.connect(':memory:')
conn.register('testing_null_values', df)
results = conn.execute('select * from testing_null_values').fetchall()
assert results[0][0] == 'val1'
assert results[0][1] == 1.05
assert results[0][2] == None
assert results[0][3] == None
assert results[1][0] == 'val3'
assert results[1][1] == None
assert results[1][2] == 'val3'
assert results[1][3] == current_time
result_df = conn.execute('select * from testing_null_values').fetchdf()
assert result_df['col1'][0] == df['col1'][0]
assert result_df['col1'][1] == df['col1'][1]
assert result_df['col2'][0] == df['col2'][0]
assert np.isnan(result_df['col2'][1])
assert np.isnan(result_df['newcol1'][0])
assert result_df['newcol1'][1] == df['newcol1'][1]
assert pandas.isnull(result_df['datetest'][0])
assert result_df['datetest'][1] == df['datetest'][1]
