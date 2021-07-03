import pandas

df = pandas.DataFrame({'value0': [1234, -1234, 1, -1, 0, None], 'value1': [12.34, -12.34, 1.0, -1.0, 0.0, None], 'value2': [12345.6789, -9765.4321, 1.0, -1.0, 0.0, None], 'value3': [123456789.987654321, -987654321.123456789, 1.0, -1.0, 0.0, None], 'value4': ['922337203685477580700.922306854775000', '-922337236854775807.92233720306854775', '1.0', '-1.0', '0.0', '0.0']})

print(df)

import decimal

df['value0'] = df['value0'].astype(str).map(decimal.Decimal)
df['value1'] = df['value1'].astype(str).map(decimal.Decimal)
df['value2'] = df['value2'].astype(str).map(decimal.Decimal)
df['value3'] = df['value3'].astype(str).map(decimal.Decimal)
df['value4'] = df['value4'].map(decimal.Decimal)

print(df)

df.to_parquet("pandas_decimal.parquet", index=False)


