import pandas

df = pandas.DataFrame({'value': [0.1, 0.0, 1.0, 1.1, None]})

print(df)

import decimal

df['value_decimal'] = df['value'].astype(str).map(decimal.Decimal)
print(df)

df.to_parquet("pandas_decimal.parquet", index=False)
