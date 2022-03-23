# import duckdb
# import pytest
# import os
# import pandas as pd
# from datetime import timezone
# from datetime import timedelta
# from pandas import Timestamp

# try:
#     import pyarrow as pa
#     can_run = True
# except:
#     can_run = False

# # class TestArrowTimezone(object):
# #     def test_arrow_simple_utc(self, duckdb_cursor):
# #         if not can_run:
# #             return
# # 		pa.timestamp(unit="ns", tz="UTC"))
# pdf = pd.DataFrame({'a': [Timestamp(year=2019, month=1, day=1, nanosecond=500, tz=timezone(timedelta(hours=0)))]})
# arrow_table = pa.Table.from_pandas(pdf)
# con = duckdb.connect()
# query = con.execute("select * from arrow_table")