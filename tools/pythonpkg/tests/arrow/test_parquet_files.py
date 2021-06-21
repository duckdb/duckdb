# import duckdb
# import os
# import sys
# try:
#     import pyarrow
#     import pyarrow.parquet
#     can_run = True
# except:
#     can_run = False


# # Due to row groups or schema differences, we check equality through arrow and pandas
# def check_equal(arrow,duck_arrow):
#     duck_arrow = duck_arrow.combine_chunks()
#     arrow = arrow.combine_chunks()
#     print (arrow)
#     print (duck_arrow)

#     return duck_arrow.equals(arrow, check_metadata=False)


# # class TestArrowParquetFiles(object):
# #     def test_parquet_files_roundtrip(self, duckdb_cursor):
# files_path = []
# path = "/home/holanda/Documents/duckdb/test/sql/copy/parquet/data"
# # skip = ["manyrowgroups.parquet"] # We dont output rowgroups
# skip = set([])
# for root, dirs, files in os.walk(path):
#     for file in files:
#         if file.endswith(".parquet"):
#             if file not in skip:
#                 files_path.append(os.path.join(root, file))


# # files_path = ['/home/holanda/Documents/duckdb/test/sql/copy/parquet/data/map.parquet']
# for file in files_path:
#     print (file)
#     #Read File with Arrow
#     arrow = pyarrow.parquet.read_table(file)

#     #Do round-trip from duckdb
#     arrow_duck = duckdb.from_arrow_table(arrow).arrow()

#     #Check if files are the same

#     if not check_equal(arrow,arrow_duck):
#         print ("fail: "  + file)


