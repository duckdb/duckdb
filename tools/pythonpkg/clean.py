import os, sys, shutil
# avoid importing from the current directory
sys.path = sys.path[1:]

try:
    import duckdb
except:
    exit(0)
next_dir = duckdb.__file__
while 'duckdb' in next_dir:
    base_dir = next_dir
    next_dir = next_dir.rsplit(os.path.sep, 1)[0]
if 'duckdb' not in base_dir:
    raise Exception("Failed to find DuckDB path to delete")
shutil.rmtree(base_dir)
