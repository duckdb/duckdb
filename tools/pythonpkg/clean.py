import os
import pathlib
import shutil
import sys

# avoid importing from the current directory
sys.path = sys.path[1:]

try:
    import duckdb
except:
    print("Failed to import duckdb")
    exit(0)
next_dir = duckdb.__file__
while 'duckdb' in pathlib.PurePath(next_dir).name:
    base_dir = next_dir
    next_dir = next_dir.rsplit(os.path.sep, 1)[0]
if 'duckdb' not in base_dir:
    raise Exception("Failed to find DuckDB path to delete")

answer = ""
while answer not in ["y", "yes", "n", "no"]:
    print("The following directory and all files in it will be deleted:")
    print(base_dir)
    answer = input(f"Delete directory and all files in it? (y/n): ")
    if answer.lower() in ["y", "yes"]:
        shutil.rmtree(base_dir)
    elif answer.lower() in ["n", "no"]:
        print("Aborting clean.py")
        quit()
    else:
        print("Please indicate if you wish to continue deletion (y) or abort (n)")
