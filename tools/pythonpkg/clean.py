import duckdb, shutil, os

# try:
base_dir = duckdb.__file__.rsplit(os.path.sep, 1)[0]
for fpath in os.listdir(base_dir):
    if '_duckdb_extension' in fpath:
        full_path = os.path.join(base_dir, fpath)
        if os.path.isfile(full_path):
            os.remove(full_path)
        else:
            shutil.rmtree(os.path.join(base_dir, fpath))
