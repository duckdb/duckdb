import argparse
import csv
import subprocess
import tempfile
import os
import glob
import pathlib 
import shutil

parser = argparse.ArgumentParser(description='Builds out-of-tree extensions for DuckDB')


parser.add_argument('--root', action='store',
                    help='Include folder for DuckDB', default="src/include")

parser.add_argument('--build', action='store',
                    help='Location of DuckDB Library', default="build/release/src")

parser.add_argument('--extensions', action='store',
                    help='CSV file with DuckDB extensions to build', default=".github/workflows/extensions.csv")

parser.add_argument('--output', action='store',
                    help='Folder to store the created extensions', required=True)

args = parser.parse_args()


tasks = []

def exec(cmd):
    res = subprocess.Popen(cmd.split(' '), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    res.wait()
    if res.returncode != 0:
        raise ValueError('failed to execute %s' % cmd)
    return res.stdout.read()


reader = csv.reader(open(args.extensions))
for row in reader:
    if len(row) != 3:
        raise ValueError('Row malformed' + str(row))

    name = row[0].strip()
    url = row[1].strip()
    commit = row[2].strip()

    if len(name) == 0 or len(url) == 0 or len(commit) != 40:
       raise ValueError('Row malformed' + str(row))

    tasks+= [{'name' : row[0], 'url' : row[1], 'commit' : row[2]}]

# just make sure this works
exec('git --help')
exec('cmake --help')

tempdir = tempfile.mkdtemp()
basedir = os.getcwd()

for task in tasks:
    print(task)
    clonedir = os.path.join(tempdir, task['name'] + "_clone")
    exec('git clone %s %s' % (task['url'], clonedir))
    os.chdir(clonedir)
    exec('git checkout %s' % (task['commit']))
    os.chdir(basedir)
    builddir = os.path.join(tempdir, task['name'] + "_build")
    exec('cmake -S %s -B %s -DDUCKDB_INCLUDE_FOLDER=%s -DDUCKDB_LIBRARY_FOLDER=%s' % (clonedir, builddir, os.path.abspath(args.root), os.path.abspath(args.build)))
    exec('cmake --build %s' % (builddir))

    for path in pathlib.Path(builddir).rglob('*.duckdb_extension'):
        res_path = os.path.join(args.output, path.name)
        shutil.copyfile(path, res_path)
        print(res_path)

print("done")