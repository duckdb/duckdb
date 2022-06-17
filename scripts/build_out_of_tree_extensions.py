import argparse
import csv
import subprocess
import tempfile
import os
import glob
import pathlib 
import shutil
import sys

parser = argparse.ArgumentParser(description='Builds out-of-tree extensions for DuckDB')

parser.add_argument('--extensions', action='store',
                    help='CSV file with DuckDB extensions to build', default=".github/workflows/extensions.csv")


args = parser.parse_args()


tasks = []

def exec(cmd):
    print(cmd)
    sys.stdout.flush()

    res = subprocess.Popen(cmd.split(' '))
    res.wait()
    if res.returncode != 0:
        raise ValueError('failed to execute %s' % cmd)


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


basedir = os.getcwd()

for task in tasks:
    print(task)
    clonedir = task['name'] + "_clone"
    exec('git clone %s %s' % (task['url'], clonedir))
    os.chdir(clonedir)
    exec('git checkout %s' % (task['commit']))
    os.chdir(basedir)
    os.environ['BUILD_OUT_OF_TREE_EXTENSION'] = clonedir
    exec('make')
print("done")