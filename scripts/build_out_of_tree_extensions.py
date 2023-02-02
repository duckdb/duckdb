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
                    help='CSV file with DuckDB extensions to build', default=os.path.join(".github", "config", "extensions.csv"))
parser.add_argument('--aarch64-cc', help='Enables Linux aarch64 crosscompile build', action='store_true')
parser.add_argument('--github-ref', action='store',
                    help='The github ref this job is launched from', default='')

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
# This skips the first row (i.e., the header) of the CSV file.
next(reader)
for row in reader:
    if len(row) != 4:
        raise ValueError('Row malformed' + str(row))

    name = row[0].strip()
    url = row[1].strip()
    commit = row[2].strip()
    if not url:
        # This is not an out-of-tree extension
        continue
    if len(name) == 0 or len(url) == 0 or len(commit) != 40:
       raise ValueError('Row malformed' + str(row))

    tasks+= [{'name' : row[0], 'url' : row[1], 'commit' : row[2], 'options' : row[3]}]

def build_extension(task):
    print(task)
    if os.name == 'nt' and 'no-windows' in task['options']:
        return False
    if 'main-repo-only' in task['options'] and args.github_ref != 'refs/heads/master':
        return False
    return True

basedir = os.getcwd()

clonedirs = []
for task in tasks:
    print(task)
    if build_extension(task):
        clonedir = "extension/" + task['name'] + "_clone"
        if not os.path.isdir(clonedir):
            exec('git clone %s %s' % (task['url'], clonedir))
        os.chdir(clonedir)
        exec('git checkout %s' % (task['commit']))
        os.chdir(basedir)
        print(f"Building extension \"{task['name']}\" from URL \"{task['url']}\" at commit \"{task['commit']}\" at clonedir \"{clonedir}\"")
        clonedirs.append(clonedir)

os.environ['BUILD_OUT_OF_TREE_EXTENSION'] = ';'.join(clonedirs)
if (args.aarch64_cc):
    os.environ['CC'] = "aarch64-linux-gnu-gcc"
    os.environ['CXX'] = "aarch64-linux-gnu-g++"
exec('make')
print("done")
