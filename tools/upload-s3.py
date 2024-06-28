import subprocess
import sys
import os

if len(sys.argv) < 3:
    print("Usage: [prefix] [filename1] [filename2] ... ")
    exit(1)


def git_rev_hash():
    return subprocess.check_output(['git', 'rev-parse', 'HEAD']).decode("utf-8").strip()


prefix = sys.argv[1].strip()

# Hannes controls this web server
# Files are served at https://download.duckdb.org/...

secret_key = os.getenv('DAV_PASSWORD')
if secret_key is None:
    print("Can't find DAV_PASSWORD in env ")
    exit(2)


files = sys.argv[2:]
basenames = [os.path.basename(x) for x in files]

basenames_set = set(basenames)
if len(basenames) != len(basenames_set):
    print("Name conflict")
    exit(3)


git_hash = git_rev_hash()

folder = 'rev/%s/%s' % (git_hash, prefix)


def curlcmd(cmd, path):
    p = subprocess.Popen(
        ['curl', '--retry', '10']
        + cmd
        + ['http://duckdb:%s@dav10635776.mywebdav.de/duckdb-download/%s' % (secret_key, path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    output, err = p.communicate()
    rc = p.returncode

    if p.returncode != 0:
        print(err)
        exit(4)


# create dirs, no recursive create supported by webdav
f_p = ''
for p in folder.split('/'):
    f_p += p + '/'
    curlcmd(['-X', 'MKCOL'], f_p)

for f in files:
    base = os.path.basename(f)
    key = '%s/%s' % (folder, base)
    print("%s\t->\thttps://download.duckdb.org/%s " % (f, key))
    curlcmd(['-T', f], key)
