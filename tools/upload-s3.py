import subprocess
import sys
import os
import ftplib

if (len(sys.argv) < 3):
    print("Usage: [prefix] [filename1] [filename2] ... ")
    exit(1)


def git_rev_hash():
    return subprocess.check_output(['git', 'rev-parse', 'HEAD']).decode("utf-8").strip()

prefix = sys.argv[1].strip()

# Hannes controls this web server
# Files are served at https://download.duckdb.org/...

secret_key=os.getenv('FTP_PASSWORD')
if secret_key is None:
    print("Can't find FTP_PASSWORD in env ")
    exit(2)

ftp = ftplib.FTP_TLS('wp10635776.server-he.de','ftp10635776-duckdb',secret_key)
ftp.set_debuglevel(1)
ftp.set_pasv(True)


files = sys.argv[2:]
basenames = [os.path.basename(x) for x in files]


def create_path(path):
    if (path == '' or path == '/'):
        return()
    files = ftp.nlst()
    first_entry = path.split('/')[0]
    if first_entry not in files:
        ftp.mkd(first_entry)
        ftp.sendcmd('SITE CHMOD 755 %s' % first_entry)
    ftp.cwd(first_entry)
    create_path('/'.join(path.split('/')[1:]))


basenames_set = set(basenames)
if len(basenames) != len(basenames_set):
    print("Name conflict")
    exit(3)

folder = 'rev/%s/%s' % (git_rev_hash(), prefix)
create_path(folder)

for f in files:
    base = os.path.basename(f)
    key = '%s/%s' % (folder, base)
    print("%s\t->\thttps://download.duckdb.org/%s " % (f, key))

    file = open(f,'rb')
    ftp.storbinary('STOR %s' % base, file)
    file.close()

    ftp.sendcmd('SITE CHMOD 755 %s' % base)


ftp.close()
