import boto3
import subprocess
import sys
import os

if (len(sys.argv) < 3):
	print("Usage: [prefix] [filename1] [filename2] ... ")
	exit(1)


def get_git_revision_short_hash():
    return subprocess.check_output(['git', 'rev-parse', '--short', 'HEAD']).decode("utf-8").strip()

prefix = sys.argv[1].strip()

# Hannes controls this AWS account
# Files are served via CloudFront CDN at https://download.duckdb.org/...

secret_key=os.getenv('SECRET_ACCESS_KEY')
if secret_key is None:
	print("Can't find SECRET_ACCESS_KEY in env ")
	exit(2)

s3 = boto3.client('s3', aws_access_key_id='AKIAVBLKPL2ZQYEIAP7T', aws_secret_access_key=secret_key)

files = sys.argv[2:]
basenames = [os.path.basename(x) for x in files]


basenames_set = set(basenames)
if len(basenames) != len(basenames_set):
	print("Name conflict")
	exit(3)

for f in files:
	base = os.path.basename(f)
	key = 'rev/%s/%s/%s' % (get_git_revision_short_hash(), prefix, base)
	print("%s\t->\thttps://download.duckdb.org/%s " % (f, key))
	s3.upload_file(f, 'duckdb-bin', key)


