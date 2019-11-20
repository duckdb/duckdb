import requests
import subprocess
import os.path
import socket
import time
from contextlib import closing
import urllib.parse

BIN_PREFIX="../../build/debug/tools/rest"
DBFILE = "tpch_sf01.duckdb"


def find_free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('', 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


# create binary
process = subprocess.Popen("make debug -C ../..".split(' '))
process.wait()
if process.returncode != 0:
	raise

server_binary = "%s/duckdb_rest_server" % BIN_PREFIX
# check if binary exists
if not os.path.isfile(server_binary):
	raise

# create database if not exists
db_file = "%s/%s" % (BIN_PREFIX, DBFILE)
if not os.path.isfile(db_file):
	process = subprocess.Popen(("%s/duckdb_dbgen --database=%s --scale_factor=0.1" % (BIN_PREFIX, db_file)).split(' '))
	process.wait()
	if process.returncode != 0 or not os.path.isfile(db_file):
		raise


# launch server
port = find_free_port()
base_url = "http://localhost:%s" % (port)

process = subprocess.Popen(("%s --listen=localhost --port=%s --database=%s --read_only --fetch_timeout=2" % (server_binary, port, db_file)).split(' '))

# now wait up to 10 seconds for server to be up
count = 0
while count < 10:
	time.sleep(1)
	count += 1
	try:
		resp = requests.get("%s/query?q=SELECT+42" % base_url)
		if resp.status_code != 200:
			raise
		break
	except:
		pass

def query(q):
	resp = requests.get("%s/query?q=%s" % (base_url, urllib.parse.quote(q)))
	if resp.status_code != 200:
		raise
	return resp.json()

def fetch(ref):
	resp = requests.get("%s/fetch?ref=%s" % (base_url, ref))
	if resp.status_code != 200:
		raise
	return resp.json()

def close(ref):
	resp = requests.get("%s/close?ref=%s" % (base_url, ref))
	if resp.status_code != 200:
		raise
	return resp.json()



# basic small result set test
res = query("SELECT COUNT(*) FROM lineitem")
if not res['success'] or res['data'][0][0] != 600572:
	raise

res = close(res['ref'])
if not res['success']:
	raise

# close again, should not be successful
res = close(res['ref'])
if res['success']:
	raise



# basic large result set test
res = query("SELECT * FROM lineitem")
if not res['success']:
	raise

if res['column_count'] != 16:
	raise

if res['column_count'] != len(res['data']):
	raise

if res['column_count'] != len(res['name_index_map']):
	raise

if res['column_count'] != len(res['sql_types']):
	raise

if res['column_count'] != len(res['types']):
	raise

if res['column_count'] != len(res['names']):
	raise

res = fetch(res['ref'])
if not res['success']:
	raise

res = fetch(res['ref'])
if not res['success']:
	raise

res = close(res['ref'])
if not res['success']:
	raise

# fetch again, should fail
res = fetch(res['ref'])
if res['success']:
	raise



# invalid ref test
res = fetch("asdf")
if res['success']:
	raise

res = close("asdf")
if res['success']:
	raise



# timeout fetch test
res = query("SELECT * FROM lineitem")
if not res['success']:
	raise

time.sleep(1)

res = fetch(res['ref'])
if not res['success']:
	raise

time.sleep(3)

# this should now fail because the timeout hit
res = fetch(res['ref'])
if res['success']:
	raise



print("SUCCESS")
