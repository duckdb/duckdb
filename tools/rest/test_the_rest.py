import requests
import subprocess
import os.path
import socket
import time
from contextlib import closing
import urllib.parse
import sys

if (len(sys.argv) < 2):
	print("Usage: test_the_rest.py [path_to_tools_rest_binaries]")
	sys.exit(-1)

BIN_PREFIX=sys.argv[1]
DBFILE = "tpch_sf01.duckdb"


def find_free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('', 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


# check for binary
server_binary = "%s/duckdb_rest_server" % BIN_PREFIX
if not os.path.isfile(server_binary):
	raise Exception('could not find rest binary')


# create database if not exists
db_file = "%s/%s" % (BIN_PREFIX, DBFILE)
if not os.path.isfile(db_file):
	process = subprocess.Popen(("%s/duckdb_dbgen --database=%s --scale_factor=0.1" % (BIN_PREFIX, db_file)).split(' '))
	process.wait()
	if process.returncode != 0 or not os.path.isfile(db_file):
		raise Exception('dbgen failed')



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
			raise Exception('startup failed')
		break
	except:
		pass

def query(q):
	resp = requests.get("%s/query?q=%s" % (base_url, urllib.parse.quote(q)))
	if resp.status_code != 200:
		raise Exception('query %s failed' % q)
	return resp.json()

def fetch(ref):
	resp = requests.get("%s/fetch?ref=%s" % (base_url, ref))
	if resp.status_code != 200:
		raise Exception('fetch %s failed' % ref)
	return resp.json()

def close(ref):
	resp = requests.get("%s/close?ref=%s" % (base_url, ref))
	if resp.status_code != 200:
		raise Exception('fetch %s failed' % ref)
	return resp.json()



# basic small result set test
res = query("SELECT COUNT(*) FROM lineitem")
if not res['success'] or res['data'][0][0] != 600572:
	raise Exception('test failed')


res = close(res['ref'])
if not res['success']:
	raise Exception('test failed')


# close again, should not be successful
res = close(res['ref'])
if res['success']:
	raise Exception('test failed')



# basic large result set test
res = query("SELECT * FROM lineitem")
if not res['success']:
	raise Exception('test failed')

if res['column_count'] != 16:
	raise Exception('test failed')

if res['column_count'] != len(res['data']):
	raise Exception('test failed')

if res['column_count'] != len(res['name_index_map']):
	raise Exception('test failed')

if res['column_count'] != len(res['sql_types']):
	raise Exception('test failed')

if res['column_count'] != len(res['types']):
	raise Exception('test failed')

if res['column_count'] != len(res['names']):
	raise Exception('test failed')

res = fetch(res['ref'])
if not res['success']:
	raise Exception('test failed')

res = fetch(res['ref'])
if not res['success']:
	raise Exception('test failed')

res = close(res['ref'])
if not res['success']:
	raise Exception('test failed')

# fetch again, should fail
res = fetch(res['ref'])
if res['success']:
	raise Exception('test failed')



# invalid ref test
res = fetch("asdf")
if res['success']:
	raise Exception('test failed')


res = close("asdf")
if res['success']:
	raise Exception('test failed')



# timeout fetch test
res = query("SELECT * FROM lineitem")
if not res['success']:
	raise Exception('test failed')

time.sleep(1)

res = fetch(res['ref'])
if not res['success']:
	raise Exception('test failed')

time.sleep(1)

res = fetch(res['ref'])
if not res['success']:
	raise Exception('test failed')

time.sleep(1)

res = fetch(res['ref'])
if not res['success']:
	raise Exception('test failed')


time.sleep(1)

# still valid because the time between the individual fetch calls was less than two seconds
res = fetch(res['ref'])
if not res['success']:
	raise Exception('test failed')


time.sleep(4)

# this should now fail because the timeout hit
res = fetch(res['ref'])
if res['success']:
	raise Exception('test failed')



print("SUCCESS")
