import subprocess
import sys
import os

if len(sys.argv) < 2 or not os.path.isfile(sys.argv[1]):
	print("Usage: [libduckdb dynamic library file, release build]")
	exit(1)

res = subprocess.run('nm -g -U -j -C'.split(' ') + [sys.argv[1]], check=True, capture_output=True)
if res.returncode != 0:
	raise ValueError('Failed to run `nm`') 

culprits = []

for symbol in res.stdout.decode('utf-8').split('\n'):
	if len(symbol.strip()) == 0:
		continue
	if 'duckdb::' in symbol:
		continue
	if 'duckdb_miniz::' in symbol:
		continue
	if 'duckdb_fmt::' in symbol:
		continue
	if symbol.startswith('_duckdb_'):
		continue
	if symbol.startswith('duckdb_'):
		continue
	# not so sure about that one
	if symbol.startswith('_utf8proc_'):
		continue

	culprits.append(symbol)
 

if len(culprits) > 0:
	print("Found leaked symbols. Either white-list above or change visibility:")
	for symbol in culprits:
		print(symbol)
	sys.exit(1)


sys.exit(0)