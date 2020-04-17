
# run SQL smith and collect breaking queries
import os
import re
import subprocess

def run_sqlsmith():
	subprocess.call(['build/debug/third_party/sqlsmith/sqlsmith', '--duckdb=:memory:'])

def get_file(i):
	return 'sqlsmith-queries/sqlsmith-%s.sql' % str(i)

i = 1
os.system('mkdir -p sqlsmith-queries')
while os.path.isfile(get_file(i)):
	i += 1
while True:
	# run SQL smith
	run_sqlsmith()
	# get the breaking query
	with open('sqlsmith.log', 'r') as f:
		text = re.sub('[ \t\n]+', ' ', f.read())

	with open(get_file(i), 'w+') as f:
		f.write(text)
		f.write('\n')
	i += 1

