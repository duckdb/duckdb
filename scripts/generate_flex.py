# use flex to generate the scanner file for the parser
# the following version of bison is used:
# flex 2.5.35 Apple(flex-32)
import os
import subprocess
import re

pg_path = os.path.join('third_party', 'libpg_query')
flex_bin = 'flex'
flex_file_path = 'scan.l'
target_file = 'src_backend_parser_scan.cpp'

os.chdir(pg_path)
proc = subprocess.Popen([flex_bin, '--nounistd', '-o', target_file, flex_file_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
stdout = proc.stdout.read().decode('utf8')
stderr = proc.stderr.read().decode('utf8')
if proc.returncode != None or len(stderr) > 0:
	print("Flex failed")
	print("stdout: ", stdout)
	print("stderr: ", stderr)
	exit(1)

with open(target_file, 'r') as f:
	text = f.read()

# add the libpg_query namespace
text = text.replace('''
#ifndef FLEXINT_H
#define FLEXINT_H
''', '''
#ifndef FLEXINT_H
#define FLEXINT_H
namespace duckdb_libpgquery {
''')

text = text + "\n} /* duckdb_libpgquery */\n"

with open(target_file, 'w+') as f:
	f.write(text)
