# use flex to generate the scanner file for the parser
# the following version of bison is used:
# flex 2.5.35 Apple(flex-32)
import os
import subprocess
import re
from python_helpers import open_utf8

pg_path = os.path.join('extension', 'geo')
bison_bin = 'bison'
bison_file_path = os.path.join(pg_path, 'parser', 'lwin_wkt_parse.y')
target_header_file = os.path.join(pg_path, "include", 'liblwgeom', 'lwin_wkt_parse.hpp')
target_source_file = os.path.join(pg_path, 'liblwgeom', 'lwin_wkt_parse.cpp')

proc = subprocess.Popen([bison_bin, "-p wkt_yy", '-o' + target_source_file, '--defines=' + target_header_file, "-d", bison_file_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
stdout = proc.stdout.read().decode('utf8')
stderr = proc.stderr.read().decode('utf8')
if proc.returncode != None or len(stderr) > 0:
	print("Flex failed")
	print("stdout: ", stdout)
	print("stderr: ", stderr)
	exit(1)

with open_utf8(target_header_file, 'r') as f:
	header_text = f.read()

# add the duckdb namespace
header_text = "namespace duckdb {\n" + header_text

header_text = header_text + "\n} /* duckdb */\n"

with open_utf8(target_header_file, 'w+') as f:
	f.write(header_text)


with open_utf8(target_source_file, 'r') as f:
	source_text = f.read()

# add the duckdb namespace
source_text = source_text.replace("/*namespace duckdb{*/", "namespace duckdb{")

source_text = source_text + "\n} /* duckdb */\n"

with open_utf8(target_source_file, 'w+') as f:
	f.write(source_text)