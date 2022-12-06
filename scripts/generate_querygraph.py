# generate_querygraph.py
# This script takes a json file as input that is the result of the QueryProfiler of duckdb
# and converts it into a Query Graph.

import os
import sys
from python_helpers import open_utf8

sys.path.insert(0, 'benchmark')

from tools.pythonpkg.duckdb_query_graph import generate

arguments = sys.argv
if len(arguments) <= 1:
	print("Usage: python generate_querygraph.py [input.json] [output.html] [open={1,0}]")
	exit(1)

input = arguments[1]
if len(arguments) <= 2:
	if ".json" in input:
		output = input.replace(".json", ".html")
	else:
		output = input + ".html"
else:
	output = arguments[2]

open_output = True
if len(arguments) >= 4:
	open_arg = arguments[3].lower().replace('open=', '')
	if open_arg == "1" or open_arg == "true":
		open_output = True
	elif open_arg == "0" or open_arg == "false":
		open_output = False
	else:
		print("Incorrect input for open_output, expected TRUE or FALSE")
		exit(1)

generate(input, output)

with open(output, 'r') as f:
	text = f.read()

if open_output:
	os.system('open "' + output.replace('"', '\\"') + '"')