# generate_querygraph.py
# This script takes a json file as input that is the result of the QueryProfiler of duckdb
# and converts it into a Query Graph.

import os
import sys
sys.path.insert(0, 'benchmark')

import duckdb_query_graph

if len(sys.argv) <= 1:
	print("Usage: python generate_querygraph.py [input.json] [output.html] [open={1,0}]")
	exit(1)


input = sys.argv[1]
if len(sys.argv) <= 2:
	if ".json" in input:
		output = input.replace(".json", ".html")
	else:
		output = input + ".html"
else:
	output = sys.argv[2]

open_output = True
if len(sys.argv) >= 4:
	open_arg = sys.argv[3].lower()
	if open_arg == "1" or open_arg == "true":
		open_output = True
	elif sys.argv[3] == "0" or open_arg == "false":
		open_output = False
	else:
		print("Incorrect input for open_output, expected TRUE or FALSE")
		exit(1)


duckdb_query_graph.generate(input, output)

with open(output, 'r') as f:
	text = f.read()

#inline javascript files
javascript_base = os.path.join('tools', 'pythonpkg', 'duckdb_query_graph')
with open(os.path.join(javascript_base, 'raphael.js'), 'r') as f:
	raphael = f.read()

with open(os.path.join(javascript_base, 'treant.js'), 'r') as f:
	treant = f.read()

text = text.replace('<script src="../../raphael.js"></script>', '<script>' + raphael + '</script>')
text = text.replace('<script src="../../treant.js"></script>', '<script>' + treant + '</script>')

with open(output, 'w+') as f:
	f.write(text)

if open_output:
	os.system('open "' + output.replace('"', '\\"') + '"')


