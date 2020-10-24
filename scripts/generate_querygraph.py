# generate_querygraph.py
# This script takes a json file as input that is the result of the QueryProfiler of duckdb
# and converts it into a Query Graph.

import os
import sys
sys.path.insert(0, 'benchmark')

import duckdb_query_graph

arguments = sys.argv
if len(arguments) <= 1:
	print("Usage: python generate_querygraph.py [input.json] [output.html] [open={1,0}] [--tree=0]")
	exit(1)

tree_index = None
for i in range(0, len(arguments)):
	if arguments[i].startswith('--tree'):
		tree_index = int(arguments[i].split('--tree=')[1])
		del arguments[i]

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
	open_arg = arguments[3].lower()
	if open_arg == "1" or open_arg == "true":
		open_output = True
	elif arguments[3] == "0" or open_arg == "false":
		open_output = False
	else:
		print("Incorrect input for open_output, expected TRUE or FALSE")
		exit(1)

if tree_index != None:
	with open(input, 'r') as f:
		text = f.read()
	new_text = '{ "result"' + text.split('{ "result"')[tree_index + 1]
	input += '.tmp'
	with open(input, 'w+') as f:
		f.write(new_text)




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

with open(output, 'w+', encoding="utf8") as f:
	f.write(text)

if open_output:
	os.system('open "' + output.replace('"', '\\"') + '"')


