# generate_querygraph.py
# This script takes a json file as input that is the result of the QueryProfiler of duckdb
# and converts it into a Query Graph.

import sys
import json
import os

sys.path.insert(0, 'benchmark')

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

def open_utf8(fpath, flags):
	import sys
	if sys.version_info[0] < 3:
		return open(fpath, flags)
	else:
		return open(fpath, flags, encoding="utf8")

def get_node_body(name, result, cardinality, timing, extra_info):
	body = "<span class=\"tf-nc\">"
	body += "<div class=\"node-body\">"
	body += f"<p> {name} </p>"
	body += f"<p> {result}s </p>"
	if timing:
		body += f"<p> {timing} </p>"
	body += f"<p> cardinality = {cardinality} </p>"
	if extra_info:
		body += f"<p> {extra_info} </p>"
	body += "</div>"
	body += "</span>"
	return body


def generate_tree_recursive(json_graph):
	node_prefix_html = "<li>"
	node_suffix_html = "</li>"
	node_body = get_node_body(json_graph["name"],
							  json_graph["timing"],
							  json_graph["cardinality"],
							  json_graph["extra_info"].replace("\n", "<br>"),
							  json_graph["timings"])

	children_html = ""
	if len(json_graph['children']) >= 1:
		children_html += "<ul>"
		for child in json_graph["children"]:
			children_html += generate_tree_recursive(child)
		children_html += "</ul>"
	return node_prefix_html + node_body + children_html + node_suffix_html



def generate_tree_html(graph_json):
	json_graph = json.loads(graph_json)
	tree_prefix = "<div class=\"tf-tree tf-gap-lg\"> \n <ul>"
	tree_suffix = "</ul> </div>"
	# first level of json is general overview
	# FIXME: make sure json output first level always has only 1 level
	tree_body = generate_tree_recursive(json_graph['children'][0])
	return tree_prefix + tree_body + tree_suffix


def generate_style_html(graph_json, include_meta_info):
	css = "<link rel=\"stylesheet\" href=\"https://unpkg.com/treeflex/dist/css/treeflex.css\">\n"
	return {
		'css': css,
		'libraries': '',
		'chart_script': ''
	}


def generate_ipython(json_input):
	from IPython.core.display import HTML

	html_output = generate_html(json_input, False)

	return HTML("""
	${CSS}
	${LIBRARIES}
	<div class="chart" id="query-profile"></div>
	${CHART_SCRIPT}
	""".replace("${CSS}", html_output['css']).replace('${CHART_SCRIPT}', html_output['chart_script']).replace('${LIBRARIES}', html_output['libraries']))


def generate(input_file, output_file):
	with open_utf8(input_file, 'r') as f:
		text = f.read()

	html_output = generate_style_html(text, True)
	tree_output = generate_tree_html(text)
	# print(html_output['chart_script'])
	# finally create and write the html
	with open_utf8(output_file, "w+") as f:
		f.write("""<!DOCTYPE html>
<html>
	<head>
	<meta charset="utf-8">
	<meta name="viewport" content="width=device-width">
	<title>Query Profile Graph for Query</title>
	${CSS}
	<style>
		.node-body {
			font-size:12px;
		}
		.tf-nc {
			position: relative;
			width: 250px;
			text-align: center;
			background-color: #fff100;
		}
	</style>
</head>
<body>
	<div id="meta-info"></div>
	<div class="chart" id="query-profile"></div>

	${TREE}
</body>
</html>
""".replace("${CSS}", html_output['css']).replace('${TREE}', tree_output).replace('[INFOSEPARATOR]', '<br>'))


generate(input, output)

with open(output, 'r') as f:
	text = f.read()

if open_output:
	os.system('open "' + output.replace('"', '\\"') + '"')
