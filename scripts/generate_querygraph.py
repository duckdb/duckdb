# generate_querygraph.py
# This script takes a json file as input that is the result of the QueryProfiler of duckdb
# and converts it into a Query Graph.

import os
from functools import reduce

class NodeTiming:

	def __init__(self, phase, time):
		self.phase = phase
		self.time = time
		# percentage is determined later.
		self.percentage = 0

	def calculate_percentage(self, total_time):
		self.percentage = self.time/total_time

	def combine_timing(l, r):
		# TODO: can only add timings for same-phase nodes
		total_time = l.time + r.time
		return NodeTiming(l.phase, total_time)

class AllTimings:

	def __init__(self):
		self.phase_to_timings = {}

	def add_node_timing(self, node_timing):
		if node_timing.phase in self.phase_to_timings:
			self.phase_to_timings[node_timing.phase].append(node_timing)
			return
		self.phase_to_timings[node_timing.phase] = [node_timing]

	def get_phase_timings(self, phase):
		return self.phase_to_timings[phase]

	def get_summary_phase_timings(self, phase):
		return reduce(NodeTiming.combine_timing, self.phase_to_timings[phase])

	def get_phases_high_to_low(self):
		phases = list(self.phase_to_timings.keys())
		phases.sort(key=lambda x: (self.get_summary_phase_timings(x)).time)
		phases.reverse()
		return phases

	def get_sum_of_all_timings(self):
		total_timing_sum = 0
		for phase in self.phase_to_timings.keys():
			total_timing_sum += self.get_summary_phase_timings(phase).time
		return total_timing_sum

QUERY_TIMINGS = AllTimings()

sys.path.insert(0, 'benchmark')

import duckdb_query_graph

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
	# if extra_info:
	# 	body += f"<p> {extra_info} </p>"
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

# if detailed profiling is enabled, then information on how long the optimizer/physical planner etc. is available
# that is "top level timing information"
def get_top_level_timings(json):
	return []

def get_child_timings(top_node):
	global QUERY_TIMINGS
	node_timing = NodeTiming(top_node['name'], float(top_node['timing']))
	QUERY_TIMINGS.add_node_timing(node_timing)
	for child in top_node['children']:
		get_child_timings(child)

def gather_timing_information(json):
	# add up all of the times
	# measure each time as a percentage of the total time.
	# then you can return a list of [phase, time, percentage]
	top_level_timings = get_top_level_timings(json)
	child_timings = get_child_timings(json['children'][0])

# For generating the table in the top left.
def generate_timing_html(graph_json):
	global QUERY_TIMINGS
	json_graph = json.loads(graph_json)
	gather_timing_information(json_graph)
	total_time = float(json_graph['timing'])
	table_head = """
	<table class=\"styled-table\"> 
		<thead>
			<tr>
				<th>Phase</th>
				<th>Time</th>
				<th>Percentage</th>
			</tr>
		</thead>"""

	table_body = "<tbody>"
	table_end = "</tbody></table>"
# 	total_time_row = f"""
# 	<tr>
# 			<td><b>TOTAL TIME</b></td>
#             <td><b>{total_time}</b></td>
#             <td></td>
#     </tr>
# """
	execution_time = QUERY_TIMINGS.get_sum_of_all_timings()
# 	total_node_time_row = f"""
# 	<tr>
# 			<td><b>Execution</b></td>
#             <td><b>{}</b></td>
#             <td></td>
#     </tr>
# """

	all_phases = QUERY_TIMINGS.get_phases_high_to_low()
	all_phases = [NodeTiming("TOTAL TIME", total_time), NodeTiming("Execution Time", execution_time)] + all_phases
	for phase in all_phases:
		summarized_phase = QUERY_TIMINGS.get_summary_phase_timings(phase)
		summarized_phase.calculate_percentage(total_time)
		table_body += f"""
	<tr>
			<td>{phase}</td>
            <td>{summarized_phase.time}</td>
            <td>{str(summarized_phase.percentage * 100)[:6]}%</td>
    </tr>
"""
	table_body += table_end
	return table_head + table_body


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
	timing_table = generate_timing_html(text)
	tree_output = generate_tree_html(text)

	# finally create and write the html
	with open_utf8(output_file, "w+") as f:
		html = """<!DOCTYPE html>
<html>
	<head>
	<meta charset="utf-8">
	<meta name="viewport" content="width=device-width">
	<title>Query Profile Graph for Query</title>
	${CSS}
	<style>
		.styled-table {
			border-collapse: collapse;
			margin: 25px 0;
			font-size: 0.9em;
			font-family: sans-serif;
			min-width: 400px;
			box-shadow: 0 0 20px rgba(0, 0, 0, 0.15);
		}
		.styled-table thead tr {
			background-color: #009879;
			color: #ffffff;
			text-align: left;
		}
		.styled-table th,
		.styled-table td {
			padding: 12px 15px;
		}
		.styled-table tbody tr {
			border-bottom: 1px solid #dddddd;
		}
		
		.styled-table tbody tr:nth-of-type(even) {
			background-color: #f3f3f3;
		}
		
		.styled-table tbody tr:last-of-type {
			border-bottom: 2px solid #009879;
		}

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
	<div class="chart" id="query-profile">
		${TIMING_TABLE}
	</div>

	${TREE}
</body>
</html>
"""
		html = html.replace("${CSS}", html_output['css'])
		html = html.replace("${TIMING_TABLE}", timing_table)
		html = html.replace('[INFOSEPARATOR]', '<br>')
		html = html.replace('${TREE}', tree_output)
		f.write(html)

generate(input, output)

with open(output, 'r') as f:
	text = f.read()

if open_output:
	os.system('open "' + output.replace('"', '\\"') + '"')
