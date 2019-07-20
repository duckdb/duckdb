import json
import os

MAX_NODES = 1000

raphael_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'raphael.js')
treant_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'treant.js')


def generate_html(parsed_json):
	global current_tone, execution_time, total_nodes
	# tones for the different nodes, we use "earth tones"
	tones = [
		"#493829",
		"#816C5B",
		"#A9A18C",
		"#613318",
		"#B99C6B",
		"#A15038",
		"#8F3B1B",
		"#D57500",
		"#DBCA69",
		"#404F24",
		"#668D3C",
		"#BDD09F",
		"#4E6172",
		"#83929F",
		"#A3ADB8"
	]
	# set fixed tones for the various common operators
	fixed_tone_map = {
		"ORDER_BY": 8,
		"HASH_GROUP_BY": 7,
		"FILTER": 13,
		"SEQ_SCAN": 4,
		"HASH_JOIN": 10,
		"PROJECTION": 5,
		"LIMIT": 0,
		"PIECEWISE_MERGE_JOIN": 1,
		"DISTINCT": 11,
		"DELIM_SCAN": 9
	}
	# remaining (unknown) operators just fetch tones in order
	remaining_tones = []
	for i in range(len(tones)):
		if i not in fixed_tone_map.values():
			remaining_tones.append(i)

	current_tone = 0
	# first parse the total time
	total_time = float(parsed_json["result"])

	# assign an impact factor to nodes
	# this is used for rendering opacity
	execution_time = 0
	node_timings = {}
	node_list = []
	def gather_nodes(node):
		global execution_time
		node_timing = float(node["timing"])
		if node['name'] not in node_timings:
			node_timings[node['name']] = 0
		node_timings[node['name']] += node_timing
		execution_time += node_timing
		node_list.append((node_timing, node))
		children = node["children"]
		for i in range(len(children)):
			gather_nodes(children[i])

	def timing_to_str(timing):
		timing_str = "%.3f" % (timing,)
		if timing_str == "0.000":
			return "0.0"
		# if "e-" in timing_str:
		# 	timing_str = "0.0"
		return timing_str

	def to_percentage(total_time, time):
		fraction = 100 * (time / total_time)
		if fraction < 0.1:
			return "0%"
		else:
			return "%.1f" % (fraction,) + "%"


	# first gather all nodes
	gather_nodes(parsed_json["tree"])

	# get the timing of the different query phases
	meta_info = ""
	if "timings" in parsed_json:
		keys = []
		timings = {}
		for entry in parsed_json["timings"]:
			timings[entry] = parsed_json["timings"][entry]
			keys.append(entry)
		keys.sort()
		meta_info = """<table class="meta-info">
<tr class="phaseheader metainfo">
<th style="font-weight:bold;">Phase</th>
<th style="font-weight:bold;">Time</th>
<th style="font-weight:bold;">Percentage</th>
</tr>
"""
		# add the total time
		meta_info += """<tr class="metainfo mainphase"><th style="font-weight:bold;">Total Time</th><th style="font-weight:bold;">${TOTAL_TIME}s</th><th></th></tr>""".replace("${TOTAL_TIME}", timing_to_str(total_time))
		# add the execution phase
		meta_info += """<tr class="metainfo mainphase"><th>Execution</th><th>${EXECUTION_TIME}s</th><th>${PERCENTAGE}</th></tr>""".replace("${EXECUTION_TIME}", str(execution_time)).replace("${PERCENTAGE}", to_percentage(total_time, execution_time))
		execution_nodes = node_timings.keys()
		execution_nodes = sorted(execution_nodes, key=lambda x: -node_timings[x])
		for node in execution_nodes:
			meta_info += '<tr class="metainfo subphase"><th>${NODE}</th><th>${NODE_TIME}s</th><th>${PERCENTAGE}</th></tr>'.replace("${NODE}", node).replace("${NODE_TIME}", timing_to_str(node_timings[node])).replace("${PERCENTAGE}", to_percentage(total_time, node_timings[node]))


		# add the planning/optimizer phase to the table
		for key in keys:
			row_class = 'subphase' if ">" in key else 'mainphase'
			meta_info += '<tr class="metainfo %s">' % (row_class)
			meta_info += '<th>'
			if ">" in key:
				# subphase
				meta_info += "> " + key.split(" > ")[-1].title().replace("_", " ")
			else:
				# primary phase
				meta_info += key.title().replace("_", " ")
			meta_info += "</th>"
			meta_info += '<th>' + timing_to_str(timings[key]) + "s" + "</th>"
			meta_info += "<th>" + to_percentage(total_time, timings[key]) + "</th>"
			meta_info += "</tr>"
		meta_info += '</table>'

	# assign impacts based on % of execution spend in the node
	# <0.1% = 0
	# <1% = 1
	# <5% = 2
	# anything else: 3
	for i in range(len(node_list)):
		percentage = 100 * (node_list[i][0] / total_time)
		if percentage < 0.1:
			node_list[i][1]["impact"] = 0
		elif percentage < 1:
			node_list[i][1]["impact"] = 1
		elif percentage < 5:
			node_list[i][1]["impact"] = 2
		else:
			node_list[i][1]["impact"] = 3


	# now convert the JSON to the format used by treant.js
	def add_spaces_to_big_number(number):
		if len(number) > 4:
			return add_spaces_to_big_number(number[:-3]) + " " + number[-3:]
		else:
			return number

	total_nodes = 0
	def convert_json(node):
		global total_nodes
		total_nodes += 1
		# format is { text { name, title, contact }, HTMLclass, children: [...] }
		global current_tone
		result = '{ "text": {'
		result += '"name": "' + node["name"] + ' (' + str(node["timing"]) + 's)",\n'
		title = ""
		splits = node["extra_info"].split('\n')
		for i in range(len(splits)):
			if len(splits[i]) > 0:
				title += splits[i].replace('"', '') + "<br>"
		result += '"title": "' + title + '",\n'
		result += '"contact": "[' + add_spaces_to_big_number(str(node["cardinality"])) + ']"},\n'
		if node["name"] not in fixed_tone_map:
			fixed_tone_map[node["name"]] = remaining_tones[current_tone]
			current_tone += 1
			if current_tone >= len(remaining_tones):
				current_tone = 0
		if node["impact"] == 0:
			impact_class = "small-impact"
		elif node["impact"] == 1:
			impact_class = "medium-impact"
		elif node["impact"] == 2:
			impact_class = "high-impact"
		else:
			impact_class = "most-impact"
		result += '"HTMLclass": "' + impact_class + ' tone-' + str(fixed_tone_map[node["name"]]) + '",\n'
		result += '"children": ['
		children = node["children"]
		for i in range(len(children)):
			result += convert_json(children[i])
			if total_nodes > MAX_NODES:
				break
			if  i + 1 < len(children):
				result += ","
		result += "]}\n"
		return result

	new_json = convert_json(parsed_json["tree"])

	# create the chart info object that is used to create the final tree
	chart_info = """
	var chart_config = {
		chart: {
			container: "#query-profile",
			nodeAlign: "BOTTOM",
			connectors: {
				type: 'step'
			},
			node: {
				HTMLclass: 'tree-node'
			}
		},
		nodeStructure: ${JSON_STRUCTURE}
	}
	""".replace("${JSON_STRUCTURE}", new_json)

	# generate tone CSS from the tones we set above
	tone_text = ""
	for i in range(len(tones)):
		tone_text += """
		.tone-${INDEX} {
			background-color: ${COLOR};
		}""".replace("${INDEX}", str(i)).replace("${COLOR}", tones[i])

	css = """<style>
	.Treant { position: relative; overflow: hidden; padding: 0 !important; }
	.Treant > .node,
	.Treant > .pseudo { position: absolute; display: block; visibility: hidden; }
	.Treant.loaded .node,
	.Treant.loaded .pseudo { visibility: visible; }
	.Treant > .pseudo { width: 0; height: 0; border: none; padding: 0; }
	.Treant .collapse-switch { width: 3px; height: 3px; display: block; border: 1px solid black; position: absolute; top: 1px; right: 1px; cursor: pointer; }
	.Treant .collapsed .collapse-switch { background-color: #868DEE; }
	.Treant > .node img {	border: none; float: left; }

	body,div,dl,dt,dd,ul,ol,li,h1,h2,h3,h4,h5,h6,pre,form,fieldset,input,textarea,p,blockquote,th,td { margin:0; padding:0; }
	table { border-collapse:collapse; border-spacing:0; }
	fieldset,img { border:0; }
	address,caption,cite,code,dfn,em,strong,th,var { font-style:normal; font-weight:normal; }
	caption,th { text-align:left; }
	h1,h2,h3,h4,h5,h6 { font-size:100%; font-weight:normal; }
	q:before,q:after { content:''; }
	abbr,acronym { border:0; }

	body { background: #fff; }
	/* optional Container STYLES */
	.chart { height: 500px; margin: 5px; width: 100%;  }
	.Treant > .node {  }
	.Treant > p { font-family: "HelveticaNeue-Light", "Helvetica Neue Light", "Helvetica Neue", Helvetica, Arial, "Lucida Grande", sans-serif; font-weight: bold; font-size: 12px; }

	.tree-node {
		padding: 2px;
		-webkit-border-radius: 3px;
		-moz-border-radius: 3px;
		border-radius: 3px;
		background-color: #ffffff;
		border: 1px solid #000;
		width: 200px;
		font-family: Tahoma;
		font-size: 12px;
	}

	.metainfo {
		font-family: Tahoma;
		font-weight: bold;
	}

	.metainfo > th {
		padding: 1px 5px;
	}

	.mainphase {
		background-color: #668D3C;
	}
	.subphase {
		background-color: #B99C6B;
	}
	.phaseheader {
		background-color: #83929F;
	}

	.node-name {
		font-weight: bold;
		text-align: center;
		font-size: 14px;
	}

	.node-title {
		text-align: center;
	}

	.node-contact {
		font-weight: bold;
		text-align: center;
	}

	.small-impact {
		opacity: 0.7;
	}

	.medium-impact {
		opacity: 0.8;
	}

	.high-impact {
		opacity: 0.9;
	}

	.most-impact {
		opacity: 1;
	}


	${TONES}
		</style>""".replace("${TONES}", tone_text)
	chart_script = """
		<script>
		${CHART_INFO}

			// create a dummy chart to figure out how wide/high it is
			new Treant(chart_config );
			max_height = 0
			document.querySelectorAll('.tree-node').forEach(function(node) {
				top_px = parseInt(node.style.top.substring(0, node.style.top.length - 2))
				if (top_px > max_height) {
					max_height = top_px
				}
			});
			// now remove the chart we just created
			document.querySelectorAll('#query-profile').forEach(function(node) {
				node.innerHTML = ""
			});
			// resize the chart
			document.querySelectorAll('.chart').forEach(function(node) {
				node.style.height = (max_height + 200) + "px"
			});
			// and recreate it
			new Treant(chart_config );


		</script>""".replace("${CHART_INFO}", chart_info)
	libraries = """
	<script>
	${RAPHAEL}
	</script>
	<script>
	${TREANT}
	</script>
	""".replace("${RAPHAEL}", open(raphael_path).read()).replace("${TREANT}", open(treant_path).read())
	return {
		'css': css,
		'meta_info': meta_info,
		'chart_script': chart_script,
		'libraries': libraries
	}

def generate_ipython(json_input):
	from IPython.core.display import HTML

	parsed_json = json.loads(json_input)

	html_output = generate_html(parsed_json)

	return HTML("""
	${CSS}
	${LIBRARIES}
	<div class="chart" id="query-profile"></div>
	${CHART_SCRIPT}
	""".replace("${CSS}", html_output['css']).replace('${CHART_SCRIPT}', html_output['chart_script']).replace('${LIBRARIES}', html_output['libraries']))


def generate(input_file, output_file):
	with open(input_file) as f:
		text = f.read()
		# we only render the first tree, extract it
		text = '{ "result"' + text.split('{ "result"')[1]

	parsed_json = json.loads(text)

	html_output = generate_html(parsed_json)

	# finally create and write the html
	with open(output_file, "w+") as f:
		f.write("""
	<!DOCTYPE html>
	<html>
		<head>
		<meta charset="utf-8">
		<meta name="viewport" content="width=device-width">
		<title>Query Profile Graph for Query</title>
		${CSS}
	</head>
	<body>
		${LIBRARIES}

		<div id="meta-info">${META_INFO}</div>
		<div class="chart" id="query-profile"></div>

		${CHART_SCRIPT}
	</body>
	</html>
		""".replace("${CSS}", html_output['css']).replace("${META_INFO}", html_output['meta_info']).replace('${CHART_SCRIPT}', html_output['chart_script']).replace('${LIBRARIES}', html_output['libraries']))
