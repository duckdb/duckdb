import json

def generate(input_file, output_file):
	with open(input_file) as f:
		text = f.read()
		# we only render the first tree, extract it
		text = '{ "result"' + text.split('{ "result"')[1]

	parsed_json = json.loads(text)

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
	}
	# remaining (unknown) operators just fetch tones in order
	remaining_tones = []
	for i in range(len(tones)):
		if i not in fixed_tone_map.values():
			remaining_tones.append(i)

	current_tone = 0
	# first parse the total time
	try:
		total_time = float(parsed_json["result"])
	except:
		exit(1)
	print(total_time)

	# assign an impact factor to nodes
	# this is used for rendering opacity
	node_list = []
	def gather_nodes(node):
		node_list.append((float(node["timing"]), node))
		children = node["children"]
		for i in range(len(children)):
			gather_nodes(children[i])


	# first gather all nodes
	gather_nodes(parsed_json["tree"])
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

	def convert_json(node):
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


	# finally create and write the html
	with open(output_file, "w+") as f:
		f.write("""
	<!DOCTYPE html>
	<html>
		<head>
		<meta charset="utf-8">
		<meta name="viewport" content="width=device-width">
		<title>Query Profile Graph for Query</title>
		<style>
		
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
		</style>
	</head>
	<body>
		<script src="../../raphael.js"></script>
		<script src="../../treant.js"></script>
		<script>
		${CHART_SCRIPT}
		</script>

		<div class="chart" id="query-profile"></div>

		<script>
			// create a dummy chart to figure out how wide/high iti s
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
			

		</script>

	</body>
	</html>
		""".replace("${TONES}", tone_text).replace("${CHART_SCRIPT}", chart_info))
