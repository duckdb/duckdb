

function gather_nodes(node, node_list, node_timings) {
	var node_timing = node["timing"];
	var node_name = node["name"];
	var node_children = node["children"];
	if (node_timings[node_name] == undefined) {
		node_timings[node_name] = 0
	}
	node_timings[node_name] += node_timing;
	node_list.push([node_timing, node]);
	var total_time = 0;
	for(var child in node_children) {
		total_time += gather_nodes(node_children[child], node_list, node_timings);
	}
	return total_time + node_timing;
}

function timing_to_str(timing) {
	var timing_str = timing.toFixed(3);
	if (timing_str == "0.000") {
		return "0.0";
	}
	return timing_str;
}

function to_percentage(total_time, time) {
	if (total_time == 0) {
		return "?%";
	}
	var fraction = 100 * (time / total_time);
	if (fraction < 0.1) {
		return "0%";
	} else {
		return fraction.toFixed(1) + "%";
	}
}

function add_spaces_to_big_number(number) {
	if (number.length > 4) {
		return add_spaces_to_big_number(number.substr(0, number.length - 3)) + " " + number.substr(number.length - 3, number.length)
	} else {
		return number;
	}
}

function toTitleCase(str) {
	return str.replace(
	  /\w\S*/g,
	  function(txt) {
		return txt.charAt(0).toUpperCase() + txt.substr(1).toLowerCase();
	  }
	);
}

function parse_profiling_output(graph_json) {
	if (graph_json == null || graph_json == undefined) {
		return ["No profile input.", null];
	}
	var n = graph_json.indexOf('"result"');
	if (n < 0) {
		// not a result we can do anything with
		return [graph_json, null];
	}
	// find the second occurrence of "result", if any
	var second_n = graph_json.indexOf('"result"', n + 1)
	if (second_n >= 0) {
		// found one: find the last { before "result"
		var split_index = graph_json.lastIndexOf("{", second_n);
		graph_json = graph_json.substring(0, split_index);
	}
	// now parse the json
	graph_data = JSON.parse(graph_json);
	// tones for the different nodes, we use "earth tones"
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
	];
	// set fixed tones for the various common operators
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
	};
	// remaining (unknown) operators just fetch tones in order
	var used_tones = {}
	var remaining_tones = []
	for(var entry in fixed_tone_map) {
		used_tones[fixed_tone_map[entry]] = true;
	}
	for(var tone in tones) {
		if (used_tones[tone] !== undefined) {
			remaining_tones.push(tone);
		}
	}
	// first parse the total time
	var total_time = graph_data["result"].toFixed(2);

	// assign an impact factor to nodes
	// this is used for rendering opacity
	var root_node = graph_data;
	var node_list = []
	var node_timings = {}
	var execution_time = gather_nodes(root_node, node_list, node_timings);

	// get the timing of the different query phases
	var meta_info = "";
	var meta_timings = graph_data["timings"];
	if (meta_timings !== undefined) {
		var keys = [];
		var timings = {};
		for(var entry in meta_timings) {
			timings[entry] = meta_timings[entry];
			keys.push(entry);
		}
		keys.sort();
		meta_info += `<table class="meta-info">
<tr class="phaseheader metainfo">
<th style="font-weight:bold;">Phase</th>
<th style="font-weight:bold;">Time</th>
<th style="font-weight:bold;">Percentage</th>
</tr>`;
		//  add the total time
		meta_info += `<tr class="metainfo mainphase"><th style="font-weight:bold;">Total Time</th><th style="font-weight:bold;">${total_time}s</th><th></th></tr>`
		// add the execution phase
		var execution_time_percentage = to_percentage(total_time, execution_time);
		meta_info += `<tr class="metainfo mainphase"><th>Execution</th><th>${execution_time}s</th><th>${execution_time_percentage}</th></tr>`
		execution_nodes = [];
		for(var execution_node in node_timings) {
			execution_nodes.push(execution_node);
		}
		execution_nodes.sort(function(a, b) {
			var a_sort = node_timings[a];
			var b_sort = node_timings[b];
			if (a_sort < b_sort) {
				return 1;
			} else if (a_sort > b_sort) {
				return -1;
			}
			return 0;
		});
		for(var node_index in execution_nodes) {
			var node = execution_nodes[node_index];
			var node_name = node;
			var node_time = timing_to_str(node_timings[node]);
			var node_percentage = to_percentage(total_time, node_timings[node]);
			meta_info += `<tr class="metainfo subphase"><th>${node_name}</th><th>${node_time}s</th><th>${node_percentage}</th></tr>`
		}
		// add the planning/optimizer phase to the table
		for(var key_index in keys) {
			var key = keys[key_index];
			var is_subphase = key.includes(">");
			row_class = is_subphase ? 'subphase' : 'mainphase'
			meta_info += `<tr class="metainfo ${row_class}">`
			meta_info += '<th>'
			if (is_subphase) {
				// subphase
				var splits = key.split(" > ");
				meta_info += "> " + toTitleCase(splits[splits.length - 1]).replace("_", " ")
			} else {
				// primary phase
				meta_info += toTitleCase(key).replace("_", " ")
			}
			meta_info += "</th>"
			meta_info += '<th>' + timing_to_str(timings[key]) + "s" + "</th>"
			meta_info += "<th>" + to_percentage(total_time, timings[key]) + "</th>"
			meta_info += "</tr>"
		}
		meta_info += '</table>'
	}

	// assign impacts based on % of execution spend in the node
	// <0.1% = 0
	// <1% = 1
	// <5% = 2
	// anything else: 3
	for(var i = 0; i < node_list.length; i++) {
		percentage = 100 * (node_list[i][0] / total_time)
		if (percentage < 0.1) {
			node_list[i][1]["impact"] = 0
		} else if (percentage < 1) {
			node_list[i][1]["impact"] = 1
		} else if (percentage < 5) {
			node_list[i][1]["impact"] = 2
		} else {
			node_list[i][1]["impact"] = 3
		}
	}
	// now convert the JSON to the format used by treant.js
	var parameters = {
		"total_nodes": 0,
		"current_tone": 0,
		"max_nodes": 1000
	};

	function convert_json(node, parameters) {
		var node_name = node["name"];
		var node_timing = node["timing"];
		var node_extra_info = node["extra_info"] || '';
		var node_impact = node["impact"];
		var node_children = node["children"];
		parameters["total_nodes"] += 1
		// format is { text { name, title, contact }, HTMLclass, children: [...] }
		var result = {}
		var text_node = {}
		title = ""
		splits = node_extra_info.split('\n')
		for(var i = 0; i < splits.length; i++) {
			if (splits[i].length > 0) {
				var text = splits[i].replace('"', '');
				if (text == "[INFOSEPARATOR]") {
					text = "- - - - - -";
				}
				title += text + "<br>"
			}
		}
		text_node["name"] = node_name + ' (' + node_timing + 's)';
		text_node["title"] = title;
		text_node["contact"] = add_spaces_to_big_number(node["cardinality"]);
		result["text"] = text_node;
		if (fixed_tone_map[node_name] == undefined) {
			fixed_tone_map[node_name] = remaining_tones[parameters["current_tone"]]
			parameters["current_tone"] += 1
			if (parameters["current_tone"] >= remaining_tones.length) {
				parameters["current_tone"] = 0
			}
		}
		if (node_impact == 0) {
			impact_class = "small-impact"
		} else if (node_impact == 1) {
			impact_class = "medium-impact"
		} else if (node_impact == 2) {
			impact_class = "high-impact"
		} else {
			impact_class = "most-impact"
		}
		result["HTMLclass"] = 'impact_class ' + ' tone-' + fixed_tone_map[node_name];
		var result_children = []
		for(var i = 0; i < node_children.length; i++) {
			result_children.push(convert_json(node_children[i], parameters))
			if (parameters["total_nodes"] > parameters["max_nodes"]) {
				break;
			}
		}
		result["children"] = result_children;
		return result
	}

	var new_json = convert_json(graph_data, parameters);
	return [meta_info, new_json];
}

function create_graph(graph_data, container_id, container_class) {
	var chart_config = {
		chart: {
			container: container_id,
			nodeAlign: "BOTTOM",
			connectors: {
				type: 'step'
			},
			node: {
				HTMLclass: 'tree-node'
			}
		},
		nodeStructure: graph_data
	};
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
	document.querySelectorAll(container_id).forEach(function(node) {
		node.innerHTML = ""
	});
	// resize the chart
	document.querySelectorAll(container_class).forEach(function(node) {
		node.style.height = (max_height + 200) + "px"
	});
	// and recreate it
	new Treant(chart_config );
};
