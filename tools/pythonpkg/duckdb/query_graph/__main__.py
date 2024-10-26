import json
import os
import sys
import webbrowser
from functools import reduce
import argparse

qgraph_css = """
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
	font-size:15px;
}
.tf-nc {
	position: relative;
	width: 250px;
	text-align: center;
	background-color: #fff100;
}
"""


class NodeTiming:

    def __init__(self, phase: str, time: float) -> object:
        self.phase = phase
        self.time = time
        # percentage is determined later.
        self.percentage = 0

    def calculate_percentage(self, total_time: float) -> None:
        self.percentage = self.time / total_time

    def combine_timing(l: object, r: object) -> object:
        # TODO: can only add timings for same-phase nodes
        total_time = l.time + r.time
        return NodeTiming(l.phase, total_time)


class AllTimings:

    def __init__(self):
        self.phase_to_timings = {}

    def add_node_timing(self, node_timing: NodeTiming):
        if node_timing.phase in self.phase_to_timings:
            self.phase_to_timings[node_timing.phase].append(node_timing)
            return
        self.phase_to_timings[node_timing.phase] = [node_timing]

    def get_phase_timings(self, phase: str):
        return self.phase_to_timings[phase]

    def get_summary_phase_timings(self, phase: str):
        return reduce(NodeTiming.combine_timing, self.phase_to_timings[phase])

    def get_phases(self):
        phases = list(self.phase_to_timings.keys())
        phases.sort(key=lambda x: (self.get_summary_phase_timings(x)).time)
        phases.reverse()
        return phases

    def get_sum_of_all_timings(self):
        total_timing_sum = 0
        for phase in self.phase_to_timings.keys():
            total_timing_sum += self.get_summary_phase_timings(phase).time
        return total_timing_sum


def open_utf8(fpath: str, flags: str) -> object:
    return open(fpath, flags, encoding="utf8")


def get_child_timings(top_node: object, query_timings: object) -> str:
    node_timing = NodeTiming(top_node['operator_type'], float(top_node['operator_timing']))
    query_timings.add_node_timing(node_timing)
    for child in top_node['children']:
        get_child_timings(child, query_timings)


color_map = {
    "HASH_JOIN": "#ffffba",
    "PROJECTION": "#ffb3ba",
    "SEQ_SCAN": "#baffc9",
    "UNGROUPED_AGGREGATE": "#ffdfba",
    "FILTER": "#bae1ff",
    "ORDER_BY": "#facd60",
    "PERFECT_HASH_GROUP_BY": "#ffffba",
    "HASH_GROUP_BY": "#ffffba",
    "NESTED_LOOP_JOIN": "#ffffba",
    "STREAMING_LIMIT": "#facd60",
    "COLUMN_DATA_SCAN": "#1ac0c6",
    "TOP_N": "#ffdfba"
}


def get_node_body(name: str, result: str, cardinality: float, extra_info: str) -> str:
    node_style = ""
    stripped_name = name.strip()
    if stripped_name in color_map:
        node_style = f"background-color: {color_map[stripped_name]};"

    body = f"<span class=\"tf-nc\" style=\"{node_style}\">"
    body += "<div class=\"node-body\">"
    new_name = name.replace("_", " ")
    body += f"<p> <b>{new_name} ({result}s) </b></p>"
    body += f"<p> ---------------- </p>"
    body += f"<p> {extra_info} </p>"
    body += f"<p> ---------------- </p>"
    body += f"<p> cardinality = {cardinality} </p>"
    # TODO: Expand on timing. Usually available from a detailed profiling
    body += "</div>"
    body += "</span>"
    return body


def generate_tree_recursive(json_graph: object) -> str:
    node_prefix_html = "<li>"
    node_suffix_html = "</li>"

    extra_info = ""
    for key in json_graph['extra_info']:
        extra_info += f"{key}: {json_graph['extra_info'][key]} <br>"

    node_body = get_node_body(json_graph["operator_type"],
                              json_graph["operator_timing"],
                              json_graph["operator_cardinality"],
                              extra_info)

    children_html = ""
    if len(json_graph['children']) >= 1:
        children_html += "<ul>"
        for child in json_graph["children"]:
            children_html += generate_tree_recursive(child)
        children_html += "</ul>"
    return node_prefix_html + node_body + children_html + node_suffix_html


# For generating the table in the top left.
def generate_timing_html(graph_json: object, query_timings: object) -> object:
    json_graph = json.loads(graph_json)
    gather_timing_information(json_graph, query_timings)
    total_time = float(json_graph['operator_timing'])
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

    execution_time = query_timings.get_sum_of_all_timings()

    all_phases = query_timings.get_phases()
    query_timings.add_node_timing(NodeTiming("TOTAL TIME", total_time))
    query_timings.add_node_timing(NodeTiming("Execution Time", execution_time))
    all_phases = ["TOTAL TIME", "Execution Time"] + all_phases
    for phase in all_phases:
        summarized_phase = query_timings.get_summary_phase_timings(phase)
        summarized_phase.calculate_percentage(total_time)
        phase_column = f"<b>{phase}</b>" if phase == "TOTAL TIME" or phase == "Execution Time" else phase
        table_body += f"""
	<tr>
			<td>{phase_column}</td>
            <td>{summarized_phase.time}</td>
            <td>{round(summarized_phase.percentage*100,4)}%</td>
    </tr>
"""
    table_body += table_end
    return table_head + table_body


def generate_tree_html(graph_json: object) -> str:
    json_graph = json.loads(graph_json)
    tree_prefix = "<div class=\"tf-tree tf-gap-lg\"> \n <ul>"
    tree_suffix = "</ul> </div>"
    # first level of json is general overview
    # FIXME: make sure json output first level always has only 1 level
    tree_body = generate_tree_recursive(json_graph['children'][0])
    return tree_prefix + tree_body + tree_suffix


def generate_ipython(json_input: str) -> str:
    from IPython.core.display import HTML

    html_output = generate_html(json_input, False)

    return HTML(("\n"
                 "	${CSS}\n"
                 "	${LIBRARIES}\n"
                 "	<div class=\"chart\" id=\"query-profile\"></div>\n"
                 "	${CHART_SCRIPT}\n"
                 "	").replace("${CSS}", html_output['css']).replace('${CHART_SCRIPT}',
                                                                       html_output['chart_script']).replace(
        '${LIBRARIES}', html_output['libraries']))


def generate_style_html(graph_json: str, include_meta_info: bool) -> None:
    treeflex_css = "<link rel=\"stylesheet\" href=\"https://unpkg.com/treeflex/dist/css/treeflex.css\">\n"
    css = "<style>\n"
    css += qgraph_css + "\n"
    css += "</style>\n"
    return {
        'treeflex_css': treeflex_css,
        'duckdb_css': css,
        'libraries': '',
        'chart_script': ''
    }


def gather_timing_information(json: str, query_timings: object) -> None:
    # add up all of the times
    # measure each time as a percentage of the total time.
    # then you can return a list of [phase, time, percentage]
    get_child_timings(json['children'][0], query_timings)


def translate_json_to_html(input_file: str, output_file: str) -> None:
    query_timings = AllTimings()
    with open_utf8(input_file, 'r') as f:
        text = f.read()

    html_output = generate_style_html(text, True)
    timing_table = generate_timing_html(text, query_timings)
    tree_output = generate_tree_html(text)

    # finally create and write the html
    with open_utf8(output_file, "w+") as f:
        html = """<!DOCTYPE html>
<html>
	<head>
	<meta charset="utf-8">
	<meta name="viewport" content="width=device-width">
	<title>Query Profile Graph for Query</title>
	${TREEFLEX_CSS}
	<style>
		${DUCKDB_CSS}
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
        html = html.replace("${TREEFLEX_CSS}", html_output['treeflex_css'])
        html = html.replace("${DUCKDB_CSS}", html_output['duckdb_css'])
        html = html.replace("${TIMING_TABLE}", timing_table)
        html = html.replace('${TREE}', tree_output)
        f.write(html)


def main() -> None:
    if sys.version_info[0] < 3:
        print("Please use python3")
        exit(1)
    parser = argparse.ArgumentParser(
        prog='Query Graph Generator',
        description='Given a json profile output, generate a html file showing the query graph and timings of operators')
    parser.add_argument('profile_input', help='profile input in json')
    parser.add_argument('--out', required=False, default=False)
    parser.add_argument('--open', required=False, action='store_true', default=True)
    args = parser.parse_args()

    input = args.profile_input
    output = args.out
    if not args.out:
        if ".json" in input:
            output = input.replace(".json", ".html")
        else:
            print("please provide profile output in json")
            exit(1)
    else:
        if ".html" in args.out:
            output = args.out
        else:
            print("please provide valid .html file for output name")
            exit(1)

    open_output = args.open

    translate_json_to_html(input, output)

    if open_output:
        webbrowser.open('file://' + os.path.abspath(output), new=2)


if __name__ == '__main__':
    main()
