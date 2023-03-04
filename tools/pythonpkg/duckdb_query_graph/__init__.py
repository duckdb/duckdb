import json
import os

qgraph_css = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'query_graph.css')
raphael_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'raphael.js')
treant_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'treant.js')
profile_output_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'parse_profiling_output.js')

def open_utf8(fpath, flags):
    import sys
    if sys.version_info[0] < 3:
        return open(fpath, flags)
    else:
        return open(fpath, flags, encoding="utf8")


def generate_html(graph_json, include_meta_info):
	libraries = "<script>\n"
	with open(raphael_path, 'r') as f:
		libraries += f.read() + "\n"
	with open(treant_path, 'r') as f:
		libraries += f.read() + "\n"
	with open(profile_output_path, 'r') as f:
		libraries += f.read() + "\n"
	libraries += "</script>"

	css = "<style>\n"
	with open(qgraph_css, 'r') as f:
		css += f.read() + "\n"
	css += "</style>"

	graph_json = graph_json.replace('\n', ' ').replace("'", "\\'").replace('"', '\\"').replace("\\n", "\\\\n")

	chart_script = f"""<script>
var graph_json = '{graph_json}';
var result = parse_profiling_output(graph_json);
var meta_info = result[0];
var graph_data = result[1];
{"" if not include_meta_info else "document.getElementById('meta-info').innerHTML = meta_info;"}
if (graph_data !== null && graph_data !== undefined) {
	create_graph(graph_data, '#query-profile', '.chart');
}
</script>
	"""
	return {
		'css': css,
		'libraries': libraries,
		'chart_script': chart_script
	}

def generate_ipython(json_input):
	from IPython.core.display import HTML

	html_output = generate_html(json_input, False)
	
	return HTML(f"""
	HTML(f"""
	{html_output['css']}
	{html_output['libraries']}
	<div class="chart" id="query-profile"></div>
	{html_output['chart_script']}
	""")

def generate(input_file, output_file):
	with open_utf8(input_file, 'r') as f:
		text = f.read()

	html_output = generate_html(text, True)
	print(html_output['chart_script'])
	# finally create and write the html
	with open_utf8(output_file, "w+") as f:
		f.write(f"""<!DOCTYPE html>
<html>
	<head>
	<meta charset="utf-8">
	<meta name="viewport" content="width=device-width">
	<title>Query Profile Graph for Query</title>
	{html_output['css']}
</head>
<body>
	{html_output['libraries'])}

	<div id="meta-info"></div>
	<div class="chart" id="query-profile"></div>

	{html_output['chart_script']}
</body>
</html>
"""
