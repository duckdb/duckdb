
import numpy, os, sys
import generate_query_graph

out_html = 'index.html'

header = """<html>
<head>
<meta charset="UTF-8">
<title>DuckDB Continuous Benchmarking</title>
<link rel="stylesheet" type="text/css" href="bootstrap.css">
</head>
<body>
<table class="table">
"""
footer = """
</table>
</body>
</html>
"""

def get_benchmark_results(results_folder):
	results = [x for x in os.listdir(results_folder) if x != 'info']
	results = sorted(results)
	results.reverse()
	return results

def get_benchmarks(results_folder):
	info_path = os.path.join(results_folder, 'info')
	# get the group and info
	benchmarks = []
	groups = []
	benchmarks_per_group = {}
	for info_file in os.listdir(info_path):
		with open(os.path.join(info_path, info_file), 'r') as f:
			header = f.readline()
			splits = header.split(' - ')
			name = splits[0].strip()
			group = splits[1].strip()
			description = f.read()
			if group not in benchmarks_per_group:
				benchmarks_per_group[group] = []
				groups.append(group)
			benchmarks_per_group[group].append(len(benchmarks))
			benchmarks.append([name, description, info_file])
	return (benchmarks, groups, benchmarks_per_group)

def begin_row(f):
	f.write("<tr>")

def end_row(f):
	f.write("</tr>")

def begin_header(f):
	f.write('<th>')

def end_header(f):
	f.write("</th>")

def begin_rotated_header(f):
	f.write('<th class="table-active"><div><div>')

def end_rotated_header(f):
	f.write("</div></div></th>")

def begin_value(f, cl = None):
	if cl:
		f.write('<td class="%s">' % cl)
	else:
		f.write("<td>")

def end_value(f):
	f.write("</td>")

def color_output(output, r, g, b):
	return '<span style="color:rgb(%d,%d,%d);">%s</span>' % (r, g, b, output)

def bold_output(output):
	return "<b>%s</b>" % (output,)

def background_color_output(output, r, g, b):
	return '<div style="background-color:rgb(%d,%d,%d);">%s</div>' % (r, g, b, output)

def write_commit(f, commit):
	f.write('<a href="https://github.com/cwida/duckdb/commit/%s">%s</a>'  % (commit, commit[:4]))

def read_results(file):
	with open(file, 'r') as f:
		lines = f.read().split('\n')
		if len(lines) == 0:
			return ('????', 'Unknown')
		# try to parse everything as numbers
		try:
			numbers = [float(x) for x in lines if len(x) > 0]
			if len(numbers) == 0:
				return ('????', 'Unknown')
			return ("%.2f" % numpy.mean(numbers), 'Result')
		except:
			# failure to parse
			if lines[0] == 'TIMEOUT':
				return ('T', 'Timeout')
			elif lines[0] == 'CRASH':
				return ('C', 'Crash')
			elif lines[0] == 'INCORRECT':
				return ('!', 'Incorrect')
			else:
				return ('????', 'Unknown')


def create_html(results_folder):
	# get a list of benchmarks, from most recent to least recent
	results = get_benchmark_results(results_folder)[:50]
	(benchmarks, groups, benchmarks_per_group) = get_benchmarks(results_folder)
	with open(out_html, 'w+') as f:
		f.write(header)
		# first write the table header
		benchmark_results = {}
		for benchmark in benchmarks:
			benchmark_results[benchmark[0]] = {}

		# first gather all the commit results in the dictionary
		for result in results:
			folder = os.path.join(results_folder, result)
			for benchmark in benchmarks:
				benchmark_name = benchmark[0]
				base_path = os.path.join(folder, benchmark_name)
				benchmark_file = base_path + '.csv'
				log_name = base_path + ".log"
				graph_html = base_path + ".html"
				stderr_name = base_path + ".stderr.log"
				if os.path.isfile(benchmark_file):
					(result_html, result_type) = read_results(benchmark_file)
				else:
					(result_html, result_type) = ('-', 'NoResult')

				result_html = bold_output(result_html)

				extra_info = []
				try:
					generate_query_graph.generate(log_name, graph_html)
				except:
					pass
				if os.path.isfile(graph_html):
					extra_info.append(['Q', graph_html])
				if os.path.isfile(log_name):
					extra_info.append(['L', log_name])
				if os.path.isfile(stderr_name):
					extra_info.append(['E', stderr_name])
				if len(extra_info) > 0:
					result_html += " ["
					for i in range(len(extra_info) - 1):
						result_html += '<a href="%s">%s</a>/' % (extra_info[i][1], extra_info[i][0])
					result_html += '<a href="%s">%s</a>]' % (extra_info[-1][1], extra_info[-1][0])
				table_class = None
				if result_type == 'Crash' or result_type == 'Incorrect':
					table_class = 'table-danger'
				elif result_type == 'Timeout':
					table_class = 'table-warning'
				elif result_type == 'Unknown':
					table_class = 'table-info'


				benchmark_results[benchmark_name][result] = (result_html, table_class)
		# now write the actual tables
		# we create one table per group
		groups.sort()
		for group in groups:
			# the header is all the commits
			begin_row(f)
			# one extra header for the benchmark name
			begin_header(f)
			f.write(group)
			end_header(f)
			for result in results:
				begin_rotated_header(f)
				write_commit(f, result[5:])
				end_rotated_header(f)
			end_row(f)
			#now write the results
			group_elements = sorted(benchmarks_per_group[group], key=lambda x: benchmarks[x][0])
			for benchmark_id in group_elements:
				benchmark_name = benchmarks[benchmark_id][0]
				begin_row(f)
				# benchmark name
				begin_value(f, 'table-active')
				f.write('<a href="benchmark_results/info/%s.log">%s</a>' % (benchmark_name,benchmark_name))
				end_value(f)
				# benchmark results
				for result in results:
					(html, table_class) = benchmark_results[benchmark_name][result]
					begin_value(f, table_class)
					f.write(html)
					end_value(f)

				end_row(f)

		f.write(footer)
	os.system('./transfer.sh')

if __name__ == "__main__":
	create_html("benchmark_results")