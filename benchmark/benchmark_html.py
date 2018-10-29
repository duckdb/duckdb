
import numpy, os, sys

out_html = 'index.html'

header = """<html>
<head>
<meta charset="UTF-8">
<style>
table, td, th {
  border-spacing: 0;
  border: 1px solid;
  border-collapse: collapse;
  text-align: center;
  table-layout: fixed;
}

.vert > div {
  overflow: hidden;
}

.vert > div > div {
  display: inline-block;
  vertical-align: middle;
  transform: rotate(-90deg);
  line-height: 1em;
}

.vert > div:before {
  content: "";
  height: 0;
  padding-top: 100%;
  display: inline-block;
  vertical-align: middle;
}
</style>
</head>
<body>
<table class="table table-header-rotated">
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
			benchmarks.append([name, description])
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
	f.write('<th class="vert"><div><div>')

def end_rotated_header(f):
	f.write("</div></div></th>")

def begin_value(f):
	f.write("<td>")

def end_value(f):
	f.write("</td>")

def color_output(output, r, g, b):
	return '<span style=color="rgb(%d,%d,%d)">%s</span>' % (r, g, b, output)

def bold_output(output):
	return "<b>%s</b>" % (output,)

def write_commit(f, commit):
	f.write('<a href="https://github.com/cwida/duckdb/commit/%s">%s</a>'  % (commit, commit[:4]))

def read_results(file):
	with open(file, 'r') as f:
		lines = f.read().split('\n')
		if len(lines) == 0:
			return bold_output(color_output('????', 184, 134, 11));
		# try to parse everything as numbers
		try:
			numbers = [float(x) for x in lines if len(x) > 0]
			return "%.2f" % numpy.mean(numbers)
		except:
			# failure to parse
			if lines[0] == 'TIME':
				return bold_output(color_output('T', 139, 10, 80));
			elif lines[0] == 'CRASH':
				return bold_output(color_output('C', 178, 34, 34));
			else:
				return bold_output(color_output('????', 184, 134, 11));


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
			results_dictionary = {}
			for benchmark in benchmarks:
				benchmark_name = benchmark[0]
				benchmark_file = os.path.join(folder, benchmark_name + '.csv')
				if os.path.isfile(benchmark_file):
					benchmark_results[benchmark_name][result] = read_results(benchmark_file)
				else:
					benchmark_results[benchmark_name][result] = "-"
		# now write the actual tables
		# we create one table per group
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
			for benchmark_id in benchmarks_per_group[group]:
				benchmark_name = benchmarks[benchmark_id][0]
				begin_row(f)
				# benchmark name
				begin_value(f)
				f.write(benchmark_name)
				end_value(f)
				# benchmark results
				for result in results:
					begin_value(f)
					f.write(benchmark_results[benchmark_name][result])
					end_value(f)

				end_row(f)

		f.write(footer)

if __name__ == "__main__":
	create_html("benchmark_results")