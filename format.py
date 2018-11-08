#!/usr/bin/python

import os, time, sys

last_format_file = '.last_format'
ignore_last_format = False
cpp_format_command = 'clang-format -i -sort-includes=${SORT_INCLUDES} -style=file "${FILE}"'
sql_format_command = 'pg_format "${FILE}" -o "${FILE}.out" && mv "${FILE}.out" "${FILE}"'
extensions = ['.cpp', '.c', '.hpp', '.h', '.cc', '.hh', '.sql']
ignored_files = ['tpch_constants.hpp', 'tpcds_constants.hpp', '_generated', 'tpce_flat_input.hpp']

for arg in sys.argv:
	if arg == '--ignore-last-format':
		ignore_last_format = True

format_commands = {
	'.cpp': cpp_format_command,
	'.c': cpp_format_command,
	'.hpp': cpp_format_command,
	'.h': cpp_format_command,
	'.hh': cpp_format_command,
	'.cc': cpp_format_command,
	'.sql': sql_format_command,
}
# get the last time this command was run, if ever

last_format_time = 0

if not ignore_last_format:
	if os.path.exists(last_format_file):
		with open(last_format_file, 'r') as f:
			try:
				last_format_time = float(f.read())
			except:
				pass


	if last_format_time > 0:
		print('Last format: %s' % (time.ctime(last_format_time)))


time.ctime(os.path.getmtime('tools/sqlite3_api_wrapper/include/sqlite3.h'))

def format_directory(directory, sort_includes=True):
	directory_printed = False
	files = os.listdir(directory)
	for f in files:
		full_path = os.path.join(directory, f)
		if os.path.isdir(full_path):
			format_directory(full_path, sort_includes)
		else:
			# don't format TPC-H constants
			ignored = False
			for ignored_file in ignored_files:
				if ignored_file in full_path:
					ignored = True
					break
			if ignored:
				continue
			for ext in extensions:
				if f.endswith(ext):
					if os.path.getmtime(full_path) > last_format_time:
						format_command = format_commands[ext]
						if not directory_printed:
							print(directory)
							directory_printed = True
						cmd = format_command.replace("${FILE}", full_path).replace("${SORT_INCLUDES}", "1" if sort_includes else "0")
						print(cmd)
						os.system(cmd)
					break

format_directory('src')
format_directory('benchmark')
format_directory('test')
format_directory('third_party/dbgen')
format_directory('third_party/dsdgen', False)
format_directory('third_party/sqlsmith', False)
format_directory('third_party/tpce-tool', False)
format_directory('tools')

# write the last modified time
if not ignore_last_format:
	with open(last_format_file, 'w+') as f:
		f.write(str(time.time()))
