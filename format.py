#!/usr/bin/python

import os

format_command = 'clang-format -i -style=file "${FILE}"'
extensions = ['.cpp', '.c', '.hpp', '.h']


def format_directory(directory):
	print(directory)
	files = os.listdir(directory)
	for f in files:
		full_path = os.path.join(directory, f)
		if os.path.isdir(full_path):
			format_directory(full_path)
		else:
			for ext in extensions:
				if f.endswith(ext):
					cmd = format_command.replace("${FILE}", full_path)
					print(cmd)
					os.system(cmd)
					break

format_directory('src')
format_directory('test')
