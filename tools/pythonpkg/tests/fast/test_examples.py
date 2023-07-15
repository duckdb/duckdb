import os
import sys
import importlib

class TestExamples(object):
	def test_examples(self):
		# In CI runs the structure is slightly different, hence the two different paths
		relative_paths = [
			'../../../../',
			'../../../../../'
		]
		script_dir = os.path.dirname(os.path.abspath(__file__))
		# Append the folder of the module to the paths
		paths = [
			os.path.join(path, 'examples/python') for path in relative_paths
		]
		# Prepend the directory of the script to the paths
		paths = [
			os.path.abspath(os.path.join(script_dir, path)) for path in paths
		]
		sys.path.extend(paths)
		
		# Import the examples module, which subsequently runs
		import duckdb_python
