import pytest

import subprocess
import os

class TestExamples(object):
	def test_examples(self):
		current_dir = os.path.dirname(os.path.abspath(__file__))
		relative_path = "../../../../examples/python/duckdb-python.py"
		absolute_path = os.path.abspath(os.path.join(current_dir, relative_path))
		result = subprocess.run(['python3', absolute_path])
		assert result.returncode == 0
		assert not result.stderr
