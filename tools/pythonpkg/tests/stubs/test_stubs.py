import mypy.stubtest
import subprocess
import sys

def test_stubs():
	# just run stubtest
	assert subprocess.run(
		[sys.executable, '-m', 'mypy.stubtest', 'duckdb'],
	).returncode == 0, 'Stubtest failed! Check stdout.'