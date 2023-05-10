import pytest
import duckdb

class TestHTTPFS:
	def test_httpfs(self):
		res = duckdb.read_json('https://jsonplaceholder.typicode.com/todos')
		assert len(res.types) == 4
