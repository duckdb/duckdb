import pytest
import sys

pyarrow = pytest.importorskip("pyarrow")

class TestImportWithoutPyArrowDataset:
	def test_import(self, monkeypatch: pytest.MonkeyPatch):
		monkeypatch.setitem(sys.modules, "pyarrow.dataset", None)
		import duckdb
		# We should be able to import duckdb even when pyarrow.dataset is missing

		rel = duckdb.query('select 1')
		arrow_table = rel.arrow()
		with pytest.raises(ModuleNotFoundError):
			# The replacement scan functionality relies on pyarrow.dataset
			duckdb.query('select * from arrow_table')
