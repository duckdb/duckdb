import pytest
import sys

pyarrow = pytest.importorskip("pyarrow")


class TestImportWithoutPyArrowDataset:
    def test_import(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setitem(sys.modules, "pyarrow.dataset", None)
        import duckdb

        # We should be able to import duckdb even when pyarrow.dataset is missing
        con = duckdb.connect()
        rel = con.query('select 1')
        arrow_record_batch = rel.record_batch()
        with pytest.raises(duckdb.InvalidInputException):
            # The replacement scan functionality relies on pyarrow.dataset
            con.query('select * from arrow_record_batch')
