import pytest
import duckdb
import io

fsspec = pytest.importorskip("fsspec")


class TestReadParquet(object):
    def test_fsspec_deadlock(self, duckdb_cursor, tmp_path):
        # Create test parquet data
        file_path = tmp_path / "data.parquet"
        duckdb_cursor.sql("COPY (FROM range(50_000)) TO '{}' (FORMAT parquet)".format(str(file_path)))
        with open(file_path, "rb") as f:
            parquet_data = f.read()

        class TestFileSystem(fsspec.AbstractFileSystem):
            protocol = "deadlock"

            @property
            def fsid(self):
                return "deadlock"

            def ls(self, path, detail=True, **kwargs):
                vals = [k for k in self._data.keys() if k.startswith(path)]
                if detail:
                    return [
                        {
                            "name": path,
                            "size": len(self._data[path]),
                            "type": "file",
                            "created": 0,
                            "islink": False,
                        }
                        for path in vals
                    ]
                else:
                    return vals

            def _open(self, path, **kwargs):
                return io.BytesIO(self._data[path])

            def __init__(self):
                super().__init__()
                self._data = {"a": parquet_data, "b": parquet_data}

        fsspec.register_implementation("deadlock", TestFileSystem, clobber=True)
        fs = fsspec.filesystem('deadlock')
        duckdb_cursor.register_filesystem(fs)

        result = duckdb_cursor.read_parquet(file_globs=["deadlock://a", "deadlock://b"], union_by_name=True)
        assert len(result.fetchall()) == 100_000
