def test_substrait_protobuf_leakage(duckdb_cursor):
    try:
        import duckdb
		# If missing: python3 -m pip install -r tools/pythonpkg/requirements-dev.txt
        from google.cloud import storage
        can_import = True
    except:
        can_import = False
    assert(can_import)