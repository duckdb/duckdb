def test_substrait_protobuf_leakage(duckdb_cursor):
    try:
        import duckdb
        from google.cloud import storage
        can_import = True
    except:
        can_import = False
    assert(can_import)