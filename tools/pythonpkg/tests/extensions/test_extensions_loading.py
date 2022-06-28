import os
def test_extension_loading(require):
    if not os.getenv('DUCKDB_PYTHON_TEST_EXTENSION_REQUIRED', False):
        return
    extensions_list = ['substrait', 'json', 'excel', 'httpfs', 'tpch', 'tpcds', 'icu', 'visualizer', 'fts']
    for extension in extensions_list:
        connection = require(extension)
        assert connection is not None