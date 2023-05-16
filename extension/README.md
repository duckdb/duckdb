# In-Tree Extensions
These are DuckDB's In-tree extensions, meaning that their code lives in the main DuckDB repository. These extensions
are considered fundamental to DuckDB and connect to DuckDB so deeply that changes to DuckDB will regularly break them. 
We aim to keep the amount of in-tree extensions to a minimum and strive to move extensions out-of-tree where possible.

## Building extensions
Both in-tree extensions and out-of-tree extensions are built the same way. To build an extension, it needs to be registered
in the main DuckDB CMake build. This can be done in several ways.

### Extension config file
To configure which extensions are built using a config file, pass the path to `<your_config>.cmake` to the `DUCKDB_EXTENSION_CONFIG`
CMake variable. This config file will be included in the DuckDB CMake build and allows configuring extensions through 
the `register_extension` CMake function.  

### Manually setting the extension variables
It's also possible to manually set the extension variables that are set by `register_extension`. 