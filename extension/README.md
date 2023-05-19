This document explains what types of extensions there are in DuckDB and how to build them.

# Extension Types
### In-tree extensions
In-tree extensions are extensions that live in the main DuckDB repository. These extensions are considered fundamental 
to DuckDB and/or connect to DuckDB so deeply that changes to DuckDB are expected to regularly break them.  We aim to 
keep the amount of in-tree extensions to a minimum and strive to move extensions out-of-tree where possible.
### Out-of-tree Extensions (OOTEs)
Out-of-tree extensions live in separate repositories outside the main DuckDB repository. These extensions can be
distributed in two ways: Firstly, they can be distributed using the CI running in their own repository. In this case the 
owner of the OOTE repository is responsible for ensuring the extension is passes CI and is kept up to date with DuckDB.
Secondly OOTEs can be pulled into the main DuckDB CI. In this case extensions are built and distributed by the main 
DuckDB CI. Some examples here are the `sqlite_scanner` and `postgres_scanner` extensions. For the complete list of
extensions built using the main DuckDB repository CI check out the extension configuration in 
`.github/config/external_extension_config.cmake`

# Building extensions
Under the hood, all types of extensions are built the same way, which is using the DuckDB's root `CMakeLists.txt` file as root CMake file
and passing the extensions that should be build to it. Configuring which extensions are built by the DuckDB can be done in
different ways.

## Makefile environment variables
Simplest way to build an extension is to use the `BUILD_<extension name>` environment variables defined in the root
`Makefile` in this repository. For example, to build the JSON extension, simply run `BUILD_JSON=1 make`. Note that this
will only work for in-tree extensions since out of tree extensions require extra configuration steps

## CMake variables
TODO

## Config files
To build out-of-tree extensions or have more control over how in-tree extensions are built, extension config files should
be used. These config files are simply CMake files thait are included by DuckDB's CMake build. There are 3 different places 
that will be searched for config files:

1) The base configuration `extension/extension_config.cmake`. The extensions specified here will be built every time duckdb
is built.
2) The local configuration file `extension/extension_config_local.cmake` This is where you would specify extensions you need 
included in your local/custom/dev build of DuckDB. 
3) Additional configuration files passed to the `DUCKDB_EXTENSION_CONFIGS` parameter. This can be used to point DuckDB
to config files stored anywhere on the machine.

Note that DuckDB will load these config files in reverse order and ignore subsequent calls to load an extension with the 
same name. This allows overriding the base configuration of an extension by providing a different configuration
in the local config. For example, currently the parquet extension is always statically linked into DuckDB, because of this 
line in `extension/extension_config.cmake`:
```cmake
duckdb_extension_load(parquet)
```
Now say we want to build DuckDB with our custom parquet extension, and we also don't want to link this statically in DuckDB, 
but only produce the loadable binary. We can achieve this creating the `extension/extension_config_local.cmake` file and adding:
```cmake
duckdb_extension_load(parquet
    DONT_LINK
    SOURCE_DIR /path/to/my/custom/parquet
)
```
Now when we run `make` cmake will output:
```shell
-- Building extension 'parquet' from 'path/to/my/custom/parquet'
-- Extensions built but not linked: parquet
```

## Loading extensions with config files
The `duckdb_extension_load` function is used in the configuration files to specify how an extension should
be loaded. There are 3 different ways this can be done. For some examples, check out `.github/config/*.cmake`. These are
the configurations used in DuckDBs CI to select which extensions are built.

### Automatic loading
The simplest way to load an extension is just passing the extension name. This will automatically try to load the extension.
Optionally, the DONT_LINK parameter can be passed to disable linking the extension into DuckDB.
```cmake
duckdb_extension_load(<extension_name> (DONT_LINK))
```
This configuration of `duckdb_extension_load` will search the `./extension` and `./extension_external` directories for
extensions and attempt to load them if possible. Note that the `extension_external` directory does not exist but should
be created and populated with the out-of-tree extensions that should be built. Extensions based on the
[extension-template](https://github.com/duckdb/extension-template) should work out of the box using this automatic
loading when placed in the `extension_external` directory.

### Custom path
When extensions are located in a  path or their project structure is different from that the
[extension-template](https://github.customcom/duckdb/extension-template), the `SOURCE_DIR` and `INCLUDE_DIR` variables can
be used to tell DuckDB how to load the extension:
```cmake
duckdb_extension_load(<extension_name>
    (DONT_LINK)
    SOURCE_DIR <absolute_path_to_extension_root>
    (INCLUDE_DIR <absolute_path_to_extension_header>)
)
```

### Remote GitHub repo
Directly installing extensions from GitHub repositories is also supported. This will download the extension to the current
cmake build directory and build it from there: 
```cmake
duckdb_extension_load(postgres_scanner
    (DONT_LINK)
    GIT_URL https://github.com/duckdblabs/postgres_scanner
    GIT_TAG cd043b49cdc9e0d3752535b8333c9433e1007a48
)
```

### Explicitly disabling extensions
Because the sometimes you may want to override extensions set by other configurations, explicitly disabling extensions is 
also possible using the `DONT_BUILD flag`. This will disable the extension from being built alltogether. For example, to 
build DuckDB without the parquet extension which is enabled by default, in `extension/extension_config_local.cmake` specify:
```cmake
duckdb_extension_load(parquet DONT_BUILD)
```