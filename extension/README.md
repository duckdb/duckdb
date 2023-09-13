This readme explains what types of extensions there are in DuckDB and how to build them.

# What are DuckDB extensions?
DuckDB extensions are libraries containing additional DuckDB functionality separate from the main codebase. These
extensions can provide added functionality to DuckDB that can/should not live in DuckDB main code for various reasons.
DuckDB extensions can be built in two ways. Firstly, they can be statically linked into DuckDBs executables (duckdb cli,
unittest binary, benchmark runner binary, etc). Doing so will automatically make them available when using these binaries.
Secondly, DuckDB has an extension loading mechanism to dynamically load extension binaries. 

# Extension Types
DuckDB Extensions can de divided into different types: In-tree extensions and out-of-tree extensions. These types refer 
to where the extensions live and who maintains them.

### In-tree extensions
In-tree extensions are extensions that live in the main DuckDB repository. These extensions are considered fundamental 
to DuckDB and/or tie into to DuckDB so deeply that changes to DuckDB are expected to regularly break them.  We aim to 
keep the amount of in-tree extensions to a minimum and strive to move extensions out-of-tree where possible.
### Out-of-tree Extensions (OOTEs)
Out-of-tree extensions live in separate repositories outside the main DuckDB repository. The reasons for moving extensions
out-of-tree can vary. Firstly, moving extensions out of the main DuckDB code-base keeps the core DuckDB code smaller
and less complex. Secondly, keeping extensions out-of-tree can be useful for licensing reasons.

There are two main types of OOTEs. Firstly, there are the **DuckDB Managed OOTEs**. These are distributed through the main
DuckDB CI. These extensions are signed using DuckDBs signing key and are maintained by the DuckDB team. Some examples are 
the `sqlite_scanner` and `postgres_scanner` extensions. The DuckDB Managed OOTEs are distributed automatically with every
release of DuckDB. For the current list of extensions in this category check out `.github/config/out_of_tree_extensions.cmake`

Secondly, there are **External OOTEs**. Extensions in this category are not tied to the DuckDB CI, but instead their CI/CD
runs in their own repository. The maintainer of the external OOTE repo is responsible for testing, distribution and making
sure that an up-to-date version of the extension is available. Depending on who maintains the extension, these extensions
may or may not be signed.

# Building extensions
Under the hood, all types of extensions are built the same way, which is using the DuckDB's root `CMakeLists.txt` file as root CMake file
and passing the extensions that should be build to it. DuckDB has various methods to configure which extensions to build. 
Additionally, we can configure for each extension how we want to build it: for example, whether to only
build the loadable extension, or also link the extension in the DuckDB binaries. There's different ways to load extensions
in DuckDB with various

## Makefile/Cmake variables
The simplest way to specify which extensions to load is using the `DUCKDB_EXTENSIONS` variable. To specify which extensions
to build when making duckdb set the extensions variable to a `;` separated list of extensions names. For example:
```bash
DUCKDB_EXTENSIONS='json;icu' make
```
The `DUCKDB_EXTENSIONS` variable is simply passed to a CMake variable `BUILD_EXTENSIONS` which can also be invoked directly:
```bash
cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_EXTENSIONS='parquet;icu;tpch;tpcds;fts;json'
```

## Makefile environment variables
Another way to specify building an extension is with the `BUILD_<extension name>` variables defined in the root
`Makefile` in this repository. For example, to build the JSON extension, simply run `BUILD_JSON=1 make`. These Makevars
should be added manually for each extension and are simply syntactic sugar around the DUCKDB_EXTENSIONS variable.

## Config files
To have more control over how in-tree extensions are built, extension config files should be used. These config files 
are simply CMake files that are included by DuckDB's CMake build. There are 4 different places that will be searched 
for config files:

1) The base configuration `extension/extension_config.cmake`. The extensions specified here will be built every time DuckDB
is built. This configuration is always loaded.
2) (Optional) The client specific extensions specification in `tools/*/duckdb_extension_config.cmake`. These config specify
which extensions are built and linked into each client.
3) (Optional) The local configuration file `extension/extension_config_local.cmake` This is where you would specify extensions you need 
included in your local/custom/dev build of DuckDB. This file is gitignored and to be created by the developer.
4) (Optional) Additional configuration files passed to the `DUCKDB_EXTENSION_CONFIGS` parameter. This can be used to point DuckDB
to config files stored anywhere on the machine.

DuckDB will load these config files in reverse order and ignore subsequent calls to load an extension with the 
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

# Using extension config files
The `duckdb_extension_load` function is used in the configuration files to specify how an extension should
be loaded. There are 3 different ways this can be done. For some examples, check out `.github/config/*.cmake`. These are
the configurations used in DuckDBs CI to select which extensions are built.

## Automatic loading
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

## Custom path
When extensions are located in a  path or their project structure is different from that the
[extension-template](https://github.com/duckdb/extension-template), the `SOURCE_DIR` and `INCLUDE_DIR` variables can
be used to tell DuckDB how to load the extension:
```cmake
duckdb_extension_load(<extension_name>
    (DONT_LINK)
    SOURCE_DIR <absolute_path_to_extension_root>
    (INCLUDE_DIR <absolute_path_to_extension_header>)
)
```

## Remote GitHub repo
Directly installing extensions from GitHub repositories is also supported. This will download the extension to the current
cmake build directory and build it from there: 
```cmake
duckdb_extension_load(postgres_scanner
    (DONT_LINK)
    GIT_URL https://github.com/duckdblabs/postgres_scanner
    GIT_TAG cd043b49cdc9e0d3752535b8333c9433e1007a48
)
```

# Explicitly disabling extensions
Because the sometimes you may want to override extensions set by other configurations, explicitly disabling extensions is 
also possible using the `DONT_BUILD flag`. This will disable the extension from being built all together. For example, to build DuckDB without the parquet extension which is enabled by default, in `extension/extension_config_local.cmake` specify:
```cmake
duckdb_extension_load(parquet DONT_BUILD)
```
Note that this can also be done from the Makefile:
```bash
DUCKDB_EXTENSIONS='tpch;json' SKIP_EXTENSIONS=parquet make
```
results in:
```bash
...
-- Building extension 'tpch' from '/Users/sam/Development/duckdb/extensions'
-- Building extension 'json' from '/Users/sam/Development/duckdb/extensions'
-- Extensions linked into DuckDB: tpch, json
-- Extensions explicitly skipped: parquet
...
```

# VCPKG dependency management
DuckDB extensions can use [VCPKG](https://vcpkg.io/en/) to manage their dependencies. Check out the [Extension Template](https://github.com/duckdb/extension-template) for an example
on how to set up vcpkg in extensions. 

## Building DuckDB with multiple extensions that use vcpkg
To build duckdb with multiple extensions that all use vcpkg, some extra steps are required. This is due to the fact that each
extension will specify their own vcpkg.json manifest for their dependencies, but vcpkg allows only a single manifest. The workaround here
is to merge the dependencies from the manifests of all extensions being built. This repo contains a script to do automatically perform this merge.

### Example build with 2 extensions using vcpkg
For example, lets say we want to create a DuckDB binary which has two extensions statically linked that each use vcpkg. The first step is to add the two extensions
to `extension/extension_config_local.cmake`:
```cmake
duckdb_extension_load(extension_1
    GIT_URL https://github.com/example/extension_1
    GIT_TAG some_git_hash
)
duckdb_extension_load(extension_2
    GIT_URL https://github.com/example/extension_2
    GIT_TAG some_git_hash
)
```
Now to merge the vcpkg.json manifests from these two extension run:
```shell   
make extension_configuration
```
This will create a merged manifest in `./build/extension_configuration/vcpkg.json`.

Next, run:
```shell
USE_MERGED_VCPKG_MANIFEST=1 VCPKG_TOOLCHAIN_PATH="/path/to/your/vcpkg/installation" make
```
which will use the merged manifest to install all required dependencies, build `extension_1` and `extension_2`, build DuckDB, 
and finally link both extensions into DuckDB.
