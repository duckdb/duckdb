# C API

The C API provides a stable interface based on DuckDB's internal C++ code.
Thus, applications built against the C API avoid breaking due to changes in DuckDB's fast-moving codebase.

## Headers

### Header generation

DuckDB's C API headers are code generated from JSON definition files.
There are multiple reasons for this:
- easy to structure the C API headers
- code generation in DuckDB clients based on the C API without parsing a C header
- versioned function-pointer-struct API for DuckDB extensions based on the C API

The C API currently consists of 2 main parts:
- the C API as defined in `duckdb.h` (main header)
- the function-pointer-struct in `duckdb_extension.h` (extension header)

### The main header `duckdb.h`

The main header contains macros, typedefs and all functions of the DuckDB C API.
Programs linking to DuckDB use this API, e.g., the C API-based clients.
To link against DuckDB via the C API, the `duckdb.h` file of the desired DuckDB version
must be included, after which the program can link to a DuckDB (bundled) library of that specific version.

### The extension API header `duckdb_extension.h`

The C extension API consists of a struct of function pointers that is passed to an extension on load.
Because this struct is versioned, extensions based on the extension API can work across a wide range of
DuckDB versions without requiring a rebuild.
Forcing the function-pointer-struct to never change between DuckDB versions achieves this stability.
It is append-only, meaning new functions are always added to the end of the struct.
Due to the rigid nature of the struct, not all function that are added to the C API are immediately added to this struct.

## Adding a function to the C API

The only way to add functions to the C api is to add them to the `JSON` definition files in `src/include/duckdb/main/capi/header_generation/functions`.
Please note that the C API is intended as a minimal wrapper around DuckDB's internal C++ functions.
As such, we strive to add as few helper-only functions to the C API as possible.
To add a C API function to the headers, there are two options.
Note that you are probably looking for option 2, which adds the function to both the unstable C API and the `dev` version of the extension API header.

### 1. Adding a function to the C API only

Here are the steps to add a function to the C API, but not the function-pointer-struct:

1. Create an entry in one of the `src/include/duckdb/main/capi/header_generation/functions/*.json` files.
2. Add the function to the exclusion list in `src/include/duckdb/main/capi/header_generation/apis/v0/exclusion_list.json` to mark it as excluded. 
Please provide a reason for each group of functions to ensure we properly document why these are excluded.
Also consider merging groups of excluded functions with similar exclusion reasons together to keep things tidy. 
3. Run the generation script `scripts/generate_c_api.py` or the Makefile target `make generate-files`.

### 2. Adding a function to the C API and the extension API

Here are the steps to add a function to the C API and to the function-pointer-struct:

1. Create an entry in one of the `src/include/duckdb/main/capi/header_generation/functions/*.json` files.
2. Add the function to `src/include/duckdb/main/capi/header_generation/apis/v1/*/*.json`. 
3. Run the generation script `scripts/generate_c_api.py` or the Makefile target `make generate-files`.

#### Adding a function to the `dev` version

The first way a function is added to the extension API is likely via the `dev` version. 
This allows testing from extensions, for example in the CI, while not yet promising stability of the function.
Most functions fall in this category.

#### Adding a function to a stable version

When a function is deemed stable, the function can be added to a versioned part of the extension API struct.

To not break backwards compatibility, functions are only added to the function-pointer-struct by 
adding a new `JSON` file to the `src/include/duckdb/main/capi/header_generation/apis/v0` directory.
Note that the version tag is important here since it determines the order of the struct.

Another thing to consider is grouping additions to the stable version, thus, avoiding needlessly bumping this version for every new stable function.

## The JSON files

As mentioned before, DuckDB's C API headers are code generated from JSON definition files.

- A schema to define a list of unstable functions. Describes the files in `./apis/v1/unstable`:
```
./schemata/unstable_function_list_schema.json
```
- A schema to define a function group. Describes the files in `./functions`:
```
./schemata/function_group_schema.json
```
