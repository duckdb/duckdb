# DuckDB C API header generation
DuckDB's C API headers are code generated from json definition files. There are multiple reasons for this:
- it makes it easy to structure the C API headers cleanly
- the definition files allows driving code-gen in DuckDB clients that are based on the C API
- it allows creating a "versioned struct of function pointer"-type API for duckdb extension

The C API currently consists of 2 main parts:
- the C API as defined in `duckdb.h`
- the struct-of-function pointers in `duckdb_extension.h`

## The main header `duckdb.h`
This header contains macros, typedefs and all functions of the DuckDB C API. This API is what programs that link to DuckDB 
will use. To link against DuckDB over the C API, the `duckdb.h` file of the desired DuckDB version 
should be included, after which the program can link to DuckDB of that specific version.

## The extension API header `duckdb_extension.h`
The C extension API consists of a struct-of-function-pointers that is passed to an extension on load. Because this struct 
is versioned, extensions based on this API can work across a wide range of DuckDB versions without requiring a rebuild. This is
achieved by forcing the struct-of-function-pointers to never change between DuckDB versions, only adding new functions at 
the end of the struct. Due to the rigid nature of the struct, not all function that are added to the C API are immediately 
added to this struct.

# How to add a function to the C API
The only way to add function to the C api is to add them to the `JSON` definition files in `src/include/duckdb/main/capi/header_generation/functions`. To add a C API function to the headers, there are two options.
Note that the one you are probably looking for is adding the function to both C API and the `dev` version of the Extension C API. 

## Adding the function to the C API only
To add a function to the C API, but not the struct-of-function-pointers, simply create an entry in one of the `src/include/duckdb/main/capi/header_generation/functions/*.json` file for it.
Next, the function should be added to the exclusion list in `src/include/duckdb/main/capi/header_generation/apis/v0/exclusion_list.json` to mark them as excluded. Please provide
a reason for each group of functions to ensure we properly document why these are excluded. Also consider merging groups of excluded functions with similar exlusion reason together to keep things clean. The final step is to 
run the generation script `scripts/generate_c_api.py` or use the makefile `make generate-files`.

## Adding the function to both the C API and Extension C API
To add a function to the C API and to the struct-of-function-pointers, again create an entry in one of the `src/include/duckdb/main/capi/header_generation/functions/*.json` files.
Then, the function should be added to `src/include/duckdb/main/capi/header_generation/apis/v0/*/*.json`. Again, run the script `scripts/generate_c_api.py` or the makefile target `make generate-files` to generate.

### Adding a function to the `dev` version
The first way a function will be added to the Extension C API is likely in the `dev` version. This will allow testing the function from extensions, for example in CI, while not yet promising eternal stability of the function.
Most functions that are added will fall in this category.

### Adding a function to a stable version
When a function is deemed stable, the function can be added to a versioned part of the Extension C API struct.

To not break backwards compatibility, functions can only be added to struct-of-function-pointers by adding a new `JSON` file to the `src/include/duckdb/main/capi/header_generation/apis/v0` directory.
Note that the version tag is important here since these determine the order of the struct.

Another thing to consider is not needlessly bumping this version for every individual C API function that is added. It is preferred to group
additions to the struct in the `dev` version first, as this struct is effectively immutable and if we make a mess out of it we will suffer for eternity. A sensible choice would
be to only merge new functions into a stable version of the struct periodically, e.g. as a new DuckDB release approaches. 