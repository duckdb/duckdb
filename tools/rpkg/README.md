<img src="https://duckdb.org/images/DuckDB_Logo_dl.png" height="50">

# duckdb R package

## Installation from CRAN

```r
install.packages("duckdb")
```

## Building

The default build compiles a release version from an amalgamation.

```sh
cd tools/rpkg
R CMD INSTALL .
```

Optional extensions can be enabled by passing them (comma-separated, if there is more than one) to the environment variable `DUCKDB_R_EXTENSIONS`:

```sh
DUCKDB_R_EXTENSIONS=tpch R CMD INSTALL .
```

## Development

To build the R package, you first need to install the dependencies, these are located in `tools/rpkg/dependencies.R`
Navigate over to the `tools/rpkg` folder and run `Rscript dependencies.R`

For development, setting the `DUCKDB_R_DEBUG` environment variable enables incremental debugging builds for the R package.

```sh
cd tools/rpkg
DUCKDB_R_DEBUG=1 R CMD INSTALL .
```

This also works for devtools:

```r
Sys.setenv(DUCKDB_R_DEBUG = "1")
pkgload::load_all()
```

Add the following to your `.Renviron` to make this the default:

```
DUCKDB_R_DEBUG=1
```

If you do this, remember to use `--vanilla` for building release builds.

### Developing with Extensions
If you wish to build or add extensions to the R package you first need to build duckdb with the 
`extension_static_build` flag and move the desired extension to a location where it won't be 
modified by other build scripts. The following commands allow you to add the httpfs extension to 
a duckdb R build. See the [extension ReadMe](https://github.com/duckdb/duckdb/tree/main/extension#readme) for more 
information about extensions 
```sh
cd duckdb/
EXTENSION_STATIC_BUILD=1 make
mv ./build/release/extension/httpfs/httpfs.duckdb_extension {{untouched_path}}/httpfs-extension.duckdb_extension
```
Then in R
```r
library(“duckdb”)
con <- DBI::dbConnect(duckdb(config=list(“allow_unsigned_extensions”=“true”)))
dbExecute(con, “LOAD {{untouched_path}}/httpfs-extension.duckdb_extension")
```