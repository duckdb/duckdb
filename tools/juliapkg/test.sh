set -e

julia -e "import Pkg; Pkg.activate(\".\"); Pkg.instantiate(); include(\"test/runtests.jl\")" $1

#export JULIA_DUCKDB_LIBRARY="/Users/myth/Programs/duckdb-bugfix/build/release/src/libduckdb.dylib"
#julia -e "import Pkg; Pkg.activate(\".\"); include(\"test/runtests.jl\")" $1
