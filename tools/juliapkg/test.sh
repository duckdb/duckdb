set -e

#julia -e "import Pkg; Pkg.activate(\".\"); Pkg.instantiate(); include(\"test/runtests.jl\")" $1

#export JULIA_DUCKDB_LIBRARY="`pwd`/../../build/debug/src/libduckdb.dylib"
export JULIA_DUCKDB_LIBRARY="`pwd`/../../build/release/src/libduckdb.dylib"

export JULIA_NUM_THREADS=1
julia -e "import Pkg; Pkg.activate(\".\"); include(\"test/runtests.jl\")" $1
