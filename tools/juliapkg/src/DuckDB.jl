module DuckDB

using DBInterface
using WeakRefStrings
using Tables
using Base.Libc

export DBInterface, DuckDBException

include("exceptions.jl")
include("ctypes.jl")
include("api.jl")
include("database.jl")
include("statement.jl")
include("result.jl")

end # module
