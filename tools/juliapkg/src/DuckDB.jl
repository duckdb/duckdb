module DuckDB

using DBInterface
using WeakRefStrings
using Serialization
using Tables
using Base.Libc
using Debugger

export DBInterface, DuckDBException

include("ctypes.jl")
include("api.jl")
include("database.jl")
include("statement.jl")
include("result.jl")
include("tables.jl")

end # module
