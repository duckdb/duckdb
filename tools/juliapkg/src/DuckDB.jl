module DuckDB

using DBInterface
using WeakRefStrings
using Tables
using Base.Libc

export DBInterface, DuckDBException

include("helper.jl")
include("exceptions.jl")
include("ctypes.jl")
include("api.jl")
include("database.jl")
include("statement.jl")
include("result.jl")
include("transaction.jl")
include("ddl.jl")
include("appender.jl")
include("old_interface.jl")

end # module
