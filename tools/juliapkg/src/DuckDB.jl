module DuckDB

using DBInterface
using WeakRefStrings
using Tables
using Base.Libc
using Dates
using DataFrames
using Tables
using UUIDs
using FixedPointDecimals

export DBInterface, DuckDBException

include("helper.jl")
include("exceptions.jl")
include("ctypes.jl")
include("api.jl")
include("logical_type.jl")
include("value.jl")
include("validity_mask.jl")
include("vector.jl")
include("data_chunk.jl")
include("config.jl")
include("database.jl")
include("statement.jl")
include("result.jl")
include("transaction.jl")
include("ddl.jl")
include("appender.jl")
include("table_function.jl")
include("replacement_scan.jl")
include("data_frame_scan.jl")
include("old_interface.jl")

end # module
