using DataFrames
using Tables
using DuckDB
using Test
using Dates
using FixedPointDecimals
using UUIDs

test_files = [
    "test_appender.jl",
    "test_basic_queries.jl",
    "test_big_nested.jl",
    "test_config.jl",
    "test_connection.jl",
    "test_tbl_scan.jl",
    "test_prepare.jl",
    "test_transaction.jl",
    "test_sqlite.jl",
    "test_replacement_scan.jl",
    "test_table_function.jl",
    "test_old_interface.jl",
    "test_all_types.jl",
    "test_union_type.jl",
    "test_decimals.jl",
    "test_threading.jl",
    "test_tpch.jl",
    "test_tpch_multithread.jl",
    "test_stream_data_chunk.jl"
]

if length(ARGS) > 0 && !isempty(ARGS[1])
    filtered_test_files = []
    for test_file in test_files
        if test_file == ARGS[1]
            push!(filtered_test_files, test_file)
        end
    end
    test_files = filtered_test_files
end

for fname in test_files
    println(fname)
    include(fname)
end
