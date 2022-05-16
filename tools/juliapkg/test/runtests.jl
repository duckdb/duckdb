using DataFrames
using DuckDB
using Test
using Dates
using FixedPointDecimals
using UUIDs

test_files = [
    "test_appender.jl",
    "test_basic_queries.jl",
    "test_config.jl",
    "test_connection.jl",
    "test_df_scan.jl",
    "test_prepare.jl",
    "test_transaction.jl",
    "test_sqlite.jl",
    "test_replacement_scan.jl",
    "test_table_function.jl",
    "test_old_interface.jl",
    "test_all_types.jl",
    "test_decimals.jl",
    "test_threading.jl",
    "test_tpch.jl"
]

if size(ARGS)[1] > 0
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
