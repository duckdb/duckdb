using DataFrames
using DuckDB
using Test

test_files = [
	"test_appender.jl",
    "test_connection.jl",
    "test_basic_queries.jl",
    "test_prepare.jl",
    "test_transaction.jl",
    "test_sqlite.jl",
    "test_old_interface.jl"
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
