# test_stream_data_chunk.jl


@testset "Test streaming result sets" begin
    result_types::Vector = Vector()
    push!(result_types, DuckDB.MaterializedResult)
    push!(result_types, DuckDB.StreamResult)

    for result_type in result_types
        con = DBInterface.connect(DuckDB.DB)
        res = DBInterface.execute(con, "SELECT * FROM range(10000) t(i)", result_type)

        @test res.names == [:i]
        @test res.types == [Union{Missing, Int64}]

        # loop over the chunks and perform a sum + count
        sum::Int64 = 0
        total_count::Int64 = 0
        while true
            # fetch the next chunk
            chunk = DuckDB.nextDataChunk(res)
            if chunk === missing
                # consumed all chunks
                break
            end
            # read the data of this chunk
            count = DuckDB.get_size(chunk)
            data = DuckDB.get_array(chunk, 1, Int64)
            for i in 1:count
                sum += data[i]
            end
            total_count += count
            DuckDB.destroy_data_chunk(chunk)
        end
        @test sum == 49995000
        @test total_count == 10000
    end
    GC.gc(true)
end

@testset "Test giant streaming result" begin
    # this would take forever if it wasn't streaming
    con = DBInterface.connect(DuckDB.DB)
    res = DBInterface.execute(con, "SELECT * FROM range(1000000000000) t(i)", DuckDB.StreamResult)

    @test res.names == [:i]
    @test res.types == [Union{Missing, Int64}]

    # fetch the first three chunks
    for i in 1:3
        chunk = DuckDB.nextDataChunk(res)
        @test chunk !== missing
        DuckDB.destroy_data_chunk(chunk)
    end
    DBInterface.close!(res)
    DBInterface.close!(con)
    GC.gc(true)
end

@testset "Test streaming data chunk destruction" begin
    paths = ["types_map.parquet", "types_list.parquet", "types_nested.parquet"]
    for path in paths
        # DuckDB "in memory database"
        connection = DBInterface.connect(DuckDB.DB)
        statement = DuckDB.Stmt(connection, "SELECT * FROM read_parquet(?, file_row_number=1)", DuckDB.StreamResult)
        result = DBInterface.execute(statement, [joinpath(@__DIR__, "resources", path)])
        num_columns = length(result.types)

        while true
            chunk = DuckDB.nextDataChunk(result)
            chunk === missing && break # are we done?

            num_rows = DuckDB.get_size(chunk) # number of rows in the retrieved chunk

            row_ids = DuckDB.get_array(chunk, num_columns, Int64)
            # move over each column, last column are the row_ids
            for column_idx in 1:(num_columns - 1)
                column_name::Symbol = result.names[column_idx]

                # Convert from the DuckDB internal types into Julia types
                duckdb_logical_type = DuckDB.LogicalType(DuckDB.duckdb_column_logical_type(result.handle, column_idx))
                duckdb_conversion_state = DuckDB.ColumnConversionData([chunk], column_idx, duckdb_logical_type, nothing)
                duckdb_data = DuckDB.convert_column(duckdb_conversion_state)

                for i in 1:num_rows
                    row_id = row_ids[i] + 1 # julia indices start at 1
                    value = duckdb_data[i]
                    @test value !== missing
                end
            end
            DuckDB.destroy_data_chunk(chunk)
        end
        close(connection)
    end
    GC.gc(true)
end
