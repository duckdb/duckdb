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
        end
        @test sum == 49995000
        @test total_count == 10000
    end
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
    end
    DBInterface.close!(res)
    DBInterface.close!(con)
end
