# test_stream_data_chunk.jl

@testset "Test streaming data chunk API" begin

    con = DBInterface.connect(DuckDB.DB)
    res = DBInterface.execute(con, "SELECT * FROM range(10000) t(i)")

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

