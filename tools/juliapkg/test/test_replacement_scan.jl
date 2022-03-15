# test_replacement_scan.jl

function RangeReplacementScan(info)
	table_name = DuckDB.GetTableName(info)
	number = tryparse(Int64, table_name)
	if number === nothing
		return
	end
	DuckDB.SetFunctionName(info, "range")
	DuckDB.AddFunctionParameter(info, DuckDB.CreateValue(number))
end

@testset "Test replacement scans" begin
    con = DBInterface.connect(DuckDB.DB)

	# add a replacement scan that turns any number provided as a table name into range(X)
	DuckDB.AddReplacementScan(con, RangeReplacementScan, nothing)

    df = DataFrame(DBInterface.execute(con, "SELECT * FROM \"2\" tbl(a)"))
    @test df.a == [0, 1]

	# this still fails
	@test_throws DuckDB.QueryException DBInterface.execute(con, "SELECT * FROM nonexistant")

    DBInterface.close!(con)
end
