# test_connection.jl

@testset "Test opening a bogus directory" begin
    @test_throws DuckDB.ConnectionException DBInterface.connect("/path/to/bogus/directory")
end

@testset "Test opening and closing an in-memory database" begin
    con = DBInterface.connect(":memory:")
    DBInterface.close!(con)
	# verify that double-closing does not cause any problems
    DBInterface.close!(con)
    DBInterface.close!(con)
end
