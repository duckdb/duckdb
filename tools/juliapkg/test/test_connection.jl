# test_connection.jl

@testset "Test opening and closing an in-memory database" begin
    con = DBInterface.connect(DuckDB.DB, ":memory:")
    DBInterface.close!(con)
    # verify that double-closing does not cause any problems
    DBInterface.close!(con)
    DBInterface.close!(con)
    @test 1 == 1

    con = DBInterface.connect(DuckDB.DB, ":memory:")
    @test isopen(con)
    close(con)
    @test !isopen(con)
end

@testset "Test opening a bogus directory" begin
    @test_throws DuckDB.ConnectionException DBInterface.connect(DuckDB.DB, "/path/to/bogus/directory")
end
