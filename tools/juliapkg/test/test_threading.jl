# test_threading.jl

@testset "Test threading" begin
    con = DBInterface.connect(DuckDB.DB)

    DBInterface.execute(con, "CREATE TABLE integers AS SELECT * FROM range(100000000) t(i)")
    results = DBInterface.execute(con, "SELECT SUM(i) sum FROM integers")
    df = DataFrame(results)
    @test df.sum == [4999999950000000]

    DBInterface.close!(con)
end
