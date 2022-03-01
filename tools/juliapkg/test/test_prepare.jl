# test_prepare.jl

@testset "Test DBInterface.prepare" begin
    con = DBInterface.connect()

    DBInterface.execute(con, "CREATE TABLE test_table(i INTEGER, j DOUBLE)")
    stmt = DBInterface.prepare(con, "INSERT INTO test_table VALUES(?, ?)")

    DBInterface.execute(stmt, [1, 3.5])
    DBInterface.execute(stmt, [missing, nothing])
    DBInterface.execute(stmt, [2, 0.5])

    results = DBInterface.execute(con, "SELECT * FROM test_table")
    df = DataFrame(results)

    @test isequal(df.i, [1, missing, 2])
    @test isequal(df.j, [3.5, missing, 0.5])

	# execute many
	DBInterface.executemany(stmt, (col1=[1,2,3,4,5], col2=[1, 2, 4, 8, -0.5]))

    results = DBInterface.execute(con, "SELECT * FROM test_table")
    df = DataFrame(results)

    @test isequal(df.i, [1, missing, 2, 1, 2, 3, 4, 5])
    @test isequal(df.j, [3.5, missing, 0.5, 1, 2, 4, 8, -0.5])

	# verify that double-closing does not cause any problems
    DBInterface.close!(stmt)
    DBInterface.close!(stmt)
    DBInterface.close!(con)
    DBInterface.close!(con)
end

@testset "DBInterface.prepare: named parameters not supported yet" begin
    con = DBInterface.connect()

    DBInterface.execute(con, "CREATE TABLE test_table(i INTEGER, j DOUBLE)")
	@test_throws DuckDB.QueryException DBInterface.prepare(con, "INSERT INTO test_table VALUES(:col1, :col2)")

    DBInterface.close!(con)
end
