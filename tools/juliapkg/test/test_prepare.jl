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
    println(df)

    @test isequal(df.i, [1, missing, 2])
    @test isequal(df.j, [3.5, missing, 0.5])

    #  	# prepare a statement with named parameters
    # 	stmt = DBInterface.prepare(conn, "INSERT INTO test_table VALUES(:col1, :col2)")
    # 	DBInterface.execute(stmt, (col1=1, col2=3.5))


    DBInterface.close!(con)
end
