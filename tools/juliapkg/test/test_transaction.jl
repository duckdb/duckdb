# test_transaction.jl

@testset "Test DBInterface.transaction" begin
    con = DBInterface.connect(DuckDB.DB, ":memory:")

    # throw an exception in DBInterface.transaction
    # this should cause a rollback to happen
    @test_throws DuckDB.QueryException DBInterface.transaction(con) do
        DBInterface.execute(con, "CREATE TABLE integers(i INTEGER)")
        return DBInterface.execute(con, "SELEC")
    end

    # verify that the table does not exist
    @test_throws DuckDB.QueryException DBInterface.execute(con, "SELECT * FROM integers")

    # no exception, this should work and be committed
    DBInterface.transaction(con) do
        return DBInterface.execute(con, "CREATE TABLE integers(i INTEGER)")
    end
    DBInterface.execute(con, "SELECT * FROM integers")

    DBInterface.close!(con)
end
