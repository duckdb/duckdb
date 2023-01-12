
# test_union_type.jl

@testset "Test Union Type" begin
    db = DBInterface.connect(DuckDB.DB)
    con = DBInterface.connect(db)

    DBInterface.execute(
        con,
        """
        	create table tbl (
        	u UNION (a BOOL, b VARCHAR)
        );
        """
    )
    DBInterface.execute(
        con,
        """
        	insert into tbl VALUES('str'), (true);
        """
    )
    df = DataFrame(DBInterface.execute(
        con,
        """
        	select u from tbl;
        """
    ))
    @test isequal(df.u, ["str", true])
    DBInterface.execute(
        con,
        """
        	insert into tbl VALUES(NULL);
        """
    )
    df = DataFrame(DBInterface.execute(
        con,
        """
        	select u from tbl;
        """
    ))
    @test isequal(df.u, ["str", true, missing])

end
