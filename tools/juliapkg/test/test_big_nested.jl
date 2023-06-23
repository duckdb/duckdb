
@testset "Test big list" begin
    con = DBInterface.connect(DuckDB.DB)

    DBInterface.execute(con, "CREATE TABLE list_table (int_list INT[]);")
    DBInterface.execute(con, "INSERT INTO list_table VALUES (range(2049));")
    df = DataFrame(DBInterface.execute(con, "SELECT * FROM list_table;"))
    @test length(df[1, :int_list]) == 2049

    DBInterface.close!(con)
end

@testset "Test big bitstring" begin
    con = DBInterface.connect(DuckDB.DB)

    DBInterface.execute(con, "CREATE TABLE bit_table (bits BIT);")
    # 131073 = 64 * 2048 + 1
    DBInterface.execute(con, "INSERT INTO bit_table VALUES (bitstring('1010'::BIT, 131073));")
    df = DataFrame(DBInterface.execute(con, "SELECT * FROM bit_table;"))
    # Currently mapped to Julia in an odd way.
    # Can reenable following https://github.com/duckdb/duckdb/issues/7065
    # Can't use skip = true prior to Julia 1.7
    @static if VERSION ≥ v"1.7"
        @test length(df[1, :bits]) == 131073 skip = true
    end

    DBInterface.close!(con)
end

@testset "Test big string" begin
    con = DBInterface.connect(DuckDB.DB)

    DBInterface.execute(con, "CREATE TABLE str_table (str VARCHAR);")
    DBInterface.execute(con, "INSERT INTO str_table VALUES (repeat('🦆', 1024) || '🪿');")
    df = DataFrame(DBInterface.execute(con, "SELECT * FROM str_table;"))
    @test length(df[1, :str]) == 1025

    DBInterface.close!(con)
end

@testset "Test big map" begin
    con = DBInterface.connect(DuckDB.DB)

    DBInterface.execute(con, "CREATE TABLE map_table (map MAP(VARCHAR, INT));")
    DBInterface.execute(
        con,
        "INSERT INTO map_table VALUES (map_from_entries([{'k': 'billy' || num, 'v': num} for num in range(2049)]));"
    )
    df = DataFrame(DBInterface.execute(con, "SELECT * FROM map_table;"))
    @test length(df[1, :map]) == 2049

    DBInterface.close!(con)
end

@testset "Test big struct" begin
    con = DBInterface.connect(DuckDB.DB)

    DBInterface.execute(con, "CREATE TABLE struct_table (stct STRUCT(a INT[], b INT[]));")
    DBInterface.execute(con, "INSERT INTO struct_table VALUES ({'a': range(1024), 'b': range(1025)});")
    df = DataFrame(DBInterface.execute(con, "SELECT * FROM struct_table;"))
    s = df[1, :stct]
    @test length(s.a) == 1024
    @test length(s.b) == 1025

    DBInterface.close!(con)
end

@testset "Test big union" begin
    con = DBInterface.connect(DuckDB.DB)

    DBInterface.execute(con, "CREATE TABLE union_table (uni UNION(a INT[], b INT));")
    DBInterface.execute(con, "INSERT INTO union_table (uni) VALUES (union_value(a := range(2049))), (42);")
    df = DataFrame(DBInterface.execute(con, "SELECT * FROM union_table;"))
    @test length(df[1, :uni]) == 2049

    DBInterface.close!(con)
end
