# test_tpch_multithread.jl

# DuckDB needs to have been built with TPCH (BUILD_TPCH=1) to run this test!

function test_tpch_multithread()
    sf = "0.10"

    # load TPC-H into DuckDB
    native_con = DBInterface.connect(DuckDB.DB)
    try
        DBInterface.execute(native_con, "CALL dbgen(sf=$sf)")
    catch
        @info "TPC-H extension not available; skipping"
        return
    end

    # convert all tables to Julia DataFrames
    customer = DataFrame(DBInterface.execute(native_con, "SELECT * FROM customer"))
    lineitem = DataFrame(DBInterface.execute(native_con, "SELECT * FROM lineitem"))
    nation = DataFrame(DBInterface.execute(native_con, "SELECT * FROM nation"))
    orders = DataFrame(DBInterface.execute(native_con, "SELECT * FROM orders"))
    part = DataFrame(DBInterface.execute(native_con, "SELECT * FROM part"))
    partsupp = DataFrame(DBInterface.execute(native_con, "SELECT * FROM partsupp"))
    region = DataFrame(DBInterface.execute(native_con, "SELECT * FROM region"))
    supplier = DataFrame(DBInterface.execute(native_con, "SELECT * FROM supplier"))

    id = Threads.threadid()
    # now open a new in-memory database, and register the dataframes there
    df_con = DBInterface.connect(DuckDB.DB)
    DuckDB.register_table(df_con, customer, "customer")
    DuckDB.register_table(df_con, lineitem, "lineitem")
    DuckDB.register_table(df_con, nation, "nation")
    DuckDB.register_table(df_con, orders, "orders")
    DuckDB.register_table(df_con, part, "part")
    DuckDB.register_table(df_con, partsupp, "partsupp")
    DuckDB.register_table(df_con, region, "region")
    DuckDB.register_table(df_con, supplier, "supplier")
    GC.gc()

    # Execute all the queries
    for _ in 1:10
        for i in 1:22

            print("T:$id | Q:$i\n")
            res = DataFrame(DBInterface.execute(df_con, "PRAGMA tpch($i)"))
        end
    end
    DBInterface.close!(df_con)
    return DBInterface.close!(native_con)
end

@testset "Test TPC-H Stresstest" begin
    test_tpch_multithread()
end
