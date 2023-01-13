# test_tpch_multithread.jl

# DuckDB needs to have been built with TPCH (BUILD_TPCH=1) to run this test!

@testset "Test TPC-H Multithreaded" begin
    sf = "0.1"

    # load TPC-H into DuckDB
    native_con = DBInterface.connect(DuckDB.DB)
    #df_con = DBInterface.connect(DuckDB.DB)
    DBInterface.execute(native_con, "CALL dbgen(sf=$sf)")

    # convert all tables to Julia DataFrames
    customer = DataFrame(DBInterface.execute(native_con, "SELECT * FROM customer"))
    lineitem = DataFrame(DBInterface.execute(native_con, "SELECT * FROM lineitem"))
    nation = DataFrame(DBInterface.execute(native_con, "SELECT * FROM nation"))
    orders = DataFrame(DBInterface.execute(native_con, "SELECT * FROM orders"))
    part = DataFrame(DBInterface.execute(native_con, "SELECT * FROM part"))
    partsupp = DataFrame(DBInterface.execute(native_con, "SELECT * FROM partsupp"))
    region = DataFrame(DBInterface.execute(native_con, "SELECT * FROM region"))
    supplier = DataFrame(DBInterface.execute(native_con, "SELECT * FROM supplier"))

	Threads.@threads for _ in 1:Threads.nthreads()
			#db = DuckDB.DB()
			id = Threads.threadid()
			# now open a new in-memory database, and register the dataframes there
			df_con = DBInterface.connect(DuckDB.DB)
			#DBInterface.execute(df_con, "SET external_threads=1");
			DuckDB.register_data_frame(df_con, customer, "customer")
			DuckDB.register_data_frame(df_con, lineitem, "lineitem")
			DuckDB.register_data_frame(df_con, nation, "nation")
			DuckDB.register_data_frame(df_con, orders, "orders")
			DuckDB.register_data_frame(df_con, part, "part")
			DuckDB.register_data_frame(df_con, partsupp, "partsupp")
			DuckDB.register_data_frame(df_con, region, "region")
			DuckDB.register_data_frame(df_con, supplier, "supplier")
			GC.gc()

			#throw(UndefVarError(:df_con))
			# Execute all the queries
			for _ in 1:100
				for i in 1:22

					print("T:$id | Q:$i\n")
					res = DataFrame(DBInterface.execute(df_con, "PRAGMA tpch($i)"))
				end
			end
    		DBInterface.close!(df_con)
       end

    DBInterface.close!(native_con)
end
