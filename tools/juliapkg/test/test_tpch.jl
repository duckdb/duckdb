# test_tpch.jl

@testset "Test TPC-H" begin
	sf = "0.01"

	# load TPC-H into DuckDB
    tpch_con = DBInterface.connect(DuckDB.DB)
	DBInterface.execute(tpch_con, "CALL dbgen(sf=$sf)")

	# convert all tables to Julia DataFrames
	customer = DataFrame(DBInterface.execute(tpch_con, "SELECT * FROM customer"))
	lineitem = DataFrame(DBInterface.execute(tpch_con, "SELECT * FROM lineitem"))
	nation   = DataFrame(DBInterface.execute(tpch_con, "SELECT * FROM nation"))
	orders   = DataFrame(DBInterface.execute(tpch_con, "SELECT * FROM orders"))
	part     = DataFrame(DBInterface.execute(tpch_con, "SELECT * FROM part"))
	partsupp = DataFrame(DBInterface.execute(tpch_con, "SELECT * FROM partsupp"))
	region   = DataFrame(DBInterface.execute(tpch_con, "SELECT * FROM region"))
	supplier = DataFrame(DBInterface.execute(tpch_con, "SELECT * FROM supplier"))

	# now open a new in-memory database, and register the dataframes there
    con = DBInterface.connect(DuckDB.DB)
	DuckDB.register_data_frame(con, customer, "customer")
	DuckDB.register_data_frame(con, lineitem, "lineitem")
	DuckDB.register_data_frame(con, nation, "nation")
	DuckDB.register_data_frame(con, orders, "orders")
	DuckDB.register_data_frame(con, part, "part")
	DuckDB.register_data_frame(con, partsupp, "partsupp")
	DuckDB.register_data_frame(con, region, "region")
	DuckDB.register_data_frame(con, supplier, "supplier")
    GC.gc()

	# run all the queries
	for i in 1:22
		# print("Q$i\n")
		# for each query, compare the results of the query ran on the original tables
		# versus the result when run on the Julia DataFrames
		res = DataFrame(DBInterface.execute(con, "PRAGMA tpch($i)"))
		res2 = DataFrame(DBInterface.execute(tpch_con, "PRAGMA tpch($i)"))
		@test isequal(res, res2)
# 		@time begin
# 			results = DBInterface.execute(con, "PRAGMA tpch($i)")
# 		end
	end

    DBInterface.close!(con)
    DBInterface.close!(tpch_con)
end
