#include "benchmark_runner.hpp"
#include "duckdb_benchmark_macro.hpp"

using namespace duckdb;

#define SSB_QUERY(QUERY, SF, THREADS)                                                                                  \
	string db_path = "duckdb_benchmark_db.db";                                                                         \
	void Load(DuckDBBenchmarkState *state) override {                                                                  \
		DeleteDatabase(db_path);                                                                                       \
		{                                                                                                              \
			DuckDB db(db_path);                                                                                        \
			Connection con(db);                                                                                        \
			con.Query("CALL ssbgen(sf=" + std::to_string(SF) + ")");                                                   \
		}                                                                                                              \
		{                                                                                                              \
			auto config = GetConfig();                                                                                 \
			config->options.checkpoint_wal_size = 0;                                                                   \
			DuckDB db(db_path, config.get());                                                                          \
		}                                                                                                              \
	}                                                                                                                  \
	void RunBenchmark(DuckDBBenchmarkState *state) override {                                                          \
		auto config = GetConfig();                                                                                     \
		DuckDB db(db_path, config.get());                                                                              \
		Connection con(db);                                                                                            \
		state->result = con.Query(QUERY);                                                                              \
	}                                                                                                                  \
	string BenchmarkInfo() override {                                                                                  \
		return string("Start a SSB SF") + std::to_string(SF) + string(" database and run ") + QUERY +                  \
		       string(" in the database");                                                                             \
	}                                                                                                                  \
	string VerifyResult(QueryResult *result) override {                                                                \
		return "";                                                                                                     \
	}

#define NORMAL_CONFIG                                                                                                  \
	duckdb::unique_ptr<DBConfig> GetConfig() {                                                                         \
		return make_uniq<DBConfig>();                                                                                  \
	}

#define SSB_BENCHMARK_GROUP(SF, THREAD)                                                                                \
	DUCKDB_BENCHMARK(SSBQ1v1S##SF##T##THREAD, "[ssb]")                                                                 \
	SSB_QUERY(                                                                                                         \
	    "SELECT SUM(lo_extendedprice * lo_discount) AS revenue FROM lineorder, date WHERE lo_orderdate = d_datekey "   \
	    "AND d_year = 1994 AND lo_discount BETWEEN 1 AND 3 AND lo_quantity < 25;",                                     \
	    SF, THREAD)                                                                                                    \
	NORMAL_CONFIG                                                                                                      \
	FINISH_BENCHMARK(SSBQ1v1S##SF##T##THREAD)                                                                          \
                                                                                                                       \
	DUCKDB_BENCHMARK(SSBQ1v2S##SF##T##THREAD, "[ssb]")                                                                 \
	SSB_QUERY("SELECT SUM(lo_extendedprice * lo_discount) AS revenue FROM lineorder, date WHERE lo_orderdate = "       \
	          "d_datekey AND d_yearmonthnum = 199401 AND lo_discount BETWEEN 4 AND 6 AND lo_quantity BETWEEN "         \
	          "26 AND 35;",                                                                                            \
	          SF, THREAD)                                                                                              \
	NORMAL_CONFIG                                                                                                      \
	FINISH_BENCHMARK(SSBQ1v2S##SF##T##THREAD)                                                                          \
                                                                                                                       \
	DUCKDB_BENCHMARK(SSBQ1v3S##SF##T##THREAD, "[ssb]")                                                                 \
	SSB_QUERY(                                                                                                         \
	    "SELECT SUM(lo_extendedprice * lo_discount) AS revenue FROM lineorder, date WHERE lo_orderdate = d_datekey "   \
	    "AND "                                                                                                         \
	    "d_weeknuminyear = 6 AND d_year = 1994 AND lo_discount BETWEEN 5 AND 7 AND lo_quantity BETWEEN 36 AND 40;",    \
	    SF, THREAD)                                                                                                    \
	NORMAL_CONFIG                                                                                                      \
	FINISH_BENCHMARK(SSBQ1v3S##SF##T##THREAD)                                                                          \
                                                                                                                       \
	DUCKDB_BENCHMARK(SSBQ2v1S##SF##T##THREAD, "[ssb]")                                                                 \
	SSB_QUERY(                                                                                                         \
	    "SELECT SUM(lo_revenue), d_year, p_brand1 FROM lineorder, date, part, supplier WHERE lo_orderdate = "          \
	    "d_datekey AND lo_partkey = p_partkey AND lo_suppkey = s_suppkey AND p_category = 'MFGR#12' AND s_region = "   \
	    "'AMERICA' GROUP BY d_year, p_brand1 ORDER BY d_year, p_brand1;",                                              \
	    SF, THREAD)                                                                                                    \
	NORMAL_CONFIG                                                                                                      \
	FINISH_BENCHMARK(SSBQ2v1S##SF##T##THREAD)                                                                          \
                                                                                                                       \
	DUCKDB_BENCHMARK(SSBQ2v2S##SF##T##THREAD, "[ssb]")                                                                 \
	SSB_QUERY("SELECT SUM(lo_revenue), d_year, p_brand1 FROM lineorder, date, part, supplier WHERE lo_orderdate = "    \
	          "d_datekey AND lo_partkey = p_partkey AND lo_suppkey = s_suppkey AND p_brand1 BETWEEN 'MFGR#2221' AND "  \
	          "'MFGR#2228' AND s_region = 'ASIA' GROUP BY d_year, p_brand1 ORDER BY d_year, p_brand1;",                \
	          SF, THREAD)                                                                                              \
	NORMAL_CONFIG                                                                                                      \
	FINISH_BENCHMARK(SSBQ2v2S##SF##T##THREAD)                                                                          \
                                                                                                                       \
	DUCKDB_BENCHMARK(SSBQ2v3S##SF##T##THREAD, "[ssb]")                                                                 \
	SSB_QUERY(                                                                                                         \
	    "SELECT SUM(lo_revenue), d_year, p_brand1 FROM lineorder, date, part, supplier WHERE lo_orderdate = "          \
	    "d_datekey AND lo_partkey = p_partkey AND lo_suppkey = s_suppkey AND p_brand1 = 'MFGR#2221' AND s_region = "   \
	    "'EUROPE' GROUP BY d_year, p_brand1 ORDER BY d_year, p_brand1;",                                               \
	    SF, THREAD)                                                                                                    \
	NORMAL_CONFIG                                                                                                      \
	FINISH_BENCHMARK(SSBQ2v3S##SF##T##THREAD)                                                                          \
                                                                                                                       \
	DUCKDB_BENCHMARK(SSBQ3v1S##SF##T##THREAD, "[ssb]")                                                                 \
	SSB_QUERY(                                                                                                         \
	    "SELECT c_nation, s_nation, d_year, SUM(lo_revenue) AS revenue FROM customer, lineorder, supplier, date "      \
	    "WHERE lo_custkey = c_custkey AND lo_suppkey = s_suppkey AND lo_orderdate = d_datekey AND c_region = 'ASIA' "  \
	    "AND s_region = 'ASIA' AND d_year >= 1992 and d_year <= 1997 GROUP BY c_nation, s_nation, d_year ORDER BY "    \
	    "d_year ASC, revenue DESC;",                                                                                   \
	    SF, THREAD)                                                                                                    \
	NORMAL_CONFIG                                                                                                      \
	FINISH_BENCHMARK(SSBQ3v1S##SF##T##THREAD)                                                                          \
                                                                                                                       \
	DUCKDB_BENCHMARK(SSBQ3v2S##SF##T##THREAD, "[ssb]")                                                                 \
	SSB_QUERY(                                                                                                         \
	    "SELECT c_city, s_city, d_year, SUM(lo_revenue) AS revenue FROM customer, lineorder, supplier, date WHERE "    \
	    "lo_custkey = c_custkey AND lo_suppkey = s_suppkey AND lo_orderdate = d_datekey AND c_nation = 'UNITED "       \
	    "STATES' AND s_nation = 'UNITED STATES' AND d_year >= 1992 and d_year <= 1997 GROUP BY c_city, s_city, "       \
	    "d_year ORDER BY d_year ASC, revenue DESC;",                                                                   \
	    SF, THREAD)                                                                                                    \
	NORMAL_CONFIG                                                                                                      \
	FINISH_BENCHMARK(SSBQ3v2S##SF##T##THREAD)                                                                          \
                                                                                                                       \
	DUCKDB_BENCHMARK(SSBQ3v3S##SF##T##THREAD, "[ssb]")                                                                 \
	SSB_QUERY(                                                                                                         \
	    "SELECT c_city, s_city, d_year, SUM(lo_revenue) AS revenue FROM customer, lineorder, supplier, date WHERE "    \
	    "lo_custkey = c_custkey AND lo_suppkey = s_suppkey AND lo_orderdate = d_datekey AND (c_city = 'UNITED KI1' "   \
	    "OR c_city = 'UNITED KI6') AND (s_city = 'UNITED KI1' OR s_city = 'UNITED KI5') AND d_year >= 1992 AND "       \
	    "d_year <= 1997 GROUP BY c_city, s_city, d_year ORDER BY d_year ASC, revenue DESC;",                           \
	    SF, THREAD)                                                                                                    \
	NORMAL_CONFIG                                                                                                      \
	FINISH_BENCHMARK(SSBQ3v3S##SF##T##THREAD)                                                                          \
                                                                                                                       \
	DUCKDB_BENCHMARK(SSBQ3v4S##SF##T##THREAD, "[ssb]")                                                                 \
	SSB_QUERY(                                                                                                         \
	    "SELECT c_city, s_city, d_year, SUM(lo_revenue) AS revenue FROM customer, lineorder, supplier, date WHERE "    \
	    "lo_custkey = c_custkey AND lo_suppkey = s_suppkey AND lo_orderdate = d_datekey AND (c_city = 'UNITED KI1' "   \
	    "OR c_city = 'UNITED KI6') AND (s_city = 'UNITED KI1' OR s_city = 'UNITED KI5') AND d_yearmonth = 'Dec1997' "  \
	    "GROUP BY c_city, s_city, d_year ORDER BY d_year ASC, revenue DESC;",                                          \
	    SF, THREAD)                                                                                                    \
	NORMAL_CONFIG                                                                                                      \
	FINISH_BENCHMARK(SSBQ3v4S##SF##T##THREAD)                                                                          \
                                                                                                                       \
	DUCKDB_BENCHMARK(SSBQ4v1S##SF##T##THREAD, "[ssb]")                                                                 \
	SSB_QUERY(                                                                                                         \
	    "SELECT d_year, c_nation, SUM(lo_revenue - lo_supplycost) AS profit FROM date, customer, supplier, part, "     \
	    "lineorder WHERE lo_custkey = c_custkey AND lo_suppkey = s_suppkey AND lo_partkey = p_partkey AND "            \
	    "lo_orderdate = d_datekey AND c_region = 'AMERICA' AND s_region = 'AMERICA' AND (p_mfgr = 'MFGR#1' OR p_mfgr " \
	    "= 'MFGR#2') GROUP BY d_year, c_nation ORDER BY d_year, c_nation;",                                            \
	    SF, THREAD)                                                                                                    \
	NORMAL_CONFIG                                                                                                      \
	FINISH_BENCHMARK(SSBQ4v1S##SF##T##THREAD)                                                                          \
                                                                                                                       \
	DUCKDB_BENCHMARK(SSBQ4v2S##SF##T##THREAD, "[ssb]")                                                                 \
	SSB_QUERY(                                                                                                         \
	    "SELECT d_year, s_nation, p_category, SUM(lo_revenue - lo_supplycost) AS profit FROM date, customer, "         \
	    "supplier, part, lineorder WHERE lo_custkey = c_custkey AND lo_suppkey = s_suppkey AND lo_partkey = "          \
	    "p_partkey AND lo_orderdate = d_datekey AND c_region = 'AMERICA' AND s_region = 'AMERICA' AND (d_year = 1997 " \
	    "OR d_year = 1999) AND (p_mfgr = 'MFGR#1' OR p_mfgr = 'MFGR#2') GROUP BY d_year, s_nation, p_category ORDER "  \
	    "BY d_year, s_nation, p_category;",                                                                            \
	    SF, THREAD)                                                                                                    \
	NORMAL_CONFIG                                                                                                      \
	FINISH_BENCHMARK(SSBQ4v2S##SF##T##THREAD)                                                                          \
                                                                                                                       \
	DUCKDB_BENCHMARK(SSBQ4v3S##SF##T##THREAD, "[ssb]")                                                                 \
	SSB_QUERY(                                                                                                         \
	    "SELECT d_year, s_city, p_brand2, SUM(lo_revenue - lo_supplycost) AS profit FROM date, customer, supplier, "   \
	    "part, "                                                                                                       \
	    "lineorder WHERE lo_custkey = c_custkey AND lo_suppkey = s_suppkey AND lo_partkey = p_partkey AND "            \
	    "lo_orderdate = "                                                                                              \
	    "d_datekey AND c_region = 'AMERICA' AND s_nation = 'UNITED STATES' AND (d_year = 1997 OR d_year = 1998) AND "  \
	    "p_category = 'MFGR#15' GROUP BY d_year, s_city, p_brand1 ORDER BY d_year, s_city, p_brand1;",                 \
	    SF, THREAD)                                                                                                    \
	NORMAL_CONFIG                                                                                                      \
	FINISH_BENCHMARK(SSBQ4v3S##SF##T##THREAD)

/* Benchmarks for task 1 - Varying on the thread count but keep the scale factor(sf) steady */
SSB_BENCHMARK_GROUP(10, 1)
SSB_BENCHMARK_GROUP(10, 8)

/* Benchmarks for task 2 - Varying on the scale factor(sf) but keep the thread count steady */
SSB_BENCHMARK_GROUP(1, 4)
SSB_BENCHMARK_GROUP(10, 4)
SSB_BENCHMARK_GROUP(100, 4)