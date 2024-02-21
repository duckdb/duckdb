#include "catch.hpp"
#include "test_helpers.hpp"

#include <thread>

using namespace duckdb;
using namespace std;

TEST_CASE("Test database maximum_threads argument", "[api]") {
	// default is number of hw threads
	// FIXME: not yet
	{
		DuckDB db(nullptr);
		REQUIRE(db.NumberOfThreads() == std::thread::hardware_concurrency());
	}
	// but we can set another value
	{
		DBConfig config;
		config.options.maximum_threads = 10;
		DuckDB db(nullptr, &config);
		REQUIRE(db.NumberOfThreads() == 10);
	}
	// zero is not erlaubt
	{
		DBConfig config;
		config.options.maximum_threads = 0;
		DuckDB db;
		REQUIRE_THROWS(db = DuckDB(nullptr, &config));
	}
}

TEST_CASE("Test external threads", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &config = DBConfig::GetConfig(*db.instance);
	auto options = config.GetOptions();

	con.Query("SET threads=13");
	REQUIRE(config.options.maximum_threads == 13);
	REQUIRE(db.NumberOfThreads() == 13);
	con.Query("SET external_threads=13");
	REQUIRE(config.options.external_threads == 13);
	REQUIRE(db.NumberOfThreads() == 13);

	con.Query("SET external_threads=0");
	REQUIRE(config.options.external_threads == 0);
	REQUIRE(db.NumberOfThreads() == 13);

	auto res = con.Query("SET external_threads=-1");
	REQUIRE(res->HasError());
	REQUIRE(res->GetError() == "Syntax Error: Must have a non-negative number of external threads!");

	res = con.Query("SET external_threads=14");
	REQUIRE(res->HasError());
	REQUIRE(res->GetError() == "Syntax Error: Number of threads can't be smaller than number of external threads!");

	con.Query("SET external_threads=5");
	REQUIRE(config.options.external_threads == 5);
	REQUIRE(db.NumberOfThreads() == 13);

	con.Query("RESET external_threads");
	REQUIRE(config.options.external_threads == DBConfig().options.external_threads);
	REQUIRE(db.NumberOfThreads() == 13);

	con.Query("RESET threads");
	REQUIRE(config.options.maximum_threads == std::thread::hardware_concurrency());
	REQUIRE(db.NumberOfThreads() == std::thread::hardware_concurrency());
}
