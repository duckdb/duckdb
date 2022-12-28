#ifdef _WIN32
#include <windows.h>
#endif
#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test compatibility with windows.h", "[windows]") {
	DuckDB db(nullptr);
	Connection con(db);

	// This test solely exists to check if compilation is hindered by including windows.h
	// before including duckdb.hpp
	con.BeginTransaction();
	con.Query("select 42;");
	con.Commit();
}
