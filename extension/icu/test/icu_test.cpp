#define CATCH_CONFIG_MAIN
#include "catch.hpp"
#include "icu-extension.hpp"
#include "duckdb.hpp"

using namespace duckdb;

TEST_CASE("Test basic ICU extension usage", "[icu]") {
	DuckDB db;
	db.LoadExtension<ICUExtension>();
	Connection con(db);


}
