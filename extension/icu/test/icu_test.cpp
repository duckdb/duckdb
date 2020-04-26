#define CATCH_CONFIG_MAIN
#include "catch.hpp"
#include "icu-extension.hpp"
#include "duckdb.hpp"
#include "test_helpers.hpp"

using namespace duckdb;

TEST_CASE("Test basic ICU extension usage", "[icu]") {
	DuckDB db;
	db.LoadExtension<ICUExtension>();
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE strings(s VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO strings VALUES ('Gabel'), ('Göbel'), ('Goethe'), ('Goldmann'), ('Göthe'), ('Götz')"));

	auto result = con.Query("SELECT * FROM strings ORDER BY s COLLATE de");
	REQUIRE(CHECK_COLUMN(result, 0, {"Gabel", "Göbel", "Goethe", "Goldmann", "Göthe", "Götz"}));
}
