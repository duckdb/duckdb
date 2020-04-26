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
	unique_ptr<QueryResult> result;

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE strings(s VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO strings VALUES ('Gabel'), ('Göbel'), ('Goethe'), ('Goldmann'), ('Göthe'), ('Götz')"));

	// ordering
	result = con.Query("SELECT * FROM strings ORDER BY s COLLATE de");
	REQUIRE(CHECK_COLUMN(result, 0, {"Gabel", "Göbel", "Goethe", "Goldmann", "Göthe", "Götz"}));

	// range filter
	result = con.Query("SELECT * FROM strings WHERE 'Goethe' > s COLLATE de ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {"Gabel", "Göbel"}));
	// default binary collation, Göbel is not smaller than Gabel in UTF8 encoding
	result = con.Query("SELECT * FROM strings WHERE 'Goethe' > s ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {"Gabel"}));
}
