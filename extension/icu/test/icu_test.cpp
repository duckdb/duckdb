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

	// we can also combine this collation with NOCASE
	result = con.Query("SELECT * FROM strings WHERE 'goethe' > s COLLATE de.NOCASE ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {"Gabel", "Göbel"}));
	result = con.Query("SELECT * FROM strings WHERE 'goethe' > s COLLATE NOCASE.de ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {"Gabel", "Göbel"}));
	// but not with NOACCENT
	REQUIRE_FAIL(con.Query("SELECT * FROM strings WHERE 'goethe' > s COLLATE NOACCENT.de ORDER BY 1"));

	// japanese collation
	REQUIRE_NO_FAIL(con.Query("DELETE FROM strings"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO strings VALUES ('賃貸人側連絡先 (Lessor side contact)'), ('賃借人側連絡先 (Lessee side contact)'), ('解約連絡先 (Termination contacts)'), ('更新連絡先 (Update contact)')"));

	result = con.Query("SELECT * FROM strings ORDER BY s");
	REQUIRE(CHECK_COLUMN(result, 0, {"更新連絡先 (Update contact)", "解約連絡先 (Termination contacts)", "賃借人側連絡先 (Lessee side contact)", "賃貸人側連絡先 (Lessor side contact)"}));
	result = con.Query("SELECT * FROM strings ORDER BY s COLLATE ja.NOCASE");
	REQUIRE(CHECK_COLUMN(result, 0, {"解約連絡先 (Termination contacts)", "更新連絡先 (Update contact)", "賃借人側連絡先 (Lessee side contact)", "賃貸人側連絡先 (Lessor side contact)"}));
}
