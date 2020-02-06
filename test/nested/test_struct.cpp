#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb.hpp"

using namespace duckdb;
using namespace std;



TEST_CASE("Test filter and projection of nested struct", "[nested]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	con.Query("CREATE TABLE struct_data (g INTEGER, e INTEGER)");
	con.Query("INSERT INTO struct_data VALUES (1, 1), (1, 2), (2, 3), (2, 4), (2, 5), (3, 6), (5, NULL)");

	// all the wrong ways of holding this
	REQUIRE_FAIL(con.Query("SELECT STRUCT_PACK() FROM struct_data"));
	REQUIRE_FAIL(con.Query("SELECT STRUCT_PACK(e+1) FROM struct_data"));
	REQUIRE_FAIL(con.Query("SELECT STRUCT_PACK(a := e, a := g) FROM struct_data"));
	REQUIRE_FAIL(con.Query("SELECT STRUCT_PACK(e, e) FROM struct_data"));

	REQUIRE_FAIL(con.Query("SELECT STRUCT_EXTRACT(e, 'e') FROM struct_data"));
	REQUIRE_FAIL(con.Query("SELECT STRUCT_EXTRACT(e) FROM struct_data"));
	REQUIRE_FAIL(con.Query("SELECT STRUCT_EXTRACT('e') FROM struct_data"));
	REQUIRE_FAIL(con.Query("SELECT STRUCT_EXTRACT() FROM struct_data"));

	REQUIRE_FAIL(con.Query("SELECT STRUCT_EXTRACT(STRUCT_PACK(xx := e, yy := g), 'zz') FROM struct_data"));
	REQUIRE_FAIL(con.Query("SELECT STRUCT_EXTRACT(STRUCT_PACK(xx := e, yy := g)) FROM struct_data"));
	REQUIRE_FAIL(con.Query("SELECT STRUCT_EXTRACT(STRUCT_PACK(xx := e, yy := g), g) FROM struct_data"));
	REQUIRE_FAIL(con.Query("SELECT STRUCT_EXTRACT(STRUCT_PACK(xx := e, yy := g), '42) FROM struct_data"));


	auto result = con.Query("SELECT e, STRUCT_PACK(e) FROM struct_data ");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, 4, 5, 6, Value()}));

	result = con.Query("SELECT e, STRUCT_EXTRACT(STRUCT_PACK(xx := e, yy := g), 'xx') as ee FROM struct_data");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, 4, 5, 6, Value()}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2, 3, 4, 5, 6, Value()}));


	// TODO filter
	//result = con.Query("SELECT e, STRUCT_PACK(xx := e, yy := g) as s FROM struct_data WHERE e > 5");
//	FIXME scalars and aliases for scalars
//	result = con.Query("SELECT STRUCT_PACK(a := 42, b := 43)");
//	result->Print();

	// TODO projection

	// TODO ORDER with NULL
//	result = con.Query("SELECT e, STRUCT_EXTRACT(STRUCT_PACK(xx := e, yy := g), 'xx') as ee FROM struct_data ORDER BY e");
//	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, 4, 5, 6, Value()}));
//	REQUIRE(CHECK_COLUMN(result, 1, {1, 2, 3, 4, 5, 6, Value()}));


}


