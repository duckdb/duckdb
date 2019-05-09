#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Unicode schema", "[catalog]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// create schema
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE 👤(🔑 INTEGER PRIMARY KEY, 🗣 varchar(64), 🗓 DATE);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE ✍(🔑 INTEGER PRIMARY KEY, 🗣 varchar(64));"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE 📕(🔑 INTEGER PRIMARY KEY, 💬 varchar(64), 🔖 varchar(64), ✍ INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE 👤🏠📕(👤 INTEGER, 📕 INTEGER, ⭐ TEXT);"));
	// insert data
	REQUIRE_NO_FAIL(con.Query("INSERT INTO 👤 VALUES (1, 'Jeff', '2019-01-01'), (2, 'Annie', '2019-01-01');"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO ✍ VALUES (1, 'Herman Melville'), (2, 'Lewis Carroll');"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO 📕 VALUES (1, 'Alice in Wonderland', '🔮', 2), (2, 'Moby Dick', '📖', 1), (3, "
	                          "'Through the Looking-Glass', '🔮', 2);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO 👤🏠📕 VALUES (1, 1, '😍'), (1, 2, '🤢'), (2, 2, '🙂');"));

	result = con.Query(
	    "SELECT 👤.🗣 AS 👤, 📕.💬 AS 📕 FROM 👤 JOIN 👤🏠📕 ON 👤.🔑 = 👤🏠📕.👤 JOIN "
	    "📕 "
	    "ON "
	    "📕.🔑 "
	    "= "
	    "👤🏠📕.📕 "
	    "ORDER "
	    "BY "
	    "👤, "
	    "📕;");
	REQUIRE(CHECK_COLUMN(result, 0, {"Annie", "Jeff", "Jeff"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"Moby Dick", "Alice in Wonderland", "Moby Dick"}));

	result = con.Query(
	    "SELECT 👤.🗣, 👤🏠📕.⭐ FROM 👤🏠📕 JOIN 📕 ON 👤🏠📕.📕 = 📕.🔑 JOIN 👤 ON "
	    "👤🏠📕.👤=👤.🔑 "
	    "WHERE "
	    "📕.💬 "
	    "= "
	    "'Moby "
	    "Dick' ORDER BY 👤.🗣;");
	REQUIRE(CHECK_COLUMN(result, 0, {"Annie", "Jeff"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"🙂", "🤢"}));

	result = con.Query("SELECT type, name FROM sqlite_master() WHERE name='👤' ORDER BY name;");
	REQUIRE(CHECK_COLUMN(result, 0, {"table"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"👤"}));
}
