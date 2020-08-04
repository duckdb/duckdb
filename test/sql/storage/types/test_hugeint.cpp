#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/common/types/hugeint.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test HUGEINT with persistent storage", "[hugeint]") {
	auto config = GetTestConfig();
	unique_ptr<QueryResult> result;
	auto storage_database = TestCreatePath("hugeint_storage_test");

	// make sure the database does not exist
	DeleteDatabase(storage_database);
	{
		// create a database and insert values
		DuckDB db(storage_database, config.get());
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE hugeints (h HUGEINT);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO hugeints VALUES (1043178439874412422424), (42), (NULL), (47289478944894789472897441242)"));
	}
	// reload the database from disk a few times
	for (idx_t i = 0; i < 2; i++) {
		DuckDB db(storage_database, config.get());
		Connection con(db);
		result = con.Query("SELECT * FROM hugeints");
		REQUIRE(CHECK_COLUMN(result, 0, {Value::HUGEINT(Hugeint::FromString("1043178439874412422424")), 42, Value(),  Value::HUGEINT(Hugeint::FromString("47289478944894789472897441242"))}));
	}
	DeleteDatabase(storage_database);
}
