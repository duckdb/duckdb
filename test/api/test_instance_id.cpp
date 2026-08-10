#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/common/set.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/main/database.hpp"

using namespace duckdb;

TEST_CASE("Test that every DatabaseInstance has its own id", "[api]") {
	SECTION("the id is a well-formed UUID and is stable for the instance") {
		DuckDB db(nullptr);
		auto &id = db.instance->GetInstanceId();
		REQUIRE(id.size() == UUID::STRING_SIZE);
		// asking again returns the same id
		REQUIRE(id == db.instance->GetInstanceId());
	}
	SECTION("two instances in the same process do not share an id") {
		// this is the case a process identifier cannot distinguish
		DuckDB db1(nullptr);
		DuckDB db2(nullptr);
		REQUIRE(db1.instance->GetInstanceId() != db2.instance->GetInstanceId());
	}
	SECTION("many instances are all distinct") {
		// UUID::GenerateUniqueUUID counts within the process, so this holds regardless of the entropy source
		set<string> ids;
		for (idx_t i = 0; i < 32; i++) {
			DuckDB db(nullptr);
			ids.insert(db.instance->GetInstanceId());
		}
		REQUIRE(ids.size() == 32);
	}
	SECTION("seeding the session RNG does not make instances share an id") {
		// the id is drawn from its own RandomEngine, so it must not be reproducible from a seeded
		// session - otherwise two instances could name the same resources
		DuckDB db1(nullptr);
		Connection con1(db1);
		REQUIRE_NO_FAIL(con1.Query("SELECT setseed(0.42)"));
		DuckDB db2(nullptr);
		Connection con2(db2);
		REQUIRE_NO_FAIL(con2.Query("SELECT setseed(0.42)"));
		REQUIRE(db1.instance->GetInstanceId() != db2.instance->GetInstanceId());
	}
}
