#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/main/secret/secret_storage.hpp"
#include "duckdb/main/secret/secret.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("A lot of unions", "[optimizer][.]") {
	DuckDB db(nullptr);
	Connection con(db);

	duckdb::stringstream q;
	q << "select 1 as num";

	int max = 0;
	int curr = 1;
	while (max < 500) {
		max = (max * 1.1) + 1;
		while (curr < max) {
			q << " union all select 1 as num";
			curr++;
		}
		REQUIRE_NO_FAIL(con.Query(q.str()));
	}
}
