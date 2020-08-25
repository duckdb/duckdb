#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/common/arrow.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test Arrow API round trip", "[arrow]") {
	DuckDB db(nullptr);
	Connection con(db);

	// query that creates a bunch of values across the types
	auto result = con.Query(
	    "select NULL c_null, (c % 4 = 0)::bool c_bool, (c%128)::tinyint c_tinyint, c::smallint*1000 c_smallint, "
	    "c::integer*100000 c_integer, c::bigint*1000000000000 c_bigint, c::hugeint*10000000000000000000000000000000 "
	    "c_hugeint, c::float c_float, c::double c_double, 'c_' || c::string c_string from (select case when range % 2 "
	    "== 0 then range else null end as c from range(-10000, 10000)) sq");

	DuckDBArrowTable dat;
	result->ToArrowSchema(&dat.schema);

	for (auto &chunk : result->collection.chunks) {
		ArrowArray array;
		chunk->ToArrowArray(&array);
		dat.chunks.push_back(array);
	}

	auto result2 = con.TableFunction("arrow_scan", {Value::POINTER((uintptr_t)&dat)})->Execute();
	REQUIRE(!result->Equals(*result2));
}
// TODO timestamp date time interval decimal
