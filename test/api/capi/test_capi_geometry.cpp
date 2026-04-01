#include "capi_tester.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test C API GEOMETRY type support", "[capi]") {
	CAPITester tester;
	duckdb::unique_ptr<CAPIResult> result;

	REQUIRE(tester.OpenDatabase(nullptr));
	REQUIRE_NO_FAIL(tester.Query("CREATE TABLE t1 (data GEOMETRY('OGC:CRS84'))"));
	REQUIRE_NO_FAIL(tester.Query("INSERT INTO t1 VALUES ('POINT(42 1337)')"));

	result = tester.Query("SELECT data FROM t1");
	REQUIRE_NO_FAIL(*result);

	REQUIRE(result->ColumnType(0) == DUCKDB_TYPE_GEOMETRY);

	auto chunk = result->FetchChunk(0);
	REQUIRE(chunk);

	auto vec = duckdb_data_chunk_get_vector(chunk->GetChunk(), 0);

	auto type = duckdb_vector_get_column_type(vec);

	REQUIRE(duckdb_get_type_id(type) == DUCKDB_TYPE_GEOMETRY);
	auto crs = duckdb_geometry_type_get_crs(type);
	REQUIRE(crs);
	REQUIRE(strcmp(crs, "OGC:CRS84") == 0);
	duckdb_free(crs);

	duckdb_destroy_logical_type(&type);

	auto blob = *static_cast<duckdb_string_t *>(duckdb_vector_get_data(vec));

	REQUIRE(duckdb_string_t_length(blob) == 21);

	auto data = duckdb_string_t_data(&blob);

	uint8_t le = 0;
	uint32_t meta = 0;
	double x;
	double y;

	memcpy(&le, data, 1);
	memcpy(&meta, data + 1, 4);
	memcpy(&x, data + 5, 8);
	memcpy(&y, data + 13, 8);

	REQUIRE(le == 1);
	REQUIRE(meta == 0x00000001); // WKB for POINT
	REQUIRE(x == 42);
	REQUIRE(y == 1337);
}
