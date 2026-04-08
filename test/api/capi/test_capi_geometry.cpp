#include "capi_tester.hpp"
#include "duckdb/common/arrow/schema_metadata.hpp"

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

TEST_CASE("Test C API GEOMETRY arrow schema export with CRS", "[capi][arrow]") {
	// Regression test: exporting a GEOMETRY column with a CRS to an Arrow schema
	// used to assert in TransactionContext::ActiveTransaction(), because
	// ArrowGeometry::PopulateSchema resolves the CRS via the catalog and the
	// auto-commit transaction from the previous statement has already closed by
	// the time duckdb_query_arrow_schema runs.
	CAPITester tester;
	REQUIRE(tester.OpenDatabase(nullptr));
	REQUIRE_NO_FAIL(tester.Query("CREATE TABLE t1 (data GEOMETRY('OGC:CRS84'))"));
	REQUIRE_NO_FAIL(tester.Query("INSERT INTO t1 VALUES ('POINT(42 1337)')"));

	duckdb_arrow arrow_result = nullptr;
	auto state = duckdb_query_arrow(tester.connection, "SELECT data FROM t1", &arrow_result);
	REQUIRE(state == DuckDBSuccess);

	ArrowSchema arrow_schema;
	arrow_schema.Init();
	auto arrow_schema_ptr = &arrow_schema;
	state = duckdb_query_arrow_schema(arrow_result, reinterpret_cast<duckdb_arrow_schema *>(&arrow_schema_ptr));
	std::string err;
	if (state != DuckDBSuccess) {
		auto err_cstr = duckdb_query_arrow_error(arrow_result);
		err = err_cstr ? err_cstr : "(no error message)";
		duckdb_destroy_arrow(&arrow_result);
	}
	INFO("duckdb_query_arrow_schema error: " << err);
	REQUIRE(state == DuckDBSuccess);
	REQUIRE(arrow_schema.n_children == 1);

	// The geoarrow.wkb extension name should appear in the child schema metadata.
	auto child = arrow_schema.children[0];
	REQUIRE(child != nullptr);
	REQUIRE(child->metadata != nullptr);
	auto md = ArrowSchemaMetadata(child->metadata);
	REQUIRE(md.GetOption(ArrowSchemaMetadata::ARROW_EXTENSION_NAME) == "geoarrow.wkb");

	if (arrow_schema.release) {
		arrow_schema.release(arrow_schema_ptr);
	}
	duckdb_destroy_arrow(&arrow_result);
}
