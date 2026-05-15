#include "catch.hpp"

#include "arrow/arrow_test_helper.hpp"
#include "duckdb/common/adbc/single_batch_array_stream.hpp"

using namespace duckdb;

namespace {

// Stack-allocated structs need a no-op release.
void NoopReleaseSchema(ArrowSchema *s) {
	s->release = nullptr;
}
void NoopReleaseArray(ArrowArray *a) {
	a->release = nullptr;
}

} // namespace

// Regression for the off-by-one in ColumnArrowToDuckDBDictionary: the base
// vector is allocated with capacity dict_length+1 (for the NULL-sentinel slot),
// but the inner ColumnArrowToDuckDB call goes through DirectConversion ->
// FlatVector::SetData, which replaces the buffer with one whose capacity
// equals dict_length. The follow-up FlatVector::SetSize(dict_length + 1) then
// trips:
//   "Vector::SetSize out of range - trying to set size to 4 for vector with capacity 3"
//
// We hand-build a minimal dict-encoded int32 ArrowArray (which DuckDB itself
// never produces — its appender only dict-encodes ENUM/VARCHAR), wrap it in
// an arrow stream, and scan it through arrow_scan. That is the same path the
// failing duckdb-python tests in test_dictionary_arrow.py take.
TEST_CASE("Arrow scan of dict-encoded int32 column with NULL sentinel", "[arrow]") {
	// indices: int32 with a dictionary of int32 values
	ArrowSchema dict_schema {};
	dict_schema.format = "i";
	dict_schema.flags = 2; // ARROW_FLAG_NULLABLE
	dict_schema.release = NoopReleaseSchema;

	ArrowSchema col_schema {};
	col_schema.format = "i";
	col_schema.name = "a";
	col_schema.flags = 2;
	col_schema.dictionary = &dict_schema;
	col_schema.release = NoopReleaseSchema;

	ArrowSchema *col_children_schemas[1] = {&col_schema};
	ArrowSchema record_schema {};
	record_schema.format = "+s";
	record_schema.n_children = 1;
	record_schema.children = col_children_schemas;
	record_schema.release = NoopReleaseSchema;

	int32_t dict_values[] = {10, 20, 30};
	const void *dict_array_buffers[2] = {nullptr, dict_values};
	ArrowArray dict_array {};
	dict_array.length = 3;
	dict_array.n_buffers = 2;
	dict_array.buffers = dict_array_buffers;
	dict_array.release = NoopReleaseArray;

	int32_t indices[] = {0, 1, 2, 0, 1, 2, 0, 1};
	const void *col_array_buffers[2] = {nullptr, indices};
	ArrowArray col_array {};
	col_array.length = 8;
	col_array.n_buffers = 2;
	col_array.buffers = col_array_buffers;
	col_array.dictionary = &dict_array;
	col_array.release = NoopReleaseArray;

	ArrowArray *col_children_arrays[1] = {&col_array};
	const void *record_array_buffers[1] = {nullptr};
	ArrowArray record_array {};
	record_array.length = 8;
	record_array.n_buffers = 1;
	record_array.buffers = record_array_buffers;
	record_array.n_children = 1;
	record_array.children = col_children_arrays;
	record_array.release = NoopReleaseArray;

	// Wrap (schema, array) as an ArrowArrayStream and compare with a SQL
	// SELECT that produces the same materialized result.
	ArrowArrayStream stream {};
	AdbcError err {};
	REQUIRE(duckdb_adbc::BatchToArrayStream(&record_array, &record_schema, &stream, &err) == ADBC_STATUS_OK);

	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE(ArrowTestHelper::RunArrowComparison(
	    con, "SELECT * FROM (VALUES (10), (20), (30), (10), (20), (30), (10), (20)) t(a)", stream));
}
