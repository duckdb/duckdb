#include "catch.hpp"

#include "arrow/arrow_test_helper.hpp"
#include "duckdb/common/adbc/single_batch_array_stream.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/query_result.hpp"
#include "test_helpers.hpp"

using namespace duckdb;

namespace {

void ReleaseSchemaNoop(ArrowSchema *s) {
	s->release = nullptr;
}
void ReleaseArrayNoop(ArrowArray *a) {
	a->release = nullptr;
}

// Builds a single-column record batch whose column is a dictionary-encoded int32
// column (dictionary of two values) with the given int32 indices, wraps it in an
// ArrowArrayStream, and runs the arrow scan. Returns the query result (which may
// hold an error) so callers can assert on success or on a rejected scan.
//
// If 'validity' is non-null, the column's index array has an unknown null count
// (-1), which forces the masked selection-vector path.
static unique_ptr<QueryResult> ScanDictionaryIndices(Connection &con, const void *indices, idx_t n,
                                                     const uint8_t *validity = nullptr, const char *format = "i",
                                                     bool empty = false, idx_t offset = 0) {
	ArrowSchema dict_schema {};
	dict_schema.format = "i";
	dict_schema.flags = 2; // ARROW_FLAG_NULLABLE
	dict_schema.release = ReleaseSchemaNoop;

	ArrowSchema col_schema {};
	col_schema.format = format;
	col_schema.name = "a";
	col_schema.flags = 2;
	col_schema.dictionary = &dict_schema;
	col_schema.release = ReleaseSchemaNoop;

	ArrowSchema *col_children_schemas[1] = {&col_schema};
	ArrowSchema record_schema {};
	record_schema.format = "+s";
	record_schema.n_children = 1;
	record_schema.children = col_children_schemas;
	record_schema.release = ReleaseSchemaNoop;

	int32_t dict_values[] = {111, 222};
	const void *dict_array_buffers[2] = {nullptr, empty ? nullptr : dict_values};
	ArrowArray dict_array {};
	dict_array.length = empty ? 0 : 2;
	dict_array.n_buffers = 2;
	dict_array.buffers = dict_array_buffers;
	dict_array.release = ReleaseArrayNoop;

	const void *col_array_buffers[2] = {validity, indices};
	ArrowArray col_array {};
	col_array.length = n;
	col_array.offset = offset;
	col_array.null_count = validity ? -1 : 0;
	col_array.n_buffers = 2;
	col_array.buffers = col_array_buffers;
	col_array.dictionary = &dict_array;
	col_array.release = ReleaseArrayNoop;

	ArrowArray *col_children_arrays[1] = {&col_array};
	const void *record_array_buffers[1] = {nullptr};
	ArrowArray record_array {};
	record_array.length = n;
	record_array.n_buffers = 1;
	record_array.buffers = record_array_buffers;
	record_array.n_children = 1;
	record_array.children = col_children_arrays;
	record_array.release = ReleaseArrayNoop;

	ArrowArrayStream stream {};
	AdbcError err {};
	REQUIRE(duckdb_adbc::BatchToArrayStream(&record_array, &record_schema, &stream, &err) == ADBC_STATUS_OK);

	auto factory = ArrowTestHelper::ConstructArrowScan(stream);
	auto select = ArrowTestHelper::ArrowScanSelect(factory);
	return con.Query(std::move(select));
}

} // namespace

// Regression for https://github.com/duckdb/duckdb/issues/29174: out-of-range or
// negative Arrow dictionary indices used to be passed straight through to
// SelectionVector, reading memory outside the dictionary (arbitrary values, or a
// segfault on negative indices). They must now be rejected up front.

TEST_CASE("Arrow scan rejects out-of-range dictionary indices", "[arrow]") {
	DuckDB db(nullptr);
	Connection con(db);

	// index 5 is past the 2-entry dictionary
	int32_t indices[] = {0, 1, 5};
	auto result = ScanDictionaryIndices(con, indices, 3);
	REQUIRE(result->HasError());
	REQUIRE(result->GetError().find("Arrow dictionary index out of range") != std::string::npos);
}

TEST_CASE("Arrow scan rejects extreme out-of-range dictionary indices", "[arrow]") {
	DuckDB db(nullptr);
	Connection con(db);

	int32_t indices[] = {0, NumericLimits<int32_t>::Maximum()};
	auto result = ScanDictionaryIndices(con, indices, 2);
	REQUIRE(result->HasError());
	REQUIRE(result->GetError().find("Arrow dictionary index out of range") != std::string::npos);
}

TEST_CASE("Arrow scan rejects negative dictionary indices", "[arrow]") {
	DuckDB db(nullptr);
	Connection con(db);

	int32_t indices[] = {0, -1};
	auto result = ScanDictionaryIndices(con, indices, 2);
	REQUIRE(result->HasError());
	REQUIRE(result->GetError().find("Arrow dictionary index out of range") != std::string::npos);
}

TEST_CASE("Arrow scan rejects out-of-range dictionary indices with a validity mask", "[arrow]") {
	DuckDB db(nullptr);
	Connection con(db);

	// both rows valid (0x03 = bits 0 and 1 set), row 1 has an out-of-range index
	int32_t indices[] = {0, 5};
	uint8_t validity[] = {0x03};
	auto result = ScanDictionaryIndices(con, indices, 2, validity);
	REQUIRE(result->HasError());
	REQUIRE(result->GetError().find("Arrow dictionary index out of range") != std::string::npos);
}

TEST_CASE("Arrow scan of valid dictionary indices", "[arrow]") {
	DuckDB db(nullptr);
	Connection con(db);

	int32_t indices[] = {0, 1, 0, 1};
	auto result = ScanDictionaryIndices(con, indices, 4);
	REQUIRE(!result->HasError());
	auto chunk = result->Fetch();
	REQUIRE(chunk != nullptr);
	REQUIRE(chunk->size() == 4);
	REQUIRE(chunk->GetValue(0, 0).GetValue<int32_t>() == 111);
	REQUIRE(chunk->GetValue(0, 1).GetValue<int32_t>() == 222);
	REQUIRE(chunk->GetValue(0, 2).GetValue<int32_t>() == 111);
	REQUIRE(chunk->GetValue(0, 3).GetValue<int32_t>() == 222);
}

template <class T>
static void CheckValidDictionaryIndices(const char *format) {
	DuckDB db(nullptr);
	Connection con(db);
	const T indices[] = {1, 0, 0};
	const uint8_t validity[] = {0b00000101};
	auto result = ScanDictionaryIndices(con, indices, 3, validity, format);
	REQUIRE(!result->HasError());
	auto chunk = result->Fetch();
	REQUIRE(chunk);
	REQUIRE(chunk->size() == 3);
	REQUIRE(chunk->GetValue(0, 0) == Value::INTEGER(222));
	REQUIRE(chunk->GetValue(0, 1).IsNull());
	REQUIRE(chunk->GetValue(0, 2) == Value::INTEGER(111));
}

TEST_CASE("Arrow dictionary preserves values and NULLs for all integer index widths", "[arrow]") {
	CheckValidDictionaryIndices<int8_t>("c");
	CheckValidDictionaryIndices<uint8_t>("C");
	CheckValidDictionaryIndices<int16_t>("s");
	CheckValidDictionaryIndices<uint16_t>("S");
	CheckValidDictionaryIndices<int32_t>("i");
	CheckValidDictionaryIndices<uint32_t>("I");
	CheckValidDictionaryIndices<int64_t>("l");
	CheckValidDictionaryIndices<uint64_t>("L");
}

template <class T>
static void CheckUnmasked64BitDictionaryIndices(const char *format) {
	DuckDB db(nullptr);
	Connection con(db);
	const T indices[] = {0, 1, 1, 0};
	auto result = ScanDictionaryIndices(con, indices, 3, nullptr, format, false, 1);
	REQUIRE(!result->HasError());
	auto chunk = result->Fetch();
	REQUIRE(chunk);
	REQUIRE(chunk->size() == 3);
	REQUIRE(chunk->GetValue(0, 0) == Value::INTEGER(222));
	REQUIRE(chunk->GetValue(0, 1) == Value::INTEGER(222));
	REQUIRE(chunk->GetValue(0, 2) == Value::INTEGER(111));
}

TEST_CASE("Arrow dictionary with 64-bit indices and no validity bitmap honors its offset", "[arrow]") {
	CheckUnmasked64BitDictionaryIndices<int64_t>("l");
	CheckUnmasked64BitDictionaryIndices<uint64_t>("L");
}

TEST_CASE("Arrow empty dictionary with NULL indices needs no value buffer", "[arrow]") {
	DuckDB db(nullptr);
	Connection con(db);
	const int32_t indices[] = {0, 0};
	const uint8_t validity[] = {0};
	auto result = ScanDictionaryIndices(con, indices, 2, validity, "i", true);
	REQUIRE(!result->HasError());
	auto chunk = result->Fetch();
	REQUIRE(chunk);
	REQUIRE(chunk->size() == 2);
	REQUIRE(chunk->GetValue(0, 0).IsNull());
	REQUIRE(chunk->GetValue(0, 1).IsNull());
}

TEST_CASE("Arrow scan rejects a non-NULL index into an empty dictionary", "[arrow]") {
	DuckDB db(nullptr);
	Connection con(db);
	const int32_t indices[] = {0};
	auto result = ScanDictionaryIndices(con, indices, 1, nullptr, "i", true);
	REQUIRE(result->HasError());
	REQUIRE(result->GetError().find("Arrow dictionary index out of range") != std::string::npos);
}
