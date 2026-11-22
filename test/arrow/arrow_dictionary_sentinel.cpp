#include "catch.hpp"

#include "arrow/arrow_test_helper.hpp"
#include "duckdb/common/adbc/single_batch_array_stream.hpp"
#include "duckdb/common/enums/debug_verification_mode.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "test_helpers.hpp"

#include <vector>

using namespace duckdb;

namespace {

// Stack-allocated structs need a no-op release.
void NoopReleaseSchema(ArrowSchema *s) {
	s->release = nullptr;
}
void NoopReleaseArray(ArrowArray *a) {
	a->release = nullptr;
}

// STRUCT(i INTEGER, s VARCHAR) column with int8 indices; the dictionary holds three values and a NULL value, row 3
// points at the NULL value and row 4 has a NULL index.
struct StructDictionaryColumn {
	StructDictionaryColumn() {
		i_schema.format = "i";
		i_schema.name = "i";
		i_schema.flags = 2; // ARROW_FLAG_NULLABLE
		i_schema.release = NoopReleaseSchema;
		s_schema.format = "u";
		s_schema.name = "s";
		s_schema.flags = 2;
		s_schema.release = NoopReleaseSchema;
		value_schema_children[0] = &i_schema;
		value_schema_children[1] = &s_schema;
		value_schema.format = "+s";
		value_schema.flags = 2;
		value_schema.n_children = 2;
		value_schema.children = value_schema_children;
		value_schema.release = NoopReleaseSchema;
		schema.format = "c";
		schema.name = "d";
		schema.flags = 2;
		schema.dictionary = &value_schema;
		schema.release = NoopReleaseSchema;

		i_array.length = 4;
		i_array.n_buffers = 2;
		i_array.buffers = i_buffers;
		i_array.release = NoopReleaseArray;
		s_array.length = 4;
		s_array.n_buffers = 3;
		s_array.buffers = s_buffers;
		s_array.release = NoopReleaseArray;
		value_array_children[0] = &i_array;
		value_array_children[1] = &s_array;
		value_array.length = 4;
		value_array.null_count = 1;
		value_array.n_buffers = 1;
		value_array.buffers = value_buffers;
		value_array.n_children = 2;
		value_array.children = value_array_children;
		value_array.release = NoopReleaseArray;
		array.length = 7;
		array.null_count = 1;
		array.n_buffers = 2;
		array.buffers = buffers;
		array.dictionary = &value_array;
		array.release = NoopReleaseArray;
	}
	StructDictionaryColumn(const StructDictionaryColumn &) = delete;

	int32_t i_values[4] = {1, 2, 3, 0};
	int32_t s_offsets[5] = {0, 1, 2, 3, 3};
	uint8_t value_validity[1] = {0x07};
	int8_t indices[7] = {0, 1, 2, 3, 0, 1, 0};
	uint8_t validity[1] = {0x6F};
	const void *i_buffers[2] = {nullptr, i_values};
	const void *s_buffers[3] = {nullptr, s_offsets, "abc"};
	const void *value_buffers[1] = {value_validity};
	const void *buffers[2] = {validity, indices};
	ArrowSchema *value_schema_children[2];
	ArrowArray *value_array_children[2];
	ArrowSchema i_schema {};
	ArrowSchema s_schema {};
	ArrowSchema value_schema {};
	ArrowSchema schema {};
	ArrowArray i_array {};
	ArrowArray s_array {};
	ArrowArray value_array {};
	ArrowArray array {};
};

// debug_verification_mode is process wide, so it is restored for the tests that run later.
struct VerificationModeRestorer {
	~VerificationModeRestorer() {
		DBConfigOptions::global_verification_mode = previous;
	}
	DebugVerificationMode previous = DBConfigOptions::global_verification_mode;
};

// int8 indices {0, NULL, 1} into a dictionary of two values.
struct NullIndexColumn {
	NullIndexColumn(ArrowSchema &value_schema, ArrowArray &value_array) {
		schema.format = "c";
		schema.name = "d";
		schema.flags = 2; // ARROW_FLAG_NULLABLE
		schema.dictionary = &value_schema;
		schema.release = NoopReleaseSchema;
		array.length = 3;
		array.null_count = 1;
		array.n_buffers = 2;
		array.buffers = buffers;
		array.dictionary = &value_array;
		array.release = NoopReleaseArray;
	}
	NullIndexColumn(const NullIndexColumn &) = delete;

	int8_t indices[3] = {0, 0, 1};
	uint8_t validity[1] = {0x05};
	const void *buffers[2] = {validity, indices};
	ArrowSchema schema {};
	ArrowArray array {};
};

void InitNullTypeArray(ArrowSchema &schema, ArrowArray &array, const char *name, int64_t length) {
	schema.format = "n";
	schema.name = name;
	schema.flags = 2;
	schema.release = NoopReleaseSchema;
	array.length = length;
	array.null_count = length;
	array.release = NoopReleaseArray;
}

// The stream keeps pointers into the record batch, so both live in one object.
struct ColumnStream {
	ColumnStream(ArrowSchema &col_schema, ArrowArray &col_array) {
		schema_children[0] = &col_schema;
		record_schema.format = "+s";
		record_schema.n_children = 1;
		record_schema.children = schema_children;
		record_schema.release = NoopReleaseSchema;
		array_children[0] = &col_array;
		record_array.length = col_array.length;
		record_array.n_buffers = 1;
		record_array.buffers = buffers;
		record_array.n_children = 1;
		record_array.children = array_children;
		record_array.release = NoopReleaseArray;
		AdbcError err {};
		REQUIRE(duckdb_adbc::BatchToArrayStream(&record_array, &record_schema, &stream, &err) == ADBC_STATUS_OK);
	}
	ColumnStream(const ColumnStream &) = delete;

	ArrowSchema *schema_children[1];
	ArrowArray *array_children[1];
	const void *buffers[1] = {nullptr};
	ArrowSchema record_schema {};
	ArrowArray record_array {};
	ArrowArrayStream stream {};
};

bool ScanColumnMatches(ArrowSchema &col_schema, ArrowArray &col_array, const string &query,
                       bool verify_vectors = false) {
	DuckDB db(nullptr);
	Connection con(db);
	VerificationModeRestorer restore_verification_mode;
	if (verify_vectors) {
		REQUIRE_NO_FAIL(con.Query("SET debug_verification_mode='verify_vectors'"));
	}
	ColumnStream compared(col_schema, col_array);
	if (!ArrowTestHelper::RunArrowComparison(con, query, compared.stream)) {
		return false;
	}
	// RunArrowComparison only checks that every scanned row is expected, so a scan that drops rows passes it.
	ColumnStream counted(col_schema, col_array);
	auto count_select = ArrowTestHelper::ArrowScanSelect(ArrowTestHelper::ConstructArrowScan(counted.stream));
	auto &count_node = count_select->node->Cast<SelectNode>();
	count_node.select_list = Parser(*con.context).ParseExpressionList("count(*)");
	count_node.aggregate_handling = AggregateHandling::FORCE_AGGREGATES;
	auto scanned = con.Query(std::move(count_select));
	auto expected = con.Query("SELECT count(*) FROM (" + query + ")");
	REQUIRE_NO_FAIL(*scanned);
	REQUIRE_NO_FAIL(*expected);
	return scanned->Fetch()->GetValue(0, 0) == expected->Fetch()->GetValue(0, 0);
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

// A null_count of -1 means the number of nulls is unknown - the validity bitmap must still be used, and the
// (arbitrary) indices in null slots must not be used to read the dictionary.
TEST_CASE("Arrow scan of dict-encoded column with an unknown null count", "[arrow]") {
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

	// row 1 is NULL, and its index points outside of the dictionary
	int32_t indices[] = {0, 1000, 2, 1};
	uint8_t validity[] = {0x0D};
	const void *col_array_buffers[2] = {validity, indices};
	ArrowArray col_array {};
	col_array.length = 4;
	col_array.null_count = -1;
	col_array.n_buffers = 2;
	col_array.buffers = col_array_buffers;
	col_array.dictionary = &dict_array;
	col_array.release = NoopReleaseArray;

	ArrowArray *col_children_arrays[1] = {&col_array};
	const void *record_array_buffers[1] = {nullptr};
	ArrowArray record_array {};
	record_array.length = 4;
	record_array.n_buffers = 1;
	record_array.buffers = record_array_buffers;
	record_array.n_children = 1;
	record_array.children = col_children_arrays;
	record_array.release = NoopReleaseArray;

	ArrowArrayStream stream {};
	AdbcError err {};
	REQUIRE(duckdb_adbc::BatchToArrayStream(&record_array, &record_schema, &stream, &err) == ADBC_STATUS_OK);

	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE(ArrowTestHelper::RunArrowComparison(con, "SELECT * FROM (VALUES (10), (NULL), (30), (20)) t(a)", stream));
}

// The NULL index selects the sentinel slot, whose struct children must be NULL too: vector verification checks that
// directly, the plain scan otherwise does not compare the row equal to a NULL struct, and with
// DUCKDB_JE_MALLOC_CONF=junk:true it crashes on a garbage string.
TEST_CASE("Arrow scan of dict-encoded STRUCT column with NULL sentinel", "[arrow]") {
	const string expected = "SELECT * FROM (VALUES ({'i': 1, 's': 'a'}), ({'i': 2, 's': 'b'}), ({'i': 3, 's': 'c'}), "
	                        "(NULL), (NULL), ({'i': 2, 's': 'b'}), ({'i': 1, 's': 'a'})) t(d)";
	StructDictionaryColumn verified_col;
	REQUIRE(ScanColumnMatches(verified_col.schema, verified_col.array, expected, /*verify_vectors=*/true));
	StructDictionaryColumn col;
	REQUIRE(ScanColumnMatches(col.schema, col.array, expected));
}

TEST_CASE("Arrow dictionary sentinel reserves capacity in nested zero-copy children", "[arrow]") {
	StructDictionaryColumn column;
	// Three dictionary rows plus the sentinel fit the STRUCT and ARRAY's original capacity.
	// The zero-copy integer elements of the ARRAY still need to be grown from six to eight.
	column.value_array.length = 3;
	column.value_array.null_count = 0;
	column.value_buffers[0] = nullptr;
	column.s_array.length = 3;
	column.i_array.length = 3;
	int32_t integers[] = {1, 2, 3, 4, 5, 6};
	const void *element_buffers[] = {nullptr, integers};
	ArrowSchema element_schema = column.i_schema;
	element_schema.name = "item";
	ArrowArray element_array = column.i_array;
	element_array.length = 6;
	element_array.buffers = element_buffers;
	ArrowSchema *array_schema_children[] = {&element_schema};
	ArrowArray *array_children[] = {&element_array};
	column.i_schema.format = "+w:2";
	column.i_schema.n_children = 1;
	column.i_schema.children = array_schema_children;
	column.i_array.n_buffers = 1;
	column.i_array.n_children = 1;
	column.i_array.children = array_children;

	NullIndexColumn dict(column.value_schema, column.value_array);
	dict.indices[2] = 2;
	REQUIRE(ScanColumnMatches(dict.schema, dict.array,
	                          "SELECT * FROM (VALUES ({'i': [1, 2]::INTEGER[2], 's': 'a'}), (NULL), "
	                          "({'i': [5, 6]::INTEGER[2], 's': 'c'})) t(d)",
	                          /*verify_vectors=*/true));
}

TEST_CASE("Arrow dictionary keeps fixed-size array values and NULL indices", "[arrow]") {
	ArrowSchema elements_schema {};
	elements_schema.format = "i";
	elements_schema.name = "item";
	elements_schema.release = NoopReleaseSchema;
	int32_t integers[] = {1, 2, 3, 4};
	const void *elements_buffers[] = {nullptr, integers};
	ArrowArray elements {};
	elements.length = 4;
	elements.n_buffers = 2;
	elements.buffers = elements_buffers;
	elements.release = NoopReleaseArray;

	ArrowSchema *schema_children[] = {&elements_schema};
	ArrowSchema array_schema {};
	array_schema.format = "+w:2";
	array_schema.n_children = 1;
	array_schema.children = schema_children;
	array_schema.release = NoopReleaseSchema;
	ArrowArray *children[] = {&elements};
	const void *buffers[] = {nullptr};
	ArrowArray array {};
	array.length = 2;
	array.n_buffers = 1;
	array.buffers = buffers;
	array.n_children = 1;
	array.children = children;
	array.release = NoopReleaseArray;

	NullIndexColumn dict(array_schema, array);
	REQUIRE(ScanColumnMatches(dict.schema, dict.array,
	                          "SELECT * FROM (VALUES ([1, 2]::INTEGER[2]), (NULL), ([3, 4]::INTEGER[2])) t(d)",
	                          /*verify_vectors=*/true));
}

TEST_CASE("Arrow dictionary of STRUCT values with a dictionary-encoded child keeps the value types", "[arrow]") {
	// inner dictionary: utf8 values {"x", "y"}, int8 indices {1, 0}
	ArrowSchema inner_values_schema {};
	inner_values_schema.format = "u";
	inner_values_schema.release = NoopReleaseSchema;
	int32_t inner_offsets[] = {0, 1, 2};
	const void *inner_values_buffers[] = {nullptr, inner_offsets, "xy"};
	ArrowArray inner_values {};
	inner_values.length = 2;
	inner_values.n_buffers = 3;
	inner_values.buffers = inner_values_buffers;
	inner_values.release = NoopReleaseArray;

	ArrowSchema s_schema {};
	s_schema.format = "c";
	s_schema.name = "s";
	s_schema.flags = 2;
	s_schema.dictionary = &inner_values_schema;
	s_schema.release = NoopReleaseSchema;
	int8_t inner_indices[] = {1, 0};
	const void *s_buffers[] = {nullptr, inner_indices};
	ArrowArray s_array {};
	s_array.length = 2;
	s_array.n_buffers = 2;
	s_array.buffers = s_buffers;
	s_array.dictionary = &inner_values;
	s_array.release = NoopReleaseArray;

	ArrowSchema *struct_schema_children[] = {&s_schema};
	ArrowSchema struct_schema {};
	struct_schema.format = "+s";
	struct_schema.n_children = 1;
	struct_schema.children = struct_schema_children;
	struct_schema.release = NoopReleaseSchema;
	ArrowArray *struct_children[] = {&s_array};
	const void *struct_buffers[] = {nullptr};
	ArrowArray struct_array {};
	struct_array.length = 2;
	struct_array.n_buffers = 1;
	struct_array.buffers = struct_buffers;
	struct_array.n_children = 1;
	struct_array.children = struct_children;
	struct_array.release = NoopReleaseArray;

	// outer indices {0, NULL, 1}
	NullIndexColumn dict(struct_schema, struct_array);
	REQUIRE(ScanColumnMatches(dict.schema, dict.array,
	                          "SELECT * FROM (VALUES ({'s': 'y'}), (NULL), ({'s': 'x'})) t(d)",
	                          /*verify_vectors=*/true));
}

TEST_CASE("Arrow dictionary of fixed-size arrays across several scan chunks", "[arrow]") {
	// dictionary values [[1, 2], [3, 4]]; row i selects i % 2 and is NULL when i % 7 == 0
	constexpr idx_t ROWS = 5000;
	ArrowSchema elements_schema {};
	elements_schema.format = "i";
	elements_schema.release = NoopReleaseSchema;
	int32_t integers[] = {1, 2, 3, 4};
	const void *elements_buffers[] = {nullptr, integers};
	ArrowArray elements {};
	elements.length = 4;
	elements.n_buffers = 2;
	elements.buffers = elements_buffers;
	elements.release = NoopReleaseArray;
	ArrowSchema *array_schema_children[] = {&elements_schema};
	ArrowSchema array_schema {};
	array_schema.format = "+w:2";
	array_schema.n_children = 1;
	array_schema.children = array_schema_children;
	array_schema.release = NoopReleaseSchema;
	ArrowArray *array_children[] = {&elements};
	const void *array_buffers[] = {nullptr};
	ArrowArray values {};
	values.length = 2;
	values.n_buffers = 1;
	values.buffers = array_buffers;
	values.n_children = 1;
	values.children = array_children;
	values.release = NoopReleaseArray;

	std::vector<int16_t> indices(ROWS);
	std::vector<uint8_t> validity((ROWS + 7) / 8, 0);
	for (idx_t i = 0; i < ROWS; i++) {
		indices[i] = int16_t(i % 2);
		if (i % 7 != 0) {
			validity[i / 8] |= uint8_t(1u << (i % 8));
		}
	}
	ArrowSchema schema {};
	schema.format = "s";
	schema.name = "d";
	schema.flags = 2;
	schema.dictionary = &array_schema;
	schema.release = NoopReleaseSchema;
	const void *buffers[] = {validity.data(), indices.data()};
	ArrowArray array {};
	array.length = ROWS;
	array.null_count = -1;
	array.n_buffers = 2;
	array.buffers = buffers;
	array.dictionary = &values;
	array.release = NoopReleaseArray;
	REQUIRE(ScanColumnMatches(schema, array,
	                          "SELECT CASE WHEN i % 7 = 0 THEN NULL WHEN i % 2 = 0 THEN [1, 2]::INTEGER[2] "
	                          "ELSE [3, 4]::INTEGER[2] END AS d FROM range(5000) r(i)",
	                          /*verify_vectors=*/true));
}

// Two dictionary values of ARRAY(width INTEGER): all 'a' and all 'b'. Rows are value 0, NULL index, value 1.
TEST_CASE("Arrow dictionary with the maximum ARRAY width and a NULL index", "[arrow]") {
	const idx_t width = ArrayType::MAX_ARRAY_SIZE;
	std::vector<int32_t> integers(width * 2);
	std::fill(integers.begin(), integers.begin() + NumericCast<idx_t>(width), 7);
	std::fill(integers.begin() + NumericCast<idx_t>(width), integers.end(), 9);
	ArrowSchema elements_schema {};
	elements_schema.format = "i";
	elements_schema.release = NoopReleaseSchema;
	const void *elements_buffers[] = {nullptr, integers.data()};
	ArrowArray elements {};
	elements.length = NumericCast<int64_t>(width * 2);
	elements.n_buffers = 2;
	elements.buffers = elements_buffers;
	elements.release = NoopReleaseArray;
	ArrowSchema *schema_children[] = {&elements_schema};
	const string format = "+w:" + std::to_string(width);
	ArrowSchema array_schema {};
	array_schema.format = format.c_str();
	array_schema.n_children = 1;
	array_schema.children = schema_children;
	array_schema.release = NoopReleaseSchema;
	ArrowArray *children[] = {&elements};
	const void *buffers[] = {nullptr};
	ArrowArray array {};
	array.length = 2;
	array.n_buffers = 1;
	array.buffers = buffers;
	array.n_children = 1;
	array.children = children;
	array.release = NoopReleaseArray;

	NullIndexColumn dict(array_schema, array);
	DuckDB db(nullptr);
	Connection con(db);
	ColumnStream stream(dict.schema, dict.array);
	ArrowTestHelper::CreateArrowScanView(con, ArrowTestHelper::ConstructArrowScan(stream.stream), "arrow_view", true, true);
	auto result = con.Query("SELECT d[1], d[" + std::to_string(width) + "], d IS NULL FROM arrow_view");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(7), Value(), Value::INTEGER(9)}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value::INTEGER(7), Value(), Value::INTEGER(9)}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value::BOOLEAN(false), Value::BOOLEAN(true), Value::BOOLEAN(false)}));
	stream.stream.release = nullptr;
}

TEST_CASE("Arrow dictionary of nested ARRAY values and a NULL index", "[arrow]") {
	// value 0 is [[7, 7], [7, 7]], value 1 is [[9, 9], [9, 9]]
	const idx_t outer = 2;
	const idx_t inner = 2;
	std::vector<int32_t> integers(outer * inner * 2);
	std::fill(integers.begin(), integers.begin() + 4, 7);
	std::fill(integers.begin() + 4, integers.end(), 9);
	ArrowSchema int_schema {};
	int_schema.format = "i";
	int_schema.release = NoopReleaseSchema;
	const void *int_buffers[] = {nullptr, integers.data()};
	ArrowArray ints {};
	ints.length = 8;
	ints.n_buffers = 2;
	ints.buffers = int_buffers;
	ints.release = NoopReleaseArray;
	ArrowSchema *inner_children[] = {&int_schema};
	ArrowSchema inner_schema {};
	inner_schema.format = "+w:2";
	inner_schema.n_children = 1;
	inner_schema.children = inner_children;
	inner_schema.release = NoopReleaseSchema;
	ArrowArray *inner_arrays[] = {&ints};
	const void *inner_buffers[] = {nullptr};
	ArrowArray inner_array {};
	inner_array.length = 4;
	inner_array.n_buffers = 1;
	inner_array.buffers = inner_buffers;
	inner_array.n_children = 1;
	inner_array.children = inner_arrays;
	inner_array.release = NoopReleaseArray;
	ArrowSchema *outer_children[] = {&inner_schema};
	ArrowSchema outer_schema {};
	outer_schema.format = "+w:2";
	outer_schema.n_children = 1;
	outer_schema.children = outer_children;
	outer_schema.release = NoopReleaseSchema;
	ArrowArray *outer_arrays[] = {&inner_array};
	const void *outer_buffers[] = {nullptr};
	ArrowArray outer_array {};
	outer_array.length = 2;
	outer_array.n_buffers = 1;
	outer_array.buffers = outer_buffers;
	outer_array.n_children = 1;
	outer_array.children = outer_arrays;
	outer_array.release = NoopReleaseArray;

	NullIndexColumn dict(outer_schema, outer_array);
	DuckDB db(nullptr);
	Connection con(db);
	ColumnStream stream(dict.schema, dict.array);
	ArrowTestHelper::CreateArrowScanView(con, ArrowTestHelper::ConstructArrowScan(stream.stream), "arrow_view", true, true);
	auto result = con.Query("SELECT d[2][2], d[1][1], d IS NULL FROM arrow_view");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(7), Value(), Value::INTEGER(9)}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value::INTEGER(7), Value(), Value::INTEGER(9)}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value::BOOLEAN(false), Value::BOOLEAN(true), Value::BOOLEAN(false)}));
	stream.stream.release = nullptr;
}

TEST_CASE("Arrow scan of dict-encoded LIST column with NULL sentinel", "[arrow]") {
	ArrowSchema element_schema {};
	element_schema.format = "u";
	element_schema.name = "item";
	element_schema.flags = 2; // ARROW_FLAG_NULLABLE
	element_schema.release = NoopReleaseSchema;
	int32_t element_offsets[] = {0, 1, 2, 3};
	const void *element_buffers[3] = {nullptr, element_offsets, "abc"};
	ArrowArray element_array {};
	element_array.length = 3;
	element_array.n_buffers = 3;
	element_array.buffers = element_buffers;
	element_array.release = NoopReleaseArray;

	// ['a'], ['b', 'c'], [] and a NULL list
	ArrowSchema *value_schema_children[1] = {&element_schema};
	ArrowSchema value_schema {};
	value_schema.format = "+l";
	value_schema.flags = 2;
	value_schema.n_children = 1;
	value_schema.children = value_schema_children;
	value_schema.release = NoopReleaseSchema;
	uint8_t value_validity[] = {0x07};
	int32_t value_offsets[] = {0, 1, 3, 3, 3};
	const void *value_buffers[2] = {value_validity, value_offsets};
	ArrowArray *value_array_children[1] = {&element_array};
	ArrowArray value_array {};
	value_array.length = 4;
	value_array.null_count = 1;
	value_array.n_buffers = 2;
	value_array.buffers = value_buffers;
	value_array.n_children = 1;
	value_array.children = value_array_children;
	value_array.release = NoopReleaseArray;

	// row 3 points at the NULL list and row 4 has a NULL index
	ArrowSchema schema {};
	schema.format = "c";
	schema.name = "d";
	schema.flags = 2;
	schema.dictionary = &value_schema;
	schema.release = NoopReleaseSchema;
	int8_t indices[] = {0, 1, 2, 3, 0, 1, 0};
	uint8_t validity[] = {0x6F};
	const void *buffers[2] = {validity, indices};
	ArrowArray array {};
	array.length = 7;
	array.null_count = 1;
	array.n_buffers = 2;
	array.buffers = buffers;
	array.dictionary = &value_array;
	array.release = NoopReleaseArray;

	REQUIRE(
	    ScanColumnMatches(schema, array,
	                      "SELECT * FROM (VALUES (['a']), (['b', 'c']), ([]::VARCHAR[]), (NULL), (NULL), (['b', 'c']), "
	                      "(['a'])) t(d)"));
}

TEST_CASE("Arrow scan of dict-encoded STRUCT column inside a STRUCT with NULL sentinel", "[arrow]") {
	StructDictionaryColumn dict;
	ArrowSchema *schema_children[1] = {&dict.schema};
	ArrowSchema schema {};
	schema.format = "+s";
	schema.name = "o";
	schema.flags = 2;
	schema.n_children = 1;
	schema.children = schema_children;
	schema.release = NoopReleaseSchema;

	// row 2 is NULL in the parent while its index is valid
	uint8_t validity[] = {0x7B};
	const void *buffers[1] = {validity};
	ArrowArray *array_children[1] = {&dict.array};
	ArrowArray array {};
	array.length = 7;
	array.null_count = 1;
	array.n_buffers = 1;
	array.buffers = buffers;
	array.n_children = 1;
	array.children = array_children;
	array.release = NoopReleaseArray;

	REQUIRE(ScanColumnMatches(
	    schema, array,
	    "SELECT * FROM (VALUES ({'d': {'i': 1, 's': 'a'}}), ({'d': {'i': 2, 's': 'b'}}), (NULL), "
	    "({'d': NULL}), ({'d': NULL}), ({'d': {'i': 2, 's': 'b'}}), ({'d': {'i': 1, 's': 'a'}})) t(o)"));
}

TEST_CASE("Arrow scan of dict-encoded null type column with NULL sentinel", "[arrow]") {
	ArrowSchema value_schema {};
	ArrowArray value_array {};
	InitNullTypeArray(value_schema, value_array, nullptr, 2);
	NullIndexColumn col(value_schema, value_array);
	REQUIRE(ScanColumnMatches(col.schema, col.array, "SELECT * FROM (VALUES (NULL), (NULL), (NULL)) t(d)"));
}

TEST_CASE("Arrow scan of dict-encoded STRUCT column with a null type field and NULL sentinel", "[arrow]") {
	ArrowSchema n_schema {};
	ArrowArray n_array {};
	InitNullTypeArray(n_schema, n_array, "n", 2);
	ArrowSchema s_schema {};
	s_schema.format = "u";
	s_schema.name = "s";
	s_schema.flags = 2;
	s_schema.release = NoopReleaseSchema;
	int32_t s_offsets[] = {0, 1, 2};
	const void *s_buffers[3] = {nullptr, s_offsets, "ab"};
	ArrowArray s_array {};
	s_array.length = 2;
	s_array.n_buffers = 3;
	s_array.buffers = s_buffers;
	s_array.release = NoopReleaseArray;

	ArrowSchema *value_schema_children[2] = {&n_schema, &s_schema};
	ArrowSchema value_schema {};
	value_schema.format = "+s";
	value_schema.flags = 2;
	value_schema.n_children = 2;
	value_schema.children = value_schema_children;
	value_schema.release = NoopReleaseSchema;
	ArrowArray *value_array_children[2] = {&n_array, &s_array};
	const void *value_buffers[1] = {nullptr};
	ArrowArray value_array {};
	value_array.length = 2;
	value_array.n_buffers = 1;
	value_array.buffers = value_buffers;
	value_array.n_children = 2;
	value_array.children = value_array_children;
	value_array.release = NoopReleaseArray;

	NullIndexColumn col(value_schema, value_array);
	REQUIRE(ScanColumnMatches(col.schema, col.array,
	                          "SELECT * FROM (VALUES ({'n': NULL, 's': 'a'}), (NULL), ({'n': NULL, 's': 'b'})) t(d)"));
}

TEST_CASE("Arrow scan of dict-encoded ARRAY of null type column with NULL sentinel", "[arrow]") {
	ArrowSchema element_schema {};
	ArrowArray element_array {};
	InitNullTypeArray(element_schema, element_array, "item", 4);

	ArrowSchema *value_schema_children[1] = {&element_schema};
	ArrowSchema value_schema {};
	value_schema.format = "+w:2";
	value_schema.flags = 2;
	value_schema.n_children = 1;
	value_schema.children = value_schema_children;
	value_schema.release = NoopReleaseSchema;
	ArrowArray *value_array_children[1] = {&element_array};
	const void *value_buffers[1] = {nullptr};
	ArrowArray value_array {};
	value_array.length = 2;
	value_array.n_buffers = 1;
	value_array.buffers = value_buffers;
	value_array.n_children = 1;
	value_array.children = value_array_children;
	value_array.release = NoopReleaseArray;

	NullIndexColumn col(value_schema, value_array);
	REQUIRE(
	    ScanColumnMatches(col.schema, col.array, "SELECT * FROM (VALUES ([NULL, NULL]), (NULL), ([NULL, NULL])) t(d)"));
}
