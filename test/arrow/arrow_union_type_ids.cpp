#include "catch.hpp"

#include "arrow/arrow_test_helper.hpp"
#include "duckdb/common/adbc/single_batch_array_stream.hpp"

using namespace duckdb;

static void NoOpSchemaRelease(ArrowSchema *schema) {
	schema->release = nullptr;
}

static void NoOpArrayRelease(ArrowArray *array) {
	array->release = nullptr;
}

// Constructs a sparse Arrow union with non-identity type IDs to verify
// that DuckDB correctly maps type_ids from the format string to child indices,
// rather than assuming type_id == child_index.
//
// The union has 3 members:
//   child 0: int32   (type_id = 5)
//   child 1: varchar (type_id = 7)
//   child 2: float   (type_id = 9)
//
// Format string: "+us:5,7,9"
TEST_CASE("Test Arrow union with non-identity type IDs", "[arrow]") {
	const idx_t n_rows = 6;

	// ---- Build ArrowSchema ----
	// Union children schemas
	ArrowSchema child_schemas[3];

	// child 0: int32 (type_id 5)
	child_schemas[0].format = "i";
	child_schemas[0].name = "int_val";
	child_schemas[0].metadata = nullptr;
	child_schemas[0].flags = ARROW_FLAG_NULLABLE;
	child_schemas[0].n_children = 0;
	child_schemas[0].children = nullptr;
	child_schemas[0].dictionary = nullptr;
	child_schemas[0].release = NoOpSchemaRelease;
	child_schemas[0].private_data = nullptr;

	// child 1: utf8 string (type_id 7)
	child_schemas[1].format = "u";
	child_schemas[1].name = "str_val";
	child_schemas[1].metadata = nullptr;
	child_schemas[1].flags = ARROW_FLAG_NULLABLE;
	child_schemas[1].n_children = 0;
	child_schemas[1].children = nullptr;
	child_schemas[1].dictionary = nullptr;
	child_schemas[1].release = NoOpSchemaRelease;
	child_schemas[1].private_data = nullptr;

	// child 2: float32 (type_id 9)
	child_schemas[2].format = "f";
	child_schemas[2].name = "float_val";
	child_schemas[2].metadata = nullptr;
	child_schemas[2].flags = ARROW_FLAG_NULLABLE;
	child_schemas[2].n_children = 0;
	child_schemas[2].children = nullptr;
	child_schemas[2].dictionary = nullptr;
	child_schemas[2].release = NoOpSchemaRelease;
	child_schemas[2].private_data = nullptr;

	// Union schema
	ArrowSchema union_schema;
	ArrowSchema *union_child_ptrs[3] = {&child_schemas[0], &child_schemas[1], &child_schemas[2]};
	union_schema.format = "+us:5,7,9";
	union_schema.name = "my_union";
	union_schema.metadata = nullptr;
	union_schema.flags = 0;
	union_schema.n_children = 3;
	union_schema.children = union_child_ptrs;
	union_schema.dictionary = nullptr;
	union_schema.release = NoOpSchemaRelease;
	union_schema.private_data = nullptr;

	// Root schema: a struct with one child (the union column)
	ArrowSchema root_schema;
	ArrowSchema *root_child_ptrs[1] = {&union_schema};
	root_schema.format = "+s";
	root_schema.name = "root";
	root_schema.metadata = nullptr;
	root_schema.flags = 0;
	root_schema.n_children = 1;
	root_schema.children = root_child_ptrs;
	root_schema.dictionary = nullptr;
	root_schema.release = NoOpSchemaRelease;
	root_schema.private_data = nullptr;

	// ---- Build ArrowArray ----
	// Type IDs buffer: 6 rows cycling through type_ids 5, 7, 9, 5, 7, 9
	int8_t type_ids[n_rows] = {5, 7, 9, 5, 7, 9};

	// child 0: int32 values (only rows 0,3 are "active" with type_id=5)
	// Sparse union: all children have n_rows entries, but only the "active" ones matter.
	int32_t int_values[n_rows] = {42, 0, 0, 100, 0, 0};

	// child 1: utf8 string values (only rows 1,4 are "active" with type_id=7)
	const char *str_data = "helloworld";
	int32_t str_offsets[n_rows + 1] = {0, 0, 5, 5, 5, 10, 10};

	// child 2: float32 values (only rows 2,5 are "active" with type_id=9)
	float float_values[n_rows] = {0.0f, 0.0f, 3.14f, 0.0f, 0.0f, 2.72f};

	// Build child arrays
	ArrowArray child_arrays[3];

	// child 0: int32
	const void *int_buffers[2] = {nullptr, int_values};
	child_arrays[0].length = static_cast<int64_t>(n_rows);
	child_arrays[0].null_count = 0;
	child_arrays[0].offset = 0;
	child_arrays[0].n_buffers = 2;
	child_arrays[0].buffers = int_buffers;
	child_arrays[0].n_children = 0;
	child_arrays[0].children = nullptr;
	child_arrays[0].dictionary = nullptr;
	child_arrays[0].release = NoOpArrayRelease;
	child_arrays[0].private_data = nullptr;

	// child 1: utf8
	const void *str_buffers[3] = {nullptr, str_offsets, str_data};
	child_arrays[1].length = static_cast<int64_t>(n_rows);
	child_arrays[1].null_count = 0;
	child_arrays[1].offset = 0;
	child_arrays[1].n_buffers = 3;
	child_arrays[1].buffers = str_buffers;
	child_arrays[1].n_children = 0;
	child_arrays[1].children = nullptr;
	child_arrays[1].dictionary = nullptr;
	child_arrays[1].release = NoOpArrayRelease;
	child_arrays[1].private_data = nullptr;

	// child 2: float32
	const void *float_buffers[2] = {nullptr, float_values};
	child_arrays[2].length = static_cast<int64_t>(n_rows);
	child_arrays[2].null_count = 0;
	child_arrays[2].offset = 0;
	child_arrays[2].n_buffers = 2;
	child_arrays[2].buffers = float_buffers;
	child_arrays[2].n_children = 0;
	child_arrays[2].children = nullptr;
	child_arrays[2].dictionary = nullptr;
	child_arrays[2].release = NoOpArrayRelease;
	child_arrays[2].private_data = nullptr;

	// Build the union array
	ArrowArray union_array;
	const void *union_buffers[1] = {type_ids};
	ArrowArray *union_child_array_ptrs[3] = {&child_arrays[0], &child_arrays[1], &child_arrays[2]};
	union_array.length = static_cast<int64_t>(n_rows);
	union_array.null_count = 0;
	union_array.offset = 0;
	union_array.n_buffers = 1;
	union_array.buffers = union_buffers;
	union_array.n_children = 3;
	union_array.children = union_child_array_ptrs;
	union_array.dictionary = nullptr;
	union_array.release = NoOpArrayRelease;
	union_array.private_data = nullptr;

	// Build the root struct array
	ArrowArray root_array;
	ArrowArray *root_child_array_ptrs[1] = {&union_array};
	const void *root_buffers[1] = {nullptr};
	root_array.length = static_cast<int64_t>(n_rows);
	root_array.null_count = 0;
	root_array.offset = 0;
	root_array.n_buffers = 1;
	root_array.buffers = root_buffers;
	root_array.n_children = 1;
	root_array.children = root_child_array_ptrs;
	root_array.dictionary = nullptr;
	root_array.release = NoOpArrayRelease;
	root_array.private_data = nullptr;

	// Wrap into a stream
	ArrowArrayStream stream;
	stream.release = nullptr;
	AdbcError adbc_error;
	adbc_error.release = nullptr;
	auto status = duckdb_adbc::BatchToArrayStream(&root_array, &root_schema, &stream, &adbc_error);
	REQUIRE(status == ADBC_STATUS_OK);

	// Scan via DuckDB
	DuckDB db(nullptr);
	Connection conn(db);

	auto params = ArrowTestHelper::ConstructArrowScan(stream);
	auto result = ArrowTestHelper::ScanArrowObject(conn, params);
	REQUIRE(result);
	REQUIRE(!result->HasError());

	auto chunk = result->Fetch();
	REQUIRE(chunk);
	REQUIRE(chunk->size() == n_rows);
	REQUIRE(chunk->ColumnCount() == 1);

	auto &col = chunk->data[0];

	// Row 0: type_id=5 -> child 0 (int32), value=42
	auto val0 = col.GetValue(0);
	REQUIRE(!val0.IsNull());
	REQUIRE(UnionValue::GetTag(val0) == 0);
	REQUIRE(UnionValue::GetValue(val0) == Value::INTEGER(42));

	// Row 1: type_id=7 -> child 1 (varchar), value="hello"
	auto val1 = col.GetValue(1);
	REQUIRE(!val1.IsNull());
	REQUIRE(UnionValue::GetTag(val1) == 1);
	REQUIRE(UnionValue::GetValue(val1) == Value("hello"));

	// Row 2: type_id=9 -> child 2 (float), value=3.14
	auto val2 = col.GetValue(2);
	REQUIRE(!val2.IsNull());
	REQUIRE(UnionValue::GetTag(val2) == 2);
	REQUIRE(UnionValue::GetValue(val2).GetValue<float>() == Approx(3.14f));

	// Row 3: type_id=5 -> child 0 (int32), value=100
	auto val3 = col.GetValue(3);
	REQUIRE(!val3.IsNull());
	REQUIRE(UnionValue::GetTag(val3) == 0);
	REQUIRE(UnionValue::GetValue(val3) == Value::INTEGER(100));

	// Row 4: type_id=7 -> child 1 (varchar), value="world"
	auto val4 = col.GetValue(4);
	REQUIRE(!val4.IsNull());
	REQUIRE(UnionValue::GetTag(val4) == 1);
	REQUIRE(UnionValue::GetValue(val4) == Value("world"));

	// Row 5: type_id=9 -> child 2 (float), value=2.72
	auto val5 = col.GetValue(5);
	REQUIRE(!val5.IsNull());
	REQUIRE(UnionValue::GetTag(val5) == 2);
	REQUIRE(UnionValue::GetValue(val5).GetValue<float>() == Approx(2.72f));

	// Fully consume the result before cleanup
	auto chunk2 = result->Fetch();
	REQUIRE(!chunk2);
	result.reset();
}
