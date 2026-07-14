// ColumnArrowToDuckDBDictionary must read the indices validity from the same effective offset as the
// indices (nested_offset); the no-gap control pins the trigger to the offsets gap, not multi-chunking.

#include "catch.hpp"

#include "arrow/arrow_test_helper.hpp"
#include "duckdb/common/adbc/single_batch_array_stream.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"

#include <string>
#include <vector>

using namespace duckdb;

namespace {

// Stack-allocated Arrow structs need a no-op release; uniquely named to avoid ODR clashes.
void NestedOffReleaseSchema(ArrowSchema *s) {
	s->release = nullptr;
}
void NestedOffReleaseArray(ArrowArray *a) {
	a->release = nullptr;
}

const int32_t DICT_VALUES[3] = {10, 20, 30};

// Dict-encoded int32 child: slot j -> DICT_VALUES[j % 3], NULL at the given slots. Not copyable:
// the Arrow structs point into the object itself.
struct DictChild {
	DictChild(idx_t len, const std::vector<idx_t> &null_slots)
	    // padded so GetValidityMask's realignment branch (reads one extra byte) stays in bounds
	    : indices(len), validity((len + 7) / 8 + 2, 0xFF) {
		for (idx_t j = 0; j < len; j++) {
			indices[j] = int32_t(j % 3);
		}
		for (auto slot : null_slots) {
			validity[slot / 8] &= uint8_t(~(1u << (slot % 8)));
		}
		dict_schema.format = "i";
		dict_schema.release = NestedOffReleaseSchema;
		schema.format = "i";
		schema.name = "item";
		schema.flags = 2; // ARROW_FLAG_NULLABLE
		schema.dictionary = &dict_schema;
		schema.release = NestedOffReleaseSchema;
		dict_buffers[0] = nullptr;
		dict_buffers[1] = DICT_VALUES;
		dict_array.length = 3;
		dict_array.n_buffers = 2;
		dict_array.buffers = dict_buffers;
		dict_array.release = NestedOffReleaseArray;
		buffers[0] = validity.data();
		buffers[1] = indices.data();
		array.length = int64_t(len);
		array.null_count = int64_t(null_slots.size());
		array.n_buffers = 2;
		array.buffers = buffers;
		array.dictionary = &dict_array;
		array.release = NestedOffReleaseArray;
	}
	DictChild(const DictChild &) = delete;

	bool IsNull(idx_t slot) const {
		return (validity[slot / 8] & (1u << (slot % 8))) == 0;
	}
	int32_t ValueAt(idx_t slot) const {
		return DICT_VALUES[indices[slot] % 3];
	}

	std::vector<int32_t> indices;
	std::vector<uint8_t> validity;
	const void *dict_buffers[2];
	const void *buffers[2];
	ArrowSchema dict_schema {};
	ArrowSchema schema {};
	ArrowArray dict_array {};
	ArrowArray array {};
};

// Asserts one converted element matches the dict child at `slot`.
void RequireSlot(const DictChild &child, idx_t slot, const Value &elem) {
	INFO("child slot " << slot);
	if (child.IsNull(slot)) {
		REQUIRE(elem.IsNull());
	} else {
		REQUIRE(!elem.IsNull());
		REQUIRE(elem.GetValue<int32_t>() == child.ValueAt(slot));
	}
}

// One-element lists with offsets[0] == gap, so row r references child slot gap + r.
std::vector<int32_t> OneElementListOffsets(idx_t rows, idx_t gap) {
	std::vector<int32_t> offsets(rows + 1);
	for (idx_t i = 0; i <= rows; i++) {
		offsets[i] = int32_t(gap + i);
	}
	return offsets;
}

// Drives ColumnArrowToDuckDB on LIST(dict-int32), optionally via a STRUCT wrapper, for one chunk of
// `size` rows starting at `chunk_offset`; row i must read child slot gap + chunk_offset + i.
void CheckListOfDict(idx_t total_rows, idx_t chunk_offset, idx_t size, idx_t gap, const std::vector<idx_t> &null_slots,
                     bool wrap_struct = false) {
	DictChild child(gap + total_rows, null_slots);

	ArrowSchema *struct_schema_children[1] = {&child.schema};
	ArrowSchema struct_schema {};
	struct_schema.format = "+s";
	struct_schema.name = "s";
	struct_schema.flags = 2;
	struct_schema.n_children = 1;
	struct_schema.children = struct_schema_children;
	struct_schema.release = NestedOffReleaseSchema;

	const void *struct_buffers[1] = {nullptr};
	ArrowArray *struct_array_children[1] = {&child.array};
	ArrowArray struct_array {};
	struct_array.length = child.array.length;
	struct_array.n_buffers = 1;
	struct_array.buffers = struct_buffers;
	struct_array.n_children = 1;
	struct_array.children = struct_array_children;
	struct_array.release = NestedOffReleaseArray;

	auto offsets = OneElementListOffsets(total_rows, gap);
	ArrowSchema *list_schema_children[1] = {wrap_struct ? &struct_schema : &child.schema};
	ArrowSchema list_schema {};
	list_schema.format = "+l";
	list_schema.name = "l";
	list_schema.flags = 2;
	list_schema.n_children = 1;
	list_schema.children = list_schema_children;
	list_schema.release = NestedOffReleaseSchema;

	const void *list_buffers[2] = {nullptr, offsets.data()};
	ArrowArray *list_array_children[1] = {wrap_struct ? &struct_array : &child.array};
	ArrowArray list_array {};
	list_array.length = int64_t(total_rows);
	list_array.n_buffers = 2;
	list_array.buffers = list_buffers;
	list_array.n_children = 1;
	list_array.children = list_array_children;
	list_array.release = NestedOffReleaseArray;

	DuckDB db(nullptr);
	Connection con(db);
	auto &context = *con.context;
	auto arrow_type = ArrowType::GetArrowLogicalType(context, list_schema);
	ArrowArrayScanState state(context);
	state.owned_data = make_shared_ptr<ArrowArrayWrapper>();

	// the real scan builds and caches the dictionary on the first chunk; later chunks must reuse it
	if (chunk_offset > 0) {
		Vector warmup(arrow_type->GetDuckType(true), STANDARD_VECTOR_SIZE);
		ArrowToDuckDBConversion::ColumnArrowToDuckDB(warmup, list_array, 0, state, STANDARD_VECTOR_SIZE, *arrow_type,
		                                             -1);
	}

	Vector result(arrow_type->GetDuckType(true), size);
	ArrowToDuckDBConversion::ColumnArrowToDuckDB(result, list_array, chunk_offset, state, size, *arrow_type, -1);

	for (idx_t i = 0; i < size; i++) {
		INFO("row " << i);
		auto row = result.GetValue(i);
		auto &elems = ListValue::GetChildren(row);
		REQUIRE(elems.size() == 1);
		Value elem = wrap_struct ? StructValue::GetChildren(elems[0])[0] : elems[0];
		RequireSlot(child, gap + chunk_offset + i, elem);
	}
}

// Drives ColumnArrowToDuckDB on fixed-size ARRAY(dict-int32, k) with nonzero array.offset; row i
// element e must read child slot (array_offset + i) * k + e.
void CheckFixedArrayOfDict(idx_t k, idx_t array_offset, idx_t size, const std::vector<idx_t> &null_slots) {
	DictChild child((array_offset + size) * k, null_slots);

	std::string fmt = "+w:" + std::to_string(k);
	ArrowSchema *arr_schema_children[1] = {&child.schema};
	ArrowSchema arr_schema {};
	arr_schema.format = fmt.c_str();
	arr_schema.name = "arr";
	arr_schema.flags = 2;
	arr_schema.n_children = 1;
	arr_schema.children = arr_schema_children;
	arr_schema.release = NestedOffReleaseSchema;

	const void *arr_buffers[1] = {nullptr};
	ArrowArray *arr_array_children[1] = {&child.array};
	ArrowArray arr_array {};
	arr_array.length = int64_t(size);
	arr_array.offset = int64_t(array_offset);
	arr_array.n_buffers = 1;
	arr_array.buffers = arr_buffers;
	arr_array.n_children = 1;
	arr_array.children = arr_array_children;
	arr_array.release = NestedOffReleaseArray;

	DuckDB db(nullptr);
	Connection con(db);
	auto &context = *con.context;
	auto arrow_type = ArrowType::GetArrowLogicalType(context, arr_schema);
	ArrowArrayScanState state(context);
	state.owned_data = make_shared_ptr<ArrowArrayWrapper>();

	Vector result(arrow_type->GetDuckType(true), size);
	ArrowToDuckDBConversion::ColumnArrowToDuckDB(result, arr_array, 0, state, size, *arrow_type, -1);

	for (idx_t i = 0; i < size; i++) {
		INFO("row " << i);
		auto row = result.GetValue(i);
		auto &elems = ListValue::GetChildren(row);
		REQUIRE(elems.size() == k);
		for (idx_t e = 0; e < k; e++) {
			RequireSlot(child, (array_offset + i) * k + e, elems[e]);
		}
	}
}

} // namespace

TEST_CASE("Dictionary child of LIST: single chunk with nonzero leading child offset", "[arrow]") {
	// offsets[0] = 5: slot 10 is row 5, slot 0 is unused
	CheckListOfDict(8, 0, 8, 5, {0, 10});
}

TEST_CASE("Dictionary child of LIST: multi-chunk, no leading gap (control, must pass)", "[arrow]") {
	// second chunk with start_offset == chunk_offset == 2048: no divergence, passes even with the bug
	CheckListOfDict(4096, 2048, 2048, 0, {3000});
}

TEST_CASE("Dictionary child of LIST: multi-chunk with leading gap (chunk_offset diverges)", "[arrow]") {
	// second chunk with start_offset 2055 != chunk_offset 2048; slot 2055 is row 0
	CheckListOfDict(4096, 2048, 2048, 7, {2055});
}

TEST_CASE("Dictionary child of fixed-size ARRAY: nonzero array.offset", "[arrow]") {
	// array.offset = 2 shifts the child window by offset * k = 6; slot 6 is row 0 elem 0
	CheckFixedArrayOfDict(3, 2, 4, {0, 6});
}

TEST_CASE("Dictionary inside STRUCT nested in LIST: nonzero leading child offset", "[arrow]") {
	// the STRUCT branch forwards nested_offset to its dict child
	CheckListOfDict(8, 0, 8, 5, {0, 10}, /*wrap_struct=*/true);
}

// End-to-end through the real arrow scan; a nonzero offsets[0] is what pyarrow slicing produces.
TEST_CASE("Arrow scan of LIST(dict-encoded int32) with nonzero leading list offset", "[arrow]") {
	const idx_t rows = 8;
	const idx_t gap = 5;
	DictChild child(gap + rows, {0, 6, 10});
	auto offsets = OneElementListOffsets(rows, gap);

	ArrowSchema *list_schema_children[1] = {&child.schema};
	ArrowSchema list_schema {};
	list_schema.format = "+l";
	list_schema.name = "a";
	list_schema.flags = 2;
	list_schema.n_children = 1;
	list_schema.children = list_schema_children;
	list_schema.release = NestedOffReleaseSchema;

	ArrowSchema *record_schema_children[1] = {&list_schema};
	ArrowSchema record_schema {};
	record_schema.format = "+s";
	record_schema.n_children = 1;
	record_schema.children = record_schema_children;
	record_schema.release = NestedOffReleaseSchema;

	const void *list_buffers[2] = {nullptr, offsets.data()};
	ArrowArray *list_array_children[1] = {&child.array};
	ArrowArray list_array {};
	list_array.length = int64_t(rows);
	list_array.n_buffers = 2;
	list_array.buffers = list_buffers;
	list_array.n_children = 1;
	list_array.children = list_array_children;
	list_array.release = NestedOffReleaseArray;

	const void *record_buffers[1] = {nullptr};
	ArrowArray *record_array_children[1] = {&list_array};
	ArrowArray record_array {};
	record_array.length = int64_t(rows);
	record_array.n_buffers = 1;
	record_array.buffers = record_buffers;
	record_array.n_children = 1;
	record_array.children = record_array_children;
	record_array.release = NestedOffReleaseArray;

	ArrowArrayStream stream {};
	AdbcError err {};
	REQUIRE(duckdb_adbc::BatchToArrayStream(&record_array, &record_schema, &stream, &err) == ADBC_STATUS_OK);

	DuckDB db(nullptr);
	Connection con(db);
	auto params = ArrowTestHelper::ConstructArrowScan(stream);
	auto result = ArrowTestHelper::ScanArrowObject(con, params);
	stream.release = nullptr;
	REQUIRE(result);

	auto &materialized = result->Cast<MaterializedQueryResult>();
	REQUIRE(materialized.RowCount() == rows);
	for (idx_t r = 0; r < rows; r++) {
		INFO("row " << r);
		auto row = materialized.GetValue(0, r);
		auto &elems = ListValue::GetChildren(row);
		REQUIRE(elems.size() == 1);
		RequireSlot(child, gap + r, elems[0]);
	}
}
