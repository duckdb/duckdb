#include "catch.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/enums/debug_verification_mode.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector/array_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/main/config.hpp"

#include <cstring>

// Vector copies must not read the (undefined) descendants of NULL ARRAY/STRUCT rows and must mark
// them NULL recursively in the target, even when the source marks them valid over garbage.

using namespace duckdb;

namespace {

constexpr idx_t ARRAY_SIZE = 3;

enum class Shape { ARRAY_VARCHAR, ARRAY_STRUCT_VARCHAR, STRUCT_ARRAY_VARCHAR, ARRAY_LIST_VARCHAR, STRUCT_LIST_VARCHAR };

LogicalType StructOfVarchar() {
	return LogicalType::STRUCT({{"s", LogicalType::VARCHAR}});
}

LogicalType MakeType(Shape shape) {
	switch (shape) {
	case Shape::ARRAY_VARCHAR:
		return LogicalType::ARRAY(LogicalType::VARCHAR, ARRAY_SIZE);
	case Shape::ARRAY_STRUCT_VARCHAR:
		return LogicalType::ARRAY(StructOfVarchar(), ARRAY_SIZE);
	case Shape::STRUCT_ARRAY_VARCHAR:
		return LogicalType::STRUCT({{"arr", LogicalType::ARRAY(LogicalType::VARCHAR, ARRAY_SIZE)}});
	case Shape::ARRAY_LIST_VARCHAR:
		return LogicalType::ARRAY(LogicalType::LIST(LogicalType::VARCHAR), ARRAY_SIZE);
	default:
		return LogicalType::STRUCT({{"l", LogicalType::LIST(LogicalType::VARCHAR)}});
	}
}

// > 12 bytes so copies go through StringHeap::AddBlob's heap path
string LeafString(idx_t row, idx_t elem) {
	return "val-r" + to_string(row) + "-e" + to_string(elem) + "-padding";
}

Value LeafList(idx_t row, idx_t elem) {
	duckdb::vector<Value> strings;
	for (idx_t s = 0; s <= elem; s++) {
		strings.push_back(Value(LeafString(row, elem * 10 + s)));
	}
	return Value::LIST(LogicalType::VARCHAR, strings);
}

Value ValidValue(Shape shape, idx_t row) {
	switch (shape) {
	case Shape::ARRAY_VARCHAR: {
		duckdb::vector<Value> leaves;
		for (idx_t e = 0; e < ARRAY_SIZE; e++) {
			leaves.push_back(Value(LeafString(row, e)));
		}
		return Value::ARRAY(LogicalType::VARCHAR, leaves);
	}
	case Shape::ARRAY_STRUCT_VARCHAR: {
		duckdb::vector<Value> structs;
		for (idx_t e = 0; e < ARRAY_SIZE; e++) {
			structs.push_back(Value::STRUCT({{"s", Value(LeafString(row, e))}}));
		}
		return Value::ARRAY(StructOfVarchar(), structs);
	}
	case Shape::STRUCT_ARRAY_VARCHAR: {
		duckdb::vector<Value> leaves;
		for (idx_t e = 0; e < ARRAY_SIZE; e++) {
			leaves.push_back(Value(LeafString(row, e)));
		}
		return Value::STRUCT({{"arr", Value::ARRAY(LogicalType::VARCHAR, leaves)}});
	}
	case Shape::ARRAY_LIST_VARCHAR: {
		duckdb::vector<Value> lists;
		for (idx_t e = 0; e < ARRAY_SIZE; e++) {
			lists.push_back(LeafList(row, e));
		}
		return Value::ARRAY(LogicalType::LIST(LogicalType::VARCHAR), lists);
	}
	default:
		return Value::STRUCT({{"l", LeafList(row, 2)}});
	}
}

Vector &NestedChild(Shape shape, Vector &parent) {
	switch (shape) {
	case Shape::ARRAY_VARCHAR:
	case Shape::ARRAY_STRUCT_VARCHAR:
	case Shape::ARRAY_LIST_VARCHAR:
		return ArrayVector::GetChildMutable(parent);
	default:
		return StructVector::GetEntries(parent)[0];
	}
}

// claims 16 MiB over a 16-byte buffer; any payload read faults under ASAN
string_t MakeUndefinedPayload(unsafe_unique_array<char> &backing) {
	backing = make_unsafe_uniq_array<char>(16);
	memset(backing.get(), 'x', 16);
	return string_t(backing.get(), 1u << 24);
}

void PlantVarcharGarbage(Vector &leaf, idx_t slot_start, idx_t slot_count, string_t payload) {
	auto &leaf_validity = FlatVector::ValidityMutable(leaf);
	auto leaf_data = FlatVector::GetDataMutable<string_t>(leaf);
	for (idx_t s = 0; s < slot_count; s++) {
		leaf_validity.SetValid(slot_start + s);
		leaf_data[slot_start + s] = payload;
	}
}

// a copy that reads these entries builds a ~10^12-entry child selection (hang / OOM)
void PlantListGarbage(Vector &list, idx_t slot_start, idx_t slot_count) {
	auto &list_validity = FlatVector::ValidityMutable(list);
	auto list_data = FlatVector::GetDataMutable<list_entry_t>(list);
	for (idx_t s = 0; s < slot_count; s++) {
		list_validity.SetValid(slot_start + s);
		list_data[slot_start + s] = list_entry_t(1ULL << 40, 1ULL << 40);
	}
}

// mark a NULL parent row's descendants valid over garbage, as Arrow-ingested data can
void PlantUndefined(Shape shape, Vector &parent, idx_t row, string_t payload) {
	auto &child = NestedChild(shape, parent);
	switch (shape) {
	case Shape::ARRAY_VARCHAR:
		PlantVarcharGarbage(child, row * ARRAY_SIZE, ARRAY_SIZE, payload);
		break;
	case Shape::ARRAY_STRUCT_VARCHAR: {
		auto &sv_validity = FlatVector::ValidityMutable(child);
		for (idx_t e = 0; e < ARRAY_SIZE; e++) {
			sv_validity.SetValid(row * ARRAY_SIZE + e);
		}
		PlantVarcharGarbage(StructVector::GetEntries(child)[0], row * ARRAY_SIZE, ARRAY_SIZE, payload);
		break;
	}
	case Shape::STRUCT_ARRAY_VARCHAR:
		FlatVector::ValidityMutable(child).SetValid(row);
		PlantVarcharGarbage(ArrayVector::GetChildMutable(child), row * ARRAY_SIZE, ARRAY_SIZE, payload);
		break;
	case Shape::ARRAY_LIST_VARCHAR:
		PlantListGarbage(child, row * ARRAY_SIZE, ARRAY_SIZE);
		break;
	default:
		PlantListGarbage(child, row, 1);
		break;
	}
}

// the deepest sub-vector under a NULL row must read NULL in the target (proves the recursive SetNull)
void RequireSkippedNull(Shape shape, Vector &target_parent, idx_t row) {
	auto &child = NestedChild(shape, target_parent);
	reference<Vector> leaf(child);
	idx_t slot_start = row * ARRAY_SIZE;
	idx_t slot_count = ARRAY_SIZE;
	switch (shape) {
	case Shape::ARRAY_STRUCT_VARCHAR:
		leaf = StructVector::GetEntries(child)[0];
		break;
	case Shape::STRUCT_ARRAY_VARCHAR:
		leaf = ArrayVector::GetChildMutable(child);
		break;
	case Shape::STRUCT_LIST_VARCHAR:
		slot_start = row;
		slot_count = 1;
		break;
	default:
		break;
	}
	auto &leaf_validity = FlatVector::Validity(leaf.get());
	for (idx_t s = 0; s < slot_count; s++) {
		REQUIRE(!leaf_validity.RowIsValid(slot_start + s));
	}
}

// a source chunk whose NULL parent rows carry valid-flagged garbage descendants
struct CopyFixture {
	CopyFixture(Shape shape, const duckdb::vector<bool> &is_null) : shape(shape), is_null(is_null) {
		auto type = MakeType(shape);
		auto payload = MakeUndefinedPayload(backing);
		source.Initialize(allocator, {type});
		source.SetChildCardinality(is_null.size());
		expected.resize(is_null.size());
		for (idx_t r = 0; r < is_null.size(); r++) {
			if (is_null[r]) {
				source.data[0].SetValue(r, Value(type));
			} else {
				expected[r] = ValidValue(shape, r);
				source.data[0].SetValue(r, expected[r]);
			}
		}
		for (idx_t r = 0; r < is_null.size(); r++) {
			if (is_null[r]) {
				PlantUndefined(shape, source.data[0], r, payload);
			}
		}
	}

	void RequireRow(Vector &target, idx_t target_row, idx_t src_row) {
		if (is_null[src_row]) {
			REQUIRE(target.GetValue(target_row).IsNull());
			RequireSkippedNull(shape, target, target_row);
		} else {
			REQUIRE(target.GetValue(target_row) == expected[src_row]);
		}
	}

	Shape shape;
	duckdb::vector<bool> is_null;
	Allocator allocator;
	unsafe_unique_array<char> backing;
	DataChunk source;
	duckdb::vector<Value> expected;
};

// plain chunk copy - the streaming/buffered result path (SimpleBufferedData::Append)
void RunCase(Shape shape, const duckdb::vector<bool> &is_null) {
	CopyFixture f(shape, is_null);
	DataChunk target;
	target.Initialize(f.allocator, {MakeType(shape)});
	f.source.Copy(target, 0);
	target.data[0].Verify();
	for (idx_t r = 0; r < is_null.size(); r++) {
		f.RequireRow(target.data[0], r, r);
	}
}

// copy with a reordering source selection (non-identity source_sel)
void RunReorderCase(Shape shape, const duckdb::vector<bool> &is_null) {
	CopyFixture f(shape, is_null);
	auto row_count = is_null.size();
	SelectionVector reorder(row_count);
	for (idx_t i = 0; i < row_count; i++) {
		reorder.set_index(i, row_count - 1 - i);
	}
	DataChunk target;
	target.Initialize(f.allocator, {MakeType(shape)});
	target.data[0].Copy(f.source.data[0], reorder, row_count, 0, 0, row_count);
	FlatVector::SetSize(target.data[0], row_count);
	target.data[0].Verify();
	for (idx_t i = 0; i < row_count; i++) {
		f.RequireRow(target.data[0], i, row_count - 1 - i);
	}
}

// append onto a non-empty target (target_offset > 0)
void RunAppendCase(Shape shape, const duckdb::vector<bool> &is_null) {
	CopyFixture f(shape, is_null);
	const idx_t prefix = 2;
	DataChunk target;
	target.Initialize(f.allocator, {MakeType(shape)});
	duckdb::vector<Value> prefix_vals;
	for (idx_t p = 0; p < prefix; p++) {
		prefix_vals.push_back(ValidValue(shape, 100 + p));
		target.data[0].SetValue(p, prefix_vals[p]);
	}
	FlatVector::SetSize(target.data[0], prefix);
	target.data[0].Append(f.source.data[0], is_null.size());
	target.data[0].Verify();
	for (idx_t p = 0; p < prefix; p++) {
		REQUIRE(target.data[0].GetValue(p) == prefix_vals[p]);
	}
	for (idx_t r = 0; r < is_null.size(); r++) {
		f.RequireRow(target.data[0], prefix + r, r);
	}
}

void RunCorpus(Shape shape) {
	duckdb::vector<duckdb::vector<bool>> patterns = {
	    {true, false, false, false, false}, // start
	    {false, false, true, false, false}, // middle
	    {false, false, false, false, true}, // end
	    {true, true, true, true, true},     // all NULL
	    {false, true, false, true, false},  // interleaved
	};
	for (auto &pattern : patterns) {
		RunCase(shape, pattern);
	}
	// reorder/append add no run-boundary logic of their own; the interleaved pattern covers them
	RunReorderCase(shape, patterns.back());
	RunAppendCase(shape, patterns.back());
}

} // namespace

TEST_CASE("copy ARRAY(VARCHAR) must not read child payloads under NULL rows", "[copy]") {
	RunCorpus(Shape::ARRAY_VARCHAR);
}

TEST_CASE("copy ARRAY(STRUCT(VARCHAR)) must not read grandchild payloads under NULL rows", "[copy]") {
	RunCorpus(Shape::ARRAY_STRUCT_VARCHAR);
}

TEST_CASE("copy STRUCT(ARRAY(VARCHAR)) must not read grandchild payloads under NULL rows", "[copy]") {
	RunCorpus(Shape::STRUCT_ARRAY_VARCHAR);
}

TEST_CASE("copy ARRAY(LIST(VARCHAR)) must not read list entries under NULL rows", "[copy]") {
	RunCorpus(Shape::ARRAY_LIST_VARCHAR);
}

TEST_CASE("copy STRUCT(LIST(VARCHAR)) must not read list entries under NULL rows", "[copy]") {
	RunCorpus(Shape::STRUCT_LIST_VARCHAR);
}

namespace {

// enable aggressive vector verification for a scope, restoring the previous mode on exit
struct VerifyVectorsGuard {
	VerifyVectorsGuard() : previous(DBConfigOptions::global_verification_mode) {
		DBConfigOptions::global_verification_mode = DebugVerificationMode::VERIFY_VECTORS;
	}
	~VerifyVectorsGuard() {
		DBConfigOptions::global_verification_mode = previous;
	}
	DebugVerificationMode previous;
};

} // namespace

TEST_CASE("Verify rejects non-NULL children under a NULL ARRAY row", "[copy]") {
#ifdef DUCKDB_CRASH_ON_ASSERT
	// the provoked InternalException aborts instead of throwing in crash-on-assert builds
	return;
#endif
	const idx_t row_count = 4;
	const idx_t null_row = 1;

	// ARRAY(child_type, ARRAY_SIZE) with every row valid over safe inlined strings
	auto make_array = [&](const LogicalType &child_type) {
		Vector v(LogicalType::ARRAY(child_type, ARRAY_SIZE));
		for (idx_t r = 0; r < row_count; r++) {
			duckdb::vector<Value> leaves;
			for (idx_t e = 0; e < ARRAY_SIZE; e++) {
				leaves.push_back(child_type.id() == LogicalTypeId::STRUCT ? Value::STRUCT({{"s", Value("ok")}})
				                                                          : Value("ok"));
			}
			v.SetValue(r, Value::ARRAY(child_type, leaves));
		}
		FlatVector::SetSize(v, row_count);
		return v;
	};

	SECTION("well-formed NULL row passes") {
		auto v = make_array(LogicalType::VARCHAR);
		// SetNull recursively nulls the child slots too, as the Copy fix does for normalized targets
		FlatVector::SetNull(v, null_row, true);
		VerifyVectorsGuard guard;
		REQUIRE_NOTHROW(v.Verify());
	}

	SECTION("full-vector Verify") {
		auto v = make_array(LogicalType::VARCHAR);
		// mark the parent row NULL without touching its (still valid) child slots
		FlatVector::ValidityMutable(v).SetInvalid(null_row);
		VerifyVectorsGuard guard;
		REQUIRE_THROWS(v.Verify());
	}

	SECTION("sliced Verify") {
		auto v = make_array(LogicalType::VARCHAR);
		FlatVector::ValidityMutable(v).SetInvalid(null_row);
		SelectionVector sel(row_count);
		for (idx_t i = 0; i < row_count; i++) {
			sel.set_index(i, i);
		}
		VerifyVectorsGuard guard;
		REQUIRE_THROWS(v.Verify(sel, row_count));
	}

	SECTION("grandchild under a correctly-NULL struct is caught by recursion (full path)") {
		auto v = make_array(StructOfVarchar());
		auto &struct_child = ArrayVector::GetChildMutable(v);
		// parent array row NULL, its struct child slots correctly NULL, grandchild left valid
		FlatVector::ValidityMutable(v).SetInvalid(null_row);
		for (idx_t e = 0; e < ARRAY_SIZE; e++) {
			FlatVector::ValidityMutable(struct_child).SetInvalid(null_row * ARRAY_SIZE + e);
		}
		VerifyVectorsGuard guard;
		string message;
		try {
			v.Verify();
		} catch (const std::exception &ex) {
			message = ex.what();
		}
		REQUIRE(message.find("Struct NULL mismatch") != string::npos);
	}

	SECTION("grandchild under a correctly-NULL struct is caught by recursion (sliced path)") {
		auto v = make_array(StructOfVarchar());
		auto &struct_child = ArrayVector::GetChildMutable(v);
		FlatVector::ValidityMutable(v).SetInvalid(null_row);
		for (idx_t e = 0; e < ARRAY_SIZE; e++) {
			FlatVector::ValidityMutable(struct_child).SetInvalid(null_row * ARRAY_SIZE + e);
		}
		SelectionVector sel(row_count);
		for (idx_t i = 0; i < row_count; i++) {
			sel.set_index(i, i);
		}
		VerifyVectorsGuard guard;
		string message;
		try {
			v.Verify(sel, row_count);
		} catch (const std::exception &ex) {
			message = ex.what();
		}
		// the struct's own full-size NULL scan catches it even though the array sel excludes NULL rows
		REQUIRE(message.find("Struct NULL mismatch") != string::npos);
	}
}

namespace {

void ScanSingleChunk(ColumnDataCollection &collection, DataChunk &result) {
	collection.InitializeScanChunk(result);
	ColumnDataScanState state;
	collection.InitializeScan(state);
	REQUIRE(collection.Scan(state, result));
}

} // namespace

TEST_CASE("ColumnDataCollection append must not read child payloads under NULL rows", "[copy]") {
	if (STANDARD_VECTOR_SIZE < 8) {
		// the scan-back assumes all rows fit in a single chunk
		return;
	}
	Allocator allocator;

	SECTION("STRUCT(VARCHAR): garbage under a NULL row") {
		auto type = StructOfVarchar();
		unsafe_unique_array<char> backing;
		auto payload = MakeUndefinedPayload(backing);
		const idx_t row_count = 3;
		const idx_t null_row = 1;
		DataChunk chunk;
		chunk.Initialize(allocator, {type});
		chunk.SetChildCardinality(row_count);
		duckdb::vector<Value> expected(row_count);
		for (idx_t r = 0; r < row_count; r++) {
			if (r == null_row) {
				chunk.data[0].SetValue(r, Value(type));
			} else {
				expected[r] = Value::STRUCT({{"s", Value(LeafString(r, 0))}});
				chunk.data[0].SetValue(r, expected[r]);
			}
		}
		PlantVarcharGarbage(StructVector::GetEntries(chunk.data[0])[0], null_row, 1, payload);

		ColumnDataCollection collection(allocator, {type});
		collection.Append(chunk);

		DataChunk result;
		ScanSingleChunk(collection, result);
		REQUIRE(result.size() == row_count);
		REQUIRE(result.data[0].GetValue(null_row).IsNull());
		// the child slot under the NULL row must read NULL in the collection
		REQUIRE(!FlatVector::Validity(StructVector::GetEntries(result.data[0])[0]).RowIsValid(null_row));
		for (idx_t r = 0; r < row_count; r++) {
			if (r != null_row) {
				REQUIRE(result.data[0].GetValue(r) == expected[r]);
			}
		}
	}

	SECTION("ARRAY(STRUCT(VARCHAR)): broadcast composes at depth 2") {
		CopyFixture f(Shape::ARRAY_STRUCT_VARCHAR, {false, true, false, true, false});
		ColumnDataCollection collection(f.allocator, {MakeType(Shape::ARRAY_STRUCT_VARCHAR)});
		collection.Append(f.source);

		DataChunk result;
		ScanSingleChunk(collection, result);
		REQUIRE(result.size() == f.is_null.size());
		for (idx_t r = 0; r < f.is_null.size(); r++) {
			f.RequireRow(result.data[0], r, r);
		}
	}

	SECTION("well-formed NULLs round-trip") {
		auto type = MakeType(Shape::ARRAY_STRUCT_VARCHAR);
		const idx_t row_count = 4;
		DataChunk chunk;
		chunk.Initialize(allocator, {type});
		chunk.SetChildCardinality(row_count);
		duckdb::vector<Value> expected(row_count);
		for (idx_t r = 0; r < row_count; r++) {
			expected[r] = r % 2 ? Value(type) : ValidValue(Shape::ARRAY_STRUCT_VARCHAR, r);
			chunk.data[0].SetValue(r, expected[r]);
		}
		ColumnDataCollection collection(allocator, {type});
		collection.Append(chunk);

		DataChunk result;
		ScanSingleChunk(collection, result);
		for (idx_t r = 0; r < row_count; r++) {
			if (r % 2) {
				REQUIRE(result.data[0].GetValue(r).IsNull());
			} else {
				REQUIRE(result.data[0].GetValue(r) == expected[r]);
			}
		}
	}

	SECTION("dictionary child sharing a slot with a NULL row is not corrupted") {
		auto type = StructOfVarchar();
		DataChunk chunk;
		chunk.Initialize(allocator, {type});
		chunk.SetChildCardinality(2);
		auto shared = Value::STRUCT({{"s", Value(LeafString(0, 0))}});
		chunk.data[0].SetValue(0, shared);
		chunk.data[0].SetValue(1, Value::STRUCT({{"s", Value(LeafString(1, 0))}}));
		// dictionary-encode the child so both rows read dict slot 0
		SelectionVector dict_sel(2);
		dict_sel.set_index(0, 0);
		dict_sel.set_index(1, 0);
		StructVector::GetEntries(chunk.data[0])[0].Slice(dict_sel, 2);
		// NULL row 1 via the raw parent mask - its shared child slot stays valid-flagged
		FlatVector::ValidityMutable(chunk.data[0]).SetInvalid(1);

		ColumnDataCollection collection(allocator, {type});
		collection.Append(chunk);

		DataChunk result;
		ScanSingleChunk(collection, result);
		REQUIRE(result.data[0].GetValue(0) == shared);
		REQUIRE(result.data[0].GetValue(1).IsNull());
		REQUIRE(!FlatVector::Validity(StructVector::GetEntries(result.data[0])[0]).RowIsValid(1));
	}
}
