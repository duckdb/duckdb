#include "catch.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/enums/debug_verification_mode.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector/array_vector.hpp"
#include "duckdb/common/vector/constant_vector.hpp"
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

// claims 16 MiB over a 16-byte buffer. Any payload read faults under ASAN
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
	if (STANDARD_VECTOR_SIZE < 8) {
		// the fixtures exceed the default vector capacity at tiny vector sizes
		return;
	}
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
	// reorder/append add no run-boundary logic of their own. The interleaved pattern covers them
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

namespace {

void ScanSingleChunk(ColumnDataCollection &collection, DataChunk &result); // defined below

// The child vector representations a STRUCT/ARRAY child can take. The nested-NULL copy paths must handle each.
// The engine keeps children in any of these, but only FLAT/CONSTANT were originally covered.
enum class ChildRep { FLAT, CONSTANT, DICTIONARY, SEQUENCE };

const char *RepName(ChildRep rep) {
	switch (rep) {
	case ChildRep::FLAT:
		return "FLAT";
	case ChildRep::CONSTANT:
		return "CONSTANT";
	case ChildRep::DICTIONARY:
		return "DICTIONARY";
	default:
		return "SEQUENCE";
	}
}

// Fill a freshly-allocated BIGINT leaf `child` (count logical rows) in representation `rep`. Logical row i holds
// base+i, except CONSTANT which holds base everywhere. The child is left fully valid on purpose: under a NULL
// parent it is then unreflected, which is exactly what the copy paths must isolate and NULL.
void FillBigintChild(Vector &child, ChildRep rep, idx_t count, int64_t base = 100) {
	switch (rep) {
	case ChildRep::SEQUENCE:
		child.Sequence(base, 1, count);
		return;
	case ChildRep::CONSTANT:
		child.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::GetData<int64_t>(child)[0] = base;
		return;
	default: {
		auto data = FlatVector::GetDataMutable<int64_t>(child);
		for (idx_t i = 0; i < count; i++) {
			data[i] = base + int64_t(i);
		}
		if (rep == ChildRep::DICTIONARY) {
			SelectionVector dict_sel(count);
			for (idx_t i = 0; i < count; i++) {
				dict_sel.set_index(i, i);
			}
			child.Slice(dict_sel, count);
		}
	}
	}
}

// Copy a STRUCT(i BIGINT) / ARRAY(BIGINT) whose child is in representation `rep` through a (possibly reversed)
// selection, starting at source_offset, then assert the target passes Verify (children under NULL rows were
// nulled) and every copied row matches. source_offset > 0 exercises the sequence/FSST/dictionary child vector
// flatten range that was the root cause of the sequence-child bug.
void RunRepCopy(const LogicalType &type, bool is_array, ChildRep rep, bool reverse, idx_t source_offset) {
	const idx_t row_count = 5;
	const idx_t copy_count = row_count - source_offset;
	const idx_t null_row = 1;
	Vector source(type);
	auto &child = is_array ? ArrayVector::GetChildMutable(source) : StructVector::GetEntries(source)[0];
	FillBigintChild(child, rep, is_array ? row_count * ARRAY_SIZE : row_count);
	FlatVector::SetSize(source, row_count);
	FlatVector::ValidityMutable(source).SetInvalid(null_row);

	SelectionVector sel(row_count);
	for (idx_t r = 0; r < row_count; r++) {
		sel.set_index(r, reverse ? row_count - r - 1 : r);
	}

	Vector target(type);
	target.Copy(source, sel, row_count, source_offset, 0, copy_count);
	FlatVector::SetSize(target, copy_count);

	VerifyVectorsGuard guard;
	target.Verify();
	for (idx_t r = 0; r < copy_count; r++) {
		auto src = sel.get_index(source_offset + r);
		INFO("stream rep=" << RepName(rep) << " reverse=" << reverse << " offset=" << source_offset << " row=" << r);
		if (src == null_row) {
			REQUIRE(target.GetValue(r).IsNull());
		} else {
			REQUIRE(target.GetValue(r) == source.GetValue(src));
		}
	}
}

// Same source shapes, but through the buffered ColumnDataCollection append + scan-back path.
void RunRepAppend(const LogicalType &type, bool is_array, ChildRep rep) {
	const idx_t row_count = 5;
	const idx_t null_row = 1;
	Allocator allocator;
	DataChunk chunk;
	chunk.Initialize(allocator, {type});
	chunk.SetChildCardinality(row_count);
	auto &source = chunk.data[0];
	auto &child = is_array ? ArrayVector::GetChildMutable(source) : StructVector::GetEntries(source)[0];
	FillBigintChild(child, rep, is_array ? row_count * ARRAY_SIZE : row_count);
	FlatVector::ValidityMutable(source).SetInvalid(null_row);
	duckdb::vector<Value> expected(row_count);
	for (idx_t r = 0; r < row_count; r++) {
		expected[r] = source.GetValue(r);
	}

	ColumnDataCollection collection(allocator, {type});
	collection.Append(chunk);
	DataChunk result;
	ScanSingleChunk(collection, result);
	for (idx_t r = 0; r < row_count; r++) {
		INFO("append rep=" << RepName(rep) << " row=" << r);
		if (r == null_row) {
			REQUIRE(result.data[0].GetValue(r).IsNull());
		} else {
			REQUIRE(result.data[0].GetValue(r) == expected[r]);
		}
	}
}

} // namespace

TEST_CASE("copy preserves NULLs across child vector representations", "[copy]") {
	if (STANDARD_VECTOR_SIZE < 8) {
		return;
	}
	auto struct_type = LogicalType::STRUCT({{"i", LogicalType::BIGINT}});
	auto array_type = LogicalType::ARRAY(LogicalType::BIGINT, ARRAY_SIZE);
	for (auto rep : {ChildRep::FLAT, ChildRep::CONSTANT, ChildRep::DICTIONARY, ChildRep::SEQUENCE}) {
		for (bool reverse : {false, true}) {
			for (idx_t source_offset : {idx_t(0), idx_t(1)}) {
				RunRepCopy(struct_type, false, rep, reverse, source_offset);
				RunRepCopy(array_type, true, rep, reverse, source_offset);
			}
		}
		RunRepAppend(struct_type, false, rep);
		RunRepAppend(array_type, true, rep);
	}
}

TEST_CASE("copy preserves NULLs for UNION and VARIANT (physical STRUCT)", "[copy]") {
	if (STANDARD_VECTOR_SIZE < 8) {
		return;
	}
	const idx_t row_count = 4;
	const idx_t null_row = 1;
	child_list_t<LogicalType> members;
	members.emplace_back("name", LogicalType::VARCHAR);
	members.emplace_back("age", LogicalType::SMALLINT);

	duckdb::vector<std::pair<LogicalType, duckdb::vector<Value>>> cases;
	{
		duckdb::vector<Value> vals;
		for (idx_t r = 0; r < row_count; r++) {
			vals.push_back(Value::UNION(members, 0, Value(LeafString(r, 0))));
		}
		cases.emplace_back(LogicalType::UNION(members), std::move(vals));
	}
	{
		auto variant_type = LogicalType::VARIANT();
		duckdb::vector<Value> vals;
		for (idx_t r = 0; r < row_count; r++) {
			vals.push_back(Value(LeafString(r, 0)).DefaultCastAs(variant_type));
		}
		cases.emplace_back(variant_type, std::move(vals));
	}

	for (auto &test_case : cases) {
		auto &type = test_case.first;
		auto &vals = test_case.second;
		// valid values, then raw-NULL the parent while its (physical STRUCT) children stay valid -> unreflected
		Vector source(type);
		for (idx_t r = 0; r < row_count; r++) {
			source.SetValue(r, vals[r]);
		}
		FlatVector::SetSize(source, row_count);
		FlatVector::ValidityMutable(source).SetInvalid(null_row);

		SelectionVector rev(row_count);
		for (idx_t r = 0; r < row_count; r++) {
			rev.set_index(r, row_count - r - 1);
		}
		Vector target(type);
		target.Copy(source, rev, row_count, 0, 0, row_count);
		FlatVector::SetSize(target, row_count);

		VerifyVectorsGuard guard;
		target.Verify();
		for (idx_t r = 0; r < row_count; r++) {
			auto src = rev.get_index(r);
			INFO("type=" << type.ToString() << " row=" << r);
			if (src == null_row) {
				REQUIRE(target.GetValue(r).IsNull());
			} else {
				REQUIRE(target.GetValue(r) == vals[src]);
			}
		}
	}
}

TEST_CASE("Verify rejects non-NULL children under a NULL ARRAY row", "[copy]") {
#ifdef DUCKDB_CRASH_ON_ASSERT
	// the provoked InternalException aborts instead of throwing in crash-on-assert builds
	return;
#endif
	if (STANDARD_VECTOR_SIZE < 8) {
		// the fixtures exceed the default vector capacity at tiny vector sizes
		return;
	}
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

	SECTION("dictionary child") {
		auto v = make_array(LogicalType::VARCHAR);
		auto &child = ArrayVector::GetChildMutable(v);
		SelectionVector dict_sel(child.size());
		for (idx_t i = 0; i < child.size(); i++) {
			dict_sel.set_index(i, 0);
		}
		child.Slice(dict_sel, child.size());
		FlatVector::ValidityMutable(v).SetInvalid(null_row);
		VerifyVectorsGuard guard;
		REQUIRE_THROWS(v.Verify());
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

	SECTION("grandchild under a correctly-NULL array is caught by recursion (sliced path)") {
		auto inner_type = LogicalType::ARRAY(LogicalType::VARCHAR, ARRAY_SIZE);
		Vector v(LogicalType::ARRAY(inner_type, ARRAY_SIZE));
		for (idx_t r = 0; r < row_count; r++) {
			duckdb::vector<Value> inner_arrays;
			for (idx_t e = 0; e < ARRAY_SIZE; e++) {
				duckdb::vector<Value> leaves(ARRAY_SIZE, Value("ok"));
				inner_arrays.push_back(Value::ARRAY(LogicalType::VARCHAR, leaves));
			}
			v.SetValue(r, Value::ARRAY(inner_type, inner_arrays));
		}
		FlatVector::SetSize(v, row_count);
		auto &inner_child = ArrayVector::GetChildMutable(v);
		FlatVector::ValidityMutable(v).SetInvalid(null_row);
		for (idx_t e = 0; e < ARRAY_SIZE; e++) {
			FlatVector::ValidityMutable(inner_child).SetInvalid(null_row * ARRAY_SIZE + e);
		}
		SelectionVector sel(row_count);
		for (idx_t i = 0; i < row_count; i++) {
			sel.set_index(i, i);
		}
		VerifyVectorsGuard guard;
		REQUIRE_THROWS(v.Verify(sel, row_count));
	}

	SECTION("deep violation is caught through two recursion levels (ARRAY(ARRAY(ARRAY)))") {
		auto lvl1 = LogicalType::ARRAY(LogicalType::VARCHAR, ARRAY_SIZE);
		auto lvl2 = LogicalType::ARRAY(lvl1, ARRAY_SIZE);
		Vector v(LogicalType::ARRAY(lvl2, ARRAY_SIZE));
		for (idx_t r = 0; r < row_count; r++) {
			duckdb::vector<Value> mids;
			for (idx_t a = 0; a < ARRAY_SIZE; a++) {
				duckdb::vector<Value> inners;
				for (idx_t b = 0; b < ARRAY_SIZE; b++) {
					duckdb::vector<Value> leaves(ARRAY_SIZE, Value("ok"));
					inners.push_back(Value::ARRAY(LogicalType::VARCHAR, leaves));
				}
				mids.push_back(Value::ARRAY(lvl1, inners));
			}
			v.SetValue(r, Value::ARRAY(lvl2, mids));
		}
		FlatVector::SetSize(v, row_count);
		auto &mid_child = ArrayVector::GetChildMutable(v);
		auto &inner_child = ArrayVector::GetChildMutable(mid_child);
		// outer row NULL. Middle and inner slots beneath it correctly NULL (reflected), but the deepest
		// VARCHAR left valid -> the only violation is two levels down, reachable only via the recursion
		FlatVector::ValidityMutable(v).SetInvalid(null_row);
		for (idx_t e = 0; e < ARRAY_SIZE; e++) {
			auto mid_slot = null_row * ARRAY_SIZE + e;
			FlatVector::ValidityMutable(mid_child).SetInvalid(mid_slot);
			for (idx_t f = 0; f < ARRAY_SIZE; f++) {
				FlatVector::ValidityMutable(inner_child).SetInvalid(mid_slot * ARRAY_SIZE + f);
			}
		}
		SelectionVector sel(row_count);
		for (idx_t i = 0; i < row_count; i++) {
			sel.set_index(i, i);
		}
		VerifyVectorsGuard guard;
		REQUIRE_THROWS(v.Verify(sel, row_count));
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

	SECTION("STRUCT(LIST(VARCHAR)): a child under a NULL parent does not shift later lists") {
		auto type = MakeType(Shape::STRUCT_LIST_VARCHAR);
		const idx_t row_count = 3;
		const idx_t null_row = 1;
		DataChunk chunk;
		chunk.Initialize(allocator, {type});
		chunk.SetChildCardinality(row_count);
		duckdb::vector<Value> expected(row_count);
		for (idx_t r = 0; r < row_count; r++) {
			expected[r] = ValidValue(Shape::STRUCT_LIST_VARCHAR, r);
			chunk.data[0].SetValue(r, expected[r]);
		}
		// Leave the LIST child valid while marking only its STRUCT parent NULL.
		FlatVector::ValidityMutable(chunk.data[0]).SetInvalid(null_row);

		ColumnDataCollection collection(allocator, {type});
		collection.Append(chunk);

		DataChunk result;
		ScanSingleChunk(collection, result);
		REQUIRE(result.data[0].GetValue(0) == expected[0]);
		REQUIRE(result.data[0].GetValue(null_row).IsNull());
		REQUIRE(result.data[0].GetValue(2) == expected[2]);
	}

	SECTION("ARRAY(LIST(VARCHAR)): a child under a NULL parent does not shift later lists") {
		auto type = MakeType(Shape::ARRAY_LIST_VARCHAR);
		const idx_t row_count = 3;
		const idx_t null_row = 1;
		DataChunk chunk;
		chunk.Initialize(allocator, {type});
		chunk.SetChildCardinality(row_count);
		duckdb::vector<Value> expected(row_count);
		for (idx_t r = 0; r < row_count; r++) {
			expected[r] = ValidValue(Shape::ARRAY_LIST_VARCHAR, r);
			chunk.data[0].SetValue(r, expected[r]);
		}
		// Leave the LIST grandchildren valid while marking only the ARRAY parent row NULL.
		FlatVector::ValidityMutable(chunk.data[0]).SetInvalid(null_row);

		ColumnDataCollection collection(allocator, {type});
		collection.Append(chunk);

		DataChunk result;
		ScanSingleChunk(collection, result);
		REQUIRE(result.data[0].GetValue(0) == expected[0]);
		REQUIRE(result.data[0].GetValue(null_row).IsNull());
		REQUIRE(result.data[0].GetValue(2) == expected[2]);
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

	SECTION("patching a STRUCT(LIST) child does not change an aliased column") {
		auto list_type = LogicalType::LIST(LogicalType::VARCHAR);
		auto struct_type = LogicalType::STRUCT({{"l", list_type}});
		const idx_t row_count = 2;
		DataChunk chunk;
		chunk.Initialize(allocator, {struct_type, list_type});
		chunk.SetChildCardinality(row_count);
		auto first = LeafList(0, 1);
		auto second = LeafList(1, 1);
		chunk.data[1].SetValue(0, first);
		chunk.data[1].SetValue(1, second);
		StructVector::GetEntries(chunk.data[0])[0].Reference(chunk.data[1]);
		FlatVector::ValidityMutable(chunk.data[0]).SetInvalid(1);

		ColumnDataCollection collection(allocator, {struct_type, list_type});
		collection.Append(chunk);

		DataChunk result;
		ScanSingleChunk(collection, result);
		REQUIRE(result.data[0].GetValue(1).IsNull());
		REQUIRE(result.data[1].GetValue(0) == first);
		REQUIRE(result.data[1].GetValue(1) == second);
	}
}
