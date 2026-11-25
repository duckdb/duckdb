#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/column/column_data_collection_segment.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector/array_vector.hpp"
#include "duckdb/common/vector/constant_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "test_helpers.hpp"

namespace duckdb {

// Whether the element at `idx` of `vector` is NULL in its own vector: for a struct, in every field too, and for an
// array, in every element.
static bool ElementIsNull(Vector &vector, idx_t idx) {
	if (FlatVector::Validity(vector).RowIsValid(idx)) {
		return false;
	}
	if (vector.GetType().InternalType() == PhysicalType::STRUCT) {
		for (auto &field : StructVector::GetEntries(vector)) {
			if (!ElementIsNull(field, idx)) {
				return false;
			}
		}
	}
	if (vector.GetType().InternalType() == PhysicalType::ARRAY) {
		auto array_size = ArrayType::GetSize(vector.GetType());
		for (idx_t j = 0; j < array_size; j++) {
			if (!ElementIsNull(ArrayVector::GetChildMutable(vector), idx * array_size + j)) {
				return false;
			}
		}
	}
	return true;
}

// The elements of every NULL row of the array column `col` are NULL in `collection`, and so are those of every NULL
// inner array; returns the NULL rows.
static idx_t CountNullArraysWithNullElements(ColumnDataCollection &collection, idx_t col) {
	auto array_size = ArrayType::GetSize(collection.Types()[col]);
	ColumnDataScanState scan_state;
	DataChunk scanned;
	collection.InitializeScanChunk(scanned);
	collection.InitializeScan(scan_state, ColumnDataScanProperties::DISALLOW_ZERO_COPY);
	idx_t null_rows = 0;
	while (collection.Scan(scan_state, scanned)) {
		auto &array = scanned.data[col];
		auto &elements = ArrayVector::GetChildMutable(array);
		if (elements.GetType().InternalType() == PhysicalType::ARRAY) {
			for (idx_t e = 0; e < scanned.size() * array_size; e++) {
				if (!FlatVector::Validity(elements).RowIsValid(e)) {
					REQUIRE(ElementIsNull(elements, e));
				}
			}
		}
		for (idx_t row = 0; row < scanned.size(); row++) {
			if (FlatVector::Validity(array).RowIsValid(row)) {
				continue;
			}
			null_rows++;
			for (idx_t j = 0; j < array_size; j++) {
				REQUIRE(ElementIsNull(ArrayVector::GetChildMutable(array), row * array_size + j));
			}
		}
	}
	return null_rows;
}

TEST_CASE("ColumnDataCollection copies NULL arrays over valid elements", "[column_data_array_nulls]") {
	if (STANDARD_VECTOR_SIZE < 1024) {
		// the appends below assume full-size vectors
		return;
	}
	DuckDB db(":memory:");
	Connection con(db);
	auto &buffer_manager = BufferManager::GetBufferManager(*db.instance);
	// every element valid; the NULL rows are set below, over those elements
	auto result = con.Query(StringUtil::Format(R"(
SELECT [i, i + 1]::BIGINT[2] AS a,
       [[i, i], [i, i + 1]]::INTEGER[2][2] AS aa,
       [repeat('z', i %% 30), i::VARCHAR]::VARCHAR[2] AS av,
       [{'p': i, 'q': [i]}, {'p': i + 1, 'q': []}]::STRUCT(p BIGINT, q BIGINT[])[2] AS asr,
       [range(i %% 3), range(i %% 4 + 1)]::BIGINT[][2] AS al
FROM range(%llu) t(i)
)",
	                                           static_cast<unsigned long long>(STANDARD_VECTOR_SIZE)));
	REQUIRE(!result->HasError());
	auto chunk = result->Fetch();
	REQUIRE(chunk);
	const idx_t size = chunk->size();
	REQUIRE(size == STANDARD_VECTOR_SIZE);
	const idx_t columns = chunk->ColumnCount();
	// inner arrays of aa under rows left valid below, NULL over valid elements
	auto &aa_inner = ArrayVector::GetChildMutable(chunk->data[1]);
	for (idx_t row = 3; row < size; row += 5) {
		FlatVector::ValidityMutable(aa_inner).SetInvalid(row * 2);
	}
	vector<vector<Value>> before(columns);
	for (idx_t col = 0; col < columns; col++) {
		auto &child = ArrayVector::GetChild(chunk->data[col]);
		for (idx_t i = 0; i < size * ArrayType::GetSize(chunk->data[col].GetType()); i++) {
			before[col].push_back(child.GetValue(i));
		}
		for (idx_t row = 1; row < size; row += 5) {
			FlatVector::ValidityMutable(chunk->data[col]).SetInvalid(row);
		}
	}

	SECTION("rows appended in two calls, the second crossing a chunk boundary") {
		ColumnDataCollection collection(buffer_manager, chunk->GetTypes());
		DataChunk part;
		part.InitializeEmpty(chunk->GetTypes());
		part.Slice(*chunk, SelectionVector(idx_t(0), idx_t(1000)), 1000);
		collection.Append(part);
		collection.Append(*chunk);
		REQUIRE(collection.Count() == 1000 + size);
		auto rows = collection.GetRows();
		for (idx_t row = 0; row < collection.Count(); row++) {
			const idx_t source_row = row < 1000 ? row : row - 1000;
			for (idx_t col = 0; col < columns; col++) {
				auto got = rows.GetValue(col, row);
				auto expected = chunk->GetValue(col, source_row);
				if (!Value::NotDistinctFrom(got, expected)) {
					FAIL("row " << row << " column " << col << ": " << got.ToString() << " != " << expected.ToString());
				}
			}
		}
		for (idx_t col = 0; col < columns; col++) {
			REQUIRE(CountNullArraysWithNullElements(collection, col) > 0);
		}
		// the input's elements are as they were
		for (idx_t col = 0; col < columns; col++) {
			auto &child = ArrayVector::GetChild(chunk->data[col]);
			for (idx_t i = 0; i < before[col].size(); i++) {
				REQUIRE(Value::NotDistinctFrom(child.GetValue(i), before[col][i]));
			}
		}
	}
	SECTION("a constant NULL array over valid elements, flattened by the append") {
		DataChunk constant;
		constant.InitializeEmpty(chunk->GetTypes());
		for (idx_t col = 0; col < columns; col++) {
			// row 0's valid elements, under a NULL row
			constant.data[col].Reference(chunk->data[col].GetValue(0), count_t(size));
			ConstantVector::Validity(constant.data[col]).SetInvalid(0);
		}
		constant.SetCardinalityUnsafe(size);
		for (idx_t col = 0; col < columns; col++) {
			REQUIRE(constant.GetValue(col, 0).IsNull());
		}
		ColumnDataCollection collection(buffer_manager, chunk->GetTypes());
		collection.Append(constant);
		for (idx_t col = 0; col < columns; col++) {
			REQUIRE(CountNullArraysWithNullElements(collection, col) == size);
		}
	}
}

// Appends a column of `type` (an array of arrays of two) whose rows are all valid and some of whose inner arrays are
// NULL over valid elements, in two appends, the second crossing a chunk boundary at a nonzero offset into its input.
static void CheckNullInnerArrays(BufferManager &buffer_manager, Connection &con, const string &elements,
                                 const string &type) {
	auto result = con.Query(StringUtil::Format("SELECT %s::%s AS aa FROM range(%llu) t(i)", elements, type,
	                                           static_cast<unsigned long long>(STANDARD_VECTOR_SIZE)));
	REQUIRE(!result->HasError());
	auto chunk = result->Fetch();
	REQUIRE(chunk);
	const idx_t size = chunk->size();
	auto &inner = ArrayVector::GetChildMutable(chunk->data[0]);
	for (idx_t row = 3; row < size; row += 5) {
		FlatVector::ValidityMutable(inner).SetInvalid(row * 2 + row % 2);
	}
	ColumnDataCollection collection(buffer_manager, chunk->GetTypes());
	DataChunk part;
	part.InitializeEmpty(chunk->GetTypes());
	part.Slice(*chunk, SelectionVector(idx_t(0), idx_t(1000)), 1000);
	collection.Append(part);
	collection.Append(*chunk);
	REQUIRE(collection.Count() == 1000 + size);
	auto rows = collection.GetRows();
	for (idx_t row = 0; row < collection.Count(); row++) {
		auto got = rows.GetValue(0, row);
		auto expected = chunk->GetValue(0, row < 1000 ? row : row - 1000);
		if (!Value::NotDistinctFrom(got, expected)) {
			FAIL("row " << row << ": " << got.ToString() << " != " << expected.ToString());
		}
	}
	// the elements of every NULL inner array are NULL
	ColumnDataScanState scan_state;
	DataChunk scanned;
	collection.InitializeScanChunk(scanned);
	collection.InitializeScan(scan_state, ColumnDataScanProperties::DISALLOW_ZERO_COPY);
	idx_t null_inner = 0;
	while (collection.Scan(scan_state, scanned)) {
		auto &scanned_inner = ArrayVector::GetChildMutable(scanned.data[0]);
		for (idx_t e = 0; e < scanned.size() * 2; e++) {
			if (FlatVector::Validity(scanned_inner).RowIsValid(e)) {
				continue;
			}
			null_inner++;
			for (idx_t k = 0; k < 2; k++) {
				REQUIRE(ElementIsNull(ArrayVector::GetChildMutable(scanned_inner), e * 2 + k));
			}
		}
	}
	REQUIRE(null_inner > 0);
}

TEST_CASE("ColumnDataCollection copies NULL inner arrays under valid rows", "[column_data_array_nulls]") {
	if (STANDARD_VECTOR_SIZE < 1024) {
		return;
	}
	DuckDB db(":memory:");
	Connection con(db);
	auto &buffer_manager = BufferManager::GetBufferManager(*db.instance);
	CheckNullInnerArrays(buffer_manager, con, "[[i, i], [i, i + 1]]", "INTEGER[2][2]");
	// nested elements, which hold validity of their own
	CheckNullInnerArrays(buffer_manager, con, "[[{'p': i}, {'p': i}], [{'p': i}, {'p': i + 1}]]",
	                     "STRUCT(p INTEGER)[2][2]");
}

// One vector of rows, the given columns of every shape the copy functions handle: NULLs, long strings, empty lists.
static unique_ptr<DataChunk> SelectionInputChunk(Connection &con, const string &columns) {
	auto result = con.Query(StringUtil::Format(R"(
WITH t AS (
SELECT i::INTEGER AS i,
       CASE WHEN i %% 5 = 0 THEN NULL ELSE i * 3 END::BIGINT AS n,
       CASE WHEN i %% 7 = 0 THEN NULL ELSE repeat(chr(65 + (i %% 26)::INT), i %% 40) END AS s,
       CASE WHEN i %% 11 = 0 THEN NULL ELSE {'a': i, 'b': 'v' || i::VARCHAR || repeat('x', i %% 20)} END AS st,
       CASE WHEN i %% 13 = 0 THEN NULL ELSE range(i %% 5) END AS l,
       CASE WHEN i %% 17 = 0 THEN NULL ELSE [i, i + 1]::BIGINT[2] END AS a,
       [i, i + 2]::INTEGER[2] AS a2,
       {'x': CASE WHEN i %% 9 = 0 THEN NULL ELSE [i, i + 1]::BIGINT[2] END} AS sa,
       CASE WHEN i %% 4 = 0 THEN []::BIGINT[2][] WHEN i %% 4 = 1 THEN NULL
            ELSE [[i, CASE WHEN i %% 6 = 0 THEN NULL ELSE i END]::BIGINT[2]] END AS la,
       [[i, i], CASE WHEN i %% 10 = 0 THEN NULL ELSE [i, CASE WHEN i %% 8 = 0 THEN NULL ELSE i END] END]::INTEGER[2][2] AS aa,
       [{'k': i %% 3, 'v': [i::VARCHAR || repeat('y', i %% 15)]}] AS ls,
       CASE WHEN i %% 17 = 0 THEN NULL ELSE [i, CASE WHEN i %% 3 = 0 THEN NULL ELSE i END]::BIGINT[2] END AS an,
       CASE WHEN i %% 19 = 0 THEN NULL ELSE [repeat('z', i %% 30), i::VARCHAR]::VARCHAR[2] END AS av,
       CASE WHEN i %% 23 = 0 THEN NULL ELSE [{'p': i}, {'p': i + 1}]::STRUCT(p BIGINT)[2] END AS asr,
       CASE WHEN i %% 29 = 0 THEN NULL ELSE [range(i %% 3), range(i %% 4)]::BIGINT[][2] END AS al,
       CASE WHEN i %% 31 = 0 THEN NULL ELSE MAP([i, i + 1], ['m' || i::VARCHAR, NULL]) END AS mp,
       list_transform(range(i %% 6), lambda x: {'q': x, 'r': [x]}) AS lst
FROM range(%llu) t(i))
SELECT %s FROM t
)",
	                                           static_cast<unsigned long long>(STANDARD_VECTOR_SIZE), columns));
	REQUIRE(!result->HasError());
	auto chunk = result->Fetch();
	REQUIRE(chunk);
	REQUIRE(chunk->size() == STANDARD_VECTOR_SIZE);
	return chunk;
}

static const char *const ALL_COLUMNS = "*";
static const char *const FLAT_COLUMNS = "i, n, s";
static const char *const ARRAY_COLUMNS = "a, aa, an, av, asr, al";

// A permutation of [0, count): every `step`th row, then the rest backwards.
static SelectionVector SelectionInputOrder(idx_t count, idx_t step = 7) {
	SelectionVector order(count);
	idx_t pos = 0;
	for (idx_t i = 0; i < count; i += step) {
		order.set_index(pos++, i);
	}
	for (idx_t i = count; i > 0; i--) {
		if ((i - 1) % step != 0) {
			order.set_index(pos++, i - 1);
		}
	}
	REQUIRE(pos == count);
	return order;
}

// Collections that each check, row by row, that they hold the rows they were given.
struct CheckedCollections {
	CheckedCollections(BufferManager &buffer_manager, const vector<LogicalType> &types, idx_t count)
	    : states(count), expected(count) {
		for (idx_t c = 0; c < count; c++) {
			collections.push_back(make_uniq<ColumnDataCollection>(buffer_manager, types));
			collections[c]->InitializeAppend(states[c]);
		}
	}
	// Appends the rows at positions [offset, offset + count) of `order` to collection `c`.
	void Append(idx_t c, DataChunk &chunk, const SelectionVector &order, idx_t offset, idx_t count) {
		SelectionVector part(count);
		for (idx_t i = 0; i < count; i++) {
			part.set_index(i, order.get_index(offset + i));
		}
		// (the expected rows first: the append flattens the input)
		for (idx_t i = 0; i < count; i++) {
			expected[c].push_back(Row(chunk, order.get_index(offset + i)));
		}
		collections[c]->Append(states[c], chunk, part, count);
	}
	// Appends the whole chunk to collection `c` through Append(state, chunk).
	void AppendChunk(idx_t c, DataChunk &chunk) {
		for (idx_t i = 0; i < chunk.size(); i++) {
			expected[c].push_back(Row(chunk, i));
		}
		collections[c]->Append(states[c], chunk);
	}
	// Scans each collection once (GetValue by row would seek from the start every time).
	void Check() {
		for (idx_t c = 0; c < collections.size(); c++) {
			REQUIRE(collections[c]->Count() == expected[c].size());
			ColumnDataScanState scan_state;
			DataChunk scanned;
			collections[c]->InitializeScanChunk(scanned);
			collections[c]->InitializeScan(scan_state);
			idx_t row = 0;
			while (collections[c]->Scan(scan_state, scanned)) {
				for (idx_t i = 0; i < scanned.size(); i++, row++) {
					for (idx_t col = 0; col < scanned.ColumnCount(); col++) {
						auto got = scanned.GetValue(col, i);
						if (!Value::NotDistinctFrom(got, expected[c][row][col])) {
							FAIL("collection " << c << " row " << row << " column " << col << ": " << got.ToString()
							                   << " != " << expected[c][row][col].ToString());
						}
					}
				}
			}
			REQUIRE(row == expected[c].size());
		}
	}
	vector<Value> Row(DataChunk &chunk, idx_t row) {
		vector<Value> values;
		for (idx_t col = 0; col < collections[0]->ColumnCount(); col++) {
			values.push_back(chunk.GetValue(col, row));
		}
		return values;
	}

	vector<unique_ptr<ColumnDataCollection>> collections;
	vector<ColumnDataAppendState> states;
	vector<vector<vector<Value>>> expected;
};

// Splits positions [0, count) of `order` into stretches of the given lengths, appended to the collections in turn.
static void Split(CheckedCollections &out, DataChunk &chunk, const SelectionVector &order, idx_t count,
                  const vector<idx_t> &stretches) {
	idx_t offset = 0;
	for (idx_t s = 0; offset < count; s++) {
		auto take = MinValue<idx_t>(stretches[s % stretches.size()], count - offset);
		REQUIRE(take > 0);
		out.Append(s % out.collections.size(), chunk, order, offset, take);
		offset += take;
	}
}

// As Split, through the batch Append: each call passes the next stretch of every collection, and an empty target.
static void SplitBatch(CheckedCollections &out, DataChunk &chunk, const SelectionVector &order, idx_t count,
                       const vector<idx_t> &stretches) {
	vector<ColumnDataSelectionTarget> targets;
	idx_t offset = 0;
	for (idx_t s = 0; offset < count; s++) {
		auto take = MinValue<idx_t>(stretches[s % stretches.size()], count - offset);
		REQUIRE(take > 0);
		auto c = s % out.collections.size();
		// (the expected rows first: the append flattens the input)
		for (idx_t i = 0; i < take; i++) {
			out.expected[c].push_back(out.Row(chunk, order.get_index(offset + i)));
		}
		targets.push_back(ColumnDataSelectionTarget {*out.collections[c], out.states[c], offset, offset + take});
		offset += take;
		if (c + 1 == out.collections.size() || offset == count) {
			targets.push_back(ColumnDataSelectionTarget {*out.collections[0], out.states[0], offset, offset});
			ColumnDataCollection::AppendBatch(chunk, order, count, targets);
			targets.clear();
		}
	}
}

static void CheckSplit(BufferManager &buffer_manager, DataChunk &chunk, const SelectionVector &order, idx_t count,
                       idx_t column_count, const vector<idx_t> &stretches, idx_t collection_count) {
	auto types = chunk.GetTypes();
	types.resize(column_count);
	CheckedCollections out(buffer_manager, types, collection_count);
	Split(out, chunk, order, count, stretches);
	out.Check();
}

// Appends the chunk's rows in stretches of 32 through selections, and through slices and Append(state, chunk): the
// same rows, in collections of the same size (a list all the rows share is stored once per append in both).
static void CheckStoredOnce(BufferManager &buffer_manager, DataChunk &chunk) {
	const idx_t size = chunk.size();
	const idx_t stretch = 32, count = 16;
	auto order = SelectionInputOrder(size);
	CheckedCollections selected(buffer_manager, chunk.GetTypes(), count);
	Split(selected, chunk, order, size, {stretch});
	selected.Check();
	CheckedCollections sliced(buffer_manager, chunk.GetTypes(), count);
	for (idx_t offset = 0, s = 0; offset < size; offset += stretch, s++) {
		auto take = MinValue<idx_t>(stretch, size - offset);
		SelectionVector part(take);
		for (idx_t i = 0; i < take; i++) {
			part.set_index(i, order.get_index(offset + i));
		}
		DataChunk slice;
		slice.InitializeEmpty(chunk.GetTypes());
		slice.Slice(chunk, part, take);
		sliced.AppendChunk(s % count, slice);
	}
	sliced.Check();
	for (idx_t c = 0; c < count; c++) {
		REQUIRE(selected.collections[c]->SizeInBytes() == sliced.collections[c]->SizeInBytes());
	}
}

// Splits a chunk of the given columns: short stretches over several collections, and long ones over two (a list's
// elements then pass a vector in one append).
static void CheckColumns(const char *columns) {
	DuckDB db(":memory:");
	Connection con(db);
	auto &buffer_manager = BufferManager::GetBufferManager(*db.instance);
	auto chunk = SelectionInputChunk(con, columns);
	const idx_t size = chunk->size();
	auto order = SelectionInputOrder(size);
	CheckSplit(buffer_manager, *chunk, order, size, chunk->ColumnCount(), {1, 3, 32, 5}, 16);
	CheckSplit(buffer_manager, *chunk, order, size, chunk->ColumnCount(), {size / 2}, 2);
}

TEST_CASE("ColumnDataCollection appends the selected rows of flat columns", "[column_data_selection_append]") {
	if (STANDARD_VECTOR_SIZE < 1024) {
		// the stretches below assume full-size vectors
		return;
	}
	DuckDB db(":memory:");
	Connection con(db);
	auto &buffer_manager = BufferManager::GetBufferManager(*db.instance);
	auto chunk = SelectionInputChunk(con, FLAT_COLUMNS);
	const idx_t size = chunk->size();
	auto order = SelectionInputOrder(size);
	// short stretches over many collections
	CheckSplit(buffer_manager, *chunk, order, size, chunk->ColumnCount(), {1, 3, 32, 5}, 64);
	// a prefix of the columns
	CheckSplit(buffer_manager, *chunk, order, size, 2, {7}, 5);
	// part of the order
	CheckSplit(buffer_manager, *chunk, order, size / 2, chunk->ColumnCount(), {9}, 4);
}

TEST_CASE("ColumnDataCollection appends the selected rows of STRUCT columns", "[column_data_selection_append]") {
	if (STANDARD_VECTOR_SIZE >= 1024) {
		CheckColumns("i, st");
	}
}

TEST_CASE("ColumnDataCollection appends the selected rows of LIST columns", "[column_data_selection_append]") {
	if (STANDARD_VECTOR_SIZE >= 1024) {
		CheckColumns("i, l, ls, lst");
	}
}

TEST_CASE("ColumnDataCollection appends the selected rows of ARRAY columns", "[column_data_selection_append]") {
	if (STANDARD_VECTOR_SIZE >= 1024) {
		CheckColumns("i, a, a2, sa, la, aa, an, av, asr, al");
	}
}

TEST_CASE("ColumnDataCollection appends the selected rows of MAP columns", "[column_data_selection_append]") {
	if (STANDARD_VECTOR_SIZE >= 1024) {
		CheckColumns("i, mp");
	}
}

TEST_CASE("ColumnDataCollection appends the selected rows of dictionary and constant inputs",
          "[column_data_selection_append]") {
	if (STANDARD_VECTOR_SIZE < 1024) {
		return;
	}
	DuckDB db(":memory:");
	Connection con(db);
	auto &buffer_manager = BufferManager::GetBufferManager(*db.instance);
	SECTION("a dictionary input") {
		// the input's own selection is composed with the appended rows' selection
		auto chunk = SelectionInputChunk(con, ALL_COLUMNS);
		const idx_t size = chunk->size();
		SelectionVector sel(size / 2);
		for (idx_t i = 0; i < size / 2; i++) {
			sel.set_index(i, (i * 5) % size);
		}
		chunk->Slice(sel, size / 2);
		CheckSplit(buffer_manager, *chunk, SelectionInputOrder(size / 2), size / 2, chunk->ColumnCount(), {4, 31}, 8);
	}
	SECTION("constant inputs") {
		auto chunk = SelectionInputChunk(con, "i, n, s, st, l, a");
		const idx_t size = chunk->size();
		chunk->data[0].Reference(Value::INTEGER(42), count_t(size));
		chunk->data[1].Reference(Value(LogicalType::BIGINT), count_t(size));
		chunk->data[2].Reference(Value("a constant string too long to be inlined"), count_t(size));
		chunk->data[3].Reference(Value(chunk->data[3].GetType()), count_t(size));
		chunk->data[4].Reference(Value::LIST(LogicalType::BIGINT, {Value::BIGINT(1), Value::BIGINT(2)}), count_t(size));
		// a NULL array over row 1's valid elements
		chunk->data[5].Reference(chunk->GetValue(5, 1), count_t(size));
		ConstantVector::Validity(chunk->data[5]).SetInvalid(0);
		CheckedCollections out(buffer_manager, chunk->GetTypes(), 3);
		Split(out, *chunk, SelectionInputOrder(size), size, {6});
		out.Check();
		for (auto &collection : out.collections) {
			REQUIRE(CountNullArraysWithNullElements(*collection, 5) > 0);
		}
	}
}

TEST_CASE("ColumnDataCollection stores a list shared by the selected rows once per append",
          "[column_data_selection_append]") {
	if (STANDARD_VECTOR_SIZE < 1024) {
		return;
	}
	DuckDB db(":memory:");
	Connection con(db);
	auto &buffer_manager = BufferManager::GetBufferManager(*db.instance);
	// a constant list, the same with NULL rows, a constant struct holding a list, a constant array of lists
	const vector<pair<string, bool>> columns {{"range(100)", false},
	                                          {"range(100)", true},
	                                          {"{'l': range(100), 'k': 1}", false},
	                                          {"[range(100)]::BIGINT[][1]", false}};
	for (auto &column : columns) {
		auto chunk = SelectionInputChunk(con, "i, " + column.first + " AS v");
		const idx_t size = chunk->size();
		chunk->data[1].Reference(chunk->GetValue(1, 0), count_t(size));
		if (column.second) {
			chunk->data[1].Flatten();
			for (idx_t row = 0; row < size; row += 3) {
				FlatVector::SetNull(chunk->data[1], row, true);
			}
		}
		CheckStoredOnce(buffer_manager, *chunk);
	}
}

TEST_CASE("ColumnDataCollection appends selected rows across a collection's chunk boundary",
          "[column_data_selection_append]") {
	if (STANDARD_VECTOR_SIZE < 1024) {
		return;
	}
	DuckDB db(":memory:");
	Connection con(db);
	auto &buffer_manager = BufferManager::GetBufferManager(*db.instance);
	auto chunk = SelectionInputChunk(con, ALL_COLUMNS);
	const idx_t size = chunk->size();
	auto order = SelectionInputOrder(size);
	SECTION("at a chunk's end") {
		// each collection's first chunk fills exactly; the next stretch starts on the full chunk and goes past it
		CheckedCollections out(buffer_manager, chunk->GetTypes(), 2);
		Split(out, *chunk, order, size, {size / 2});
		Split(out, *chunk, SelectionInputOrder(size, 3), size, {size / 2});
		Split(out, *chunk, order, size, {size * 3 / 4, size / 4});
		out.Check();
		REQUIRE(out.collections[0]->ChunkCount() == 2);
		REQUIRE(out.collections[1]->ChunkCount() == 2);
	}
	SECTION("partway into a chunk") {
		// an append that starts 8 rows before the end of a collection's chunk: the rest is copied at offset 8
		CheckedCollections out(buffer_manager, chunk->GetTypes(), 1);
		Split(out, *chunk, order, size - 8, {size - 8});
		Split(out, *chunk, SelectionInputOrder(size, 3), size, {100});
		out.Check();
		REQUIRE(out.collections[0]->ChunkCount() == 2);
	}
}

TEST_CASE("ColumnDataCollection appends selected NULL arrays over valid elements", "[column_data_selection_append]") {
	if (STANDARD_VECTOR_SIZE < 1024) {
		return;
	}
	DuckDB db(":memory:");
	Connection con(db);
	auto &buffer_manager = BufferManager::GetBufferManager(*db.instance);
	auto chunk = SelectionInputChunk(con, ARRAY_COLUMNS);
	const idx_t size = chunk->size();
	auto order = SelectionInputOrder(size);
	// the input's elements under the NULL rows stay valid: the copy must make them NULL, and not in the input
	// a: BIGINT[2]; aa: INTEGER[2][2], whose NULL rows must also make their inner arrays' elements NULL; an:
	// BIGINT[2]; av: VARCHAR[2]; asr: STRUCT(p BIGINT)[2]; al: BIGINT[][2]
	const idx_t aa = 1, an = 2;
	auto &child = ArrayVector::GetChildMutable(chunk->data[an]);
	auto &aa_inner = ArrayVector::GetChildMutable(chunk->data[aa]);
	auto &aa_leaf = ArrayVector::GetChildMutable(aa_inner);
	// and inner arrays of aa under rows left valid, NULL over valid elements
	for (idx_t row = 3; row < size; row += 13) {
		FlatVector::ValidityMutable(aa_inner).SetInvalid(row * 2);
	}
	ValidityMask before(FlatVector::Validity(child), size * 2);
	ValidityMask aa_inner_before(FlatVector::Validity(aa_inner), size * 2);
	ValidityMask aa_leaf_before(FlatVector::Validity(aa_leaf), size * 4);
	for (idx_t row = 1; row < size; row += 13) {
		for (idx_t col = 0; col < chunk->ColumnCount(); col++) {
			FlatVector::ValidityMutable(chunk->data[col]).SetInvalid(row);
		}
	}
	// through Append(state, chunk), one append crossing a chunk boundary
	CheckedCollections appended(buffer_manager, chunk->GetTypes(), 1);
	DataChunk part;
	part.InitializeEmpty(chunk->GetTypes());
	part.Slice(*chunk, SelectionVector(idx_t(0), idx_t(1000)), 1000);
	appended.AppendChunk(0, part);
	appended.AppendChunk(0, *chunk);
	// through selections, short stretches and one crossing a chunk boundary at offset 8
	CheckedCollections out(buffer_manager, chunk->GetTypes(), 4);
	Split(out, *chunk, order, size, {1, 3, 32, 5});
	CheckedCollections crossing(buffer_manager, chunk->GetTypes(), 1);
	Split(crossing, *chunk, order, size - 8, {size - 8});
	Split(crossing, *chunk, SelectionInputOrder(size, 3), size, {100});
	// through the batch append, at nonzero offsets of the selection
	CheckedCollections batched(buffer_manager, chunk->GetTypes(), 4);
	SplitBatch(batched, *chunk, order, size, {1, 3, 32, 5});
	for (auto results : {&appended, &out, &crossing, &batched}) {
		results->Check();
		for (auto &collection : results->collections) {
			for (idx_t col = 0; col < chunk->ColumnCount(); col++) {
				REQUIRE(CountNullArraysWithNullElements(*collection, col) > 0);
			}
		}
	}
	for (idx_t i = 0; i < size * 2; i++) {
		REQUIRE(FlatVector::Validity(child).RowIsValid(i) == before.RowIsValid(i));
		REQUIRE(FlatVector::Validity(aa_inner).RowIsValid(i) == aa_inner_before.RowIsValid(i));
	}
	for (idx_t i = 0; i < size * 4; i++) {
		REQUIRE(FlatVector::Validity(aa_leaf).RowIsValid(i) == aa_leaf_before.RowIsValid(i));
	}
}

TEST_CASE("ColumnDataCollection appends selected rows of several chunks in turn", "[column_data_selection_append]") {
	if (STANDARD_VECTOR_SIZE < 1024) {
		return;
	}
	DuckDB db(":memory:");
	Connection con(db);
	auto &buffer_manager = BufferManager::GetBufferManager(*db.instance);
	auto chunk = SelectionInputChunk(con, ALL_COLUMNS);
	const idx_t size = chunk->size();
	auto order = SelectionInputOrder(size);
	CheckedCollections out(buffer_manager, chunk->GetTypes(), 3);
	auto first = SelectionInputChunk(con, ALL_COLUMNS);
	first->Slice(order, size);
	auto second = SelectionInputChunk(con, ALL_COLUMNS);
	second->Slice(SelectionInputOrder(size, 5), size);
	// dictionary, dictionary with another order, then flat
	Split(out, *first, order, size, {8});
	Split(out, *second, SelectionInputOrder(size, 3), size, {8});
	Split(out, *chunk, SelectionInputOrder(size, 11), size, {8});
	out.Check();

	// then fewer columns
	auto types = chunk->GetTypes();
	types.resize(2);
	CheckedCollections narrow(buffer_manager, types, 2);
	Split(narrow, *first, order, size, {5});
	narrow.Check();
}

TEST_CASE("ColumnDataCollection appends selected rows after constant and dictionary entries",
          "[column_data_selection_append]") {
	if (STANDARD_VECTOR_SIZE < 1024) {
		// the shapes below assume full-size vectors
		return;
	}
	// Append(state, chunk) keeps a constant or dictionary vector as such; an append of selected rows
	// writes the rows after it
	DuckDB db(":memory:");
	auto &buffer_manager = BufferManager::GetBufferManager(*db.instance);
	const vector<LogicalType> types {LogicalType::VARCHAR};
	const idx_t size = STANDARD_VECTOR_SIZE;
	const idx_t dict_size = 100;
	auto dictionary = DictionaryVector::CreateReusableDictionary(LogicalType::VARCHAR, dict_size);
	{
		auto writer = FlatVector::Writer<string_t>(dictionary->data, dict_size);
		for (idx_t i = 0; i < dict_size; i++) {
			writer.WriteValue(string_t("dictionary_payload_value_" + to_string(i)));
		}
	}
	SelectionVector dict_sel(size);
	for (idx_t i = 0; i < size; i++) {
		dict_sel.set_index(i, i % dict_size);
	}
	DataChunk flat;
	flat.Initialize(Allocator::DefaultAllocator(), types);
	for (idx_t i = 0; i < size; i++) {
		flat.data[0].SetValue(i, Value("flat_value_long_enough_" + to_string(i)));
	}
	flat.SetCardinalityUnsafe(size);
	auto order = SelectionInputOrder(size);

	for (auto representation : {VectorType::CONSTANT_VECTOR, VectorType::DICTIONARY_VECTOR}) {
		CheckedCollections out(buffer_manager, types, 1);
		DataChunk compact;
		compact.InitializeEmpty(types);
		if (representation == VectorType::CONSTANT_VECTOR) {
			compact.data[0].Reference(Value("a constant string too long to be inlined"), count_t(size / 4));
			compact.SetCardinalityUnsafe(size / 4);
		} else {
			compact.data[0].Dictionary(dictionary, dict_sel, size / 2);
			compact.SetCardinalityUnsafe(size / 2);
		}
		out.AppendChunk(0, compact);
		{
			// the collection holds the entry compact
			ColumnDataScanState scan_state;
			DataChunk scanned;
			out.collections[0]->InitializeScanChunk(scanned);
			out.collections[0]->InitializeScan(scan_state, ColumnDataScanProperties::ALLOW_ZERO_COPY);
			REQUIRE(out.collections[0]->Scan(scan_state, scanned));
			REQUIRE(scanned.data[0].GetVectorType() == representation);
		}
		Split(out, flat, order, size, {size / 3});
		out.Check();
	}
}

TEST_CASE("ColumnDataCollection appends selected rows to several collections in one call",
          "[column_data_selection_append]") {
	if (STANDARD_VECTOR_SIZE < 1024) {
		return;
	}
	DuckDB db(":memory:");
	Connection con(db);
	auto &buffer_manager = BufferManager::GetBufferManager(*db.instance);
	auto chunk = SelectionInputChunk(con, ALL_COLUMNS);
	const idx_t size = chunk->size();
	auto order = SelectionInputOrder(size);
	SECTION("every column shape") {
		CheckedCollections out(buffer_manager, chunk->GetTypes(), 16);
		SplitBatch(out, *chunk, order, size, {1, 3, 32, 5});
		out.Check();
		CheckedCollections halves(buffer_manager, chunk->GetTypes(), 2);
		SplitBatch(halves, *chunk, order, size, {size / 2});
		halves.Check();
	}
	SECTION("a prefix of the columns, part of the order") {
		auto types = chunk->GetTypes();
		types.resize(2);
		CheckedCollections out(buffer_manager, types, 5);
		SplitBatch(out, *chunk, order, size / 2, {7});
		out.Check();
	}
	SECTION("a dictionary input, then constant inputs") {
		CheckedCollections out(buffer_manager, chunk->GetTypes(), 8);
		auto dict = SelectionInputChunk(con, ALL_COLUMNS);
		SelectionVector sel(size / 2);
		for (idx_t i = 0; i < size / 2; i++) {
			sel.set_index(i, (i * 5) % size);
		}
		dict->Slice(sel, size / 2);
		SplitBatch(out, *dict, SelectionInputOrder(size / 2), size / 2, {4, 31});
		// row 0 is NULL in most columns; the arrays a, aa, an, av, asr and al are NULL over row 1's valid elements
		const vector<idx_t> arrays {5, 9, 11, 12, 13, 14};
		for (idx_t col = 0; col < chunk->ColumnCount(); col++) {
			const bool array = std::find(arrays.begin(), arrays.end(), col) != arrays.end();
			chunk->data[col].Reference(chunk->GetValue(col, array ? 1 : 0), count_t(size));
			if (array) {
				ConstantVector::Validity(chunk->data[col]).SetInvalid(0);
			}
		}
		SplitBatch(out, *chunk, order, size, {6});
		out.Check();
		for (idx_t col : arrays) {
			for (auto &collection : out.collections) {
				REQUIRE(CountNullArraysWithNullElements(*collection, col) > 0);
			}
		}
	}
	SECTION("across a collection's chunk boundary, mixed with single appends") {
		CheckedCollections out(buffer_manager, chunk->GetTypes(), 2);
		// collection 0 holds 8 rows fewer than a chunk, so its first stretch in the batch crosses the chunk's end
		Split(out, *chunk, order, size - 8, {size - 8});
		SplitBatch(out, *chunk, SelectionInputOrder(size, 3), size, {100});
		Split(out, *chunk, order, size / 2, {9});
		out.Check();
		REQUIRE(out.collections[0]->ChunkCount() == 2);
	}
}

// A list vector stores no more elements than its valid rows reference: NULL rows' elements are not copied.
static void CheckListStorage(const Vector &list, idx_t count) {
	auto entries = FlatVector::GetData<list_entry_t>(list);
	idx_t referenced = 0;
	for (idx_t row = 0; row < count; row++) {
		if (FlatVector::Validity(list).RowIsValid(row)) {
			referenced += entries[row].length;
		}
	}
	REQUIRE(ListVector::GetListSize(list) <= referenced);
}

TEST_CASE("ColumnDataCollection reuses the selections over the children of nested columns",
          "[column_data_selection_append]") {
	if (STANDARD_VECTOR_SIZE < 1024) {
		return;
	}
	DuckDB db(":memory:");
	Connection con(db);
	auto &buffer_manager = BufferManager::GetBufferManager(*db.instance);
	// sibling lists and arrays at the same depth, the second longer than the first; lists of lists; a map of lists; a
	// wide array; NULL structs and lists at several depths
	auto result = con.Query(StringUtil::Format(R"(
SELECT CASE WHEN i %% 9 = 0 THEN NULL ELSE {'x': CASE WHEN i %% 4 = 0 THEN NULL ELSE range(i %% 3) END,
       'y': CASE WHEN i %% 6 = 1 THEN NULL ELSE range(i %% 40) END, 'z': [i, i + 1]::BIGINT[2]} END AS st,
       [{'p': range(i %% 2), 'q': [CASE WHEN i %% 5 = 2 THEN NULL ELSE range(i %% 5) END, range(i %% 7)],
         'r': [i, i]::BIGINT[2]}] AS ls,
       MAP([i, i + 1], [range(i %% 4), range(i %% 30)]) AS mp,
       [range(i %% 3), range(i %% 50)]::BIGINT[][2] AS al,
       list_transform(range(20), lambda x: i + x)::BIGINT[20] AS wide
FROM range(%llu) t(i)
)",
	                                           static_cast<unsigned long long>(STANDARD_VECTOR_SIZE)));
	REQUIRE(!result->HasError());
	auto chunk = result->Fetch();
	REQUIRE(chunk->size() == STANDARD_VECTOR_SIZE);
	const idx_t size = chunk->size();
	// NULL lists over elements, which the copies must skip: st.x and ls
	for (auto list : {&StructVector::GetEntries(chunk->data[0])[0], &chunk->data[1]}) {
		auto entries = FlatVector::GetData<list_entry_t>(*list);
		for (idx_t row = 2; row < size; row += 7) {
			if (entries[row].length > 0) {
				FlatVector::SetNull(*list, row, true);
			}
		}
	}
	auto order = SelectionInputOrder(size);
	// short stretches, long ones (the selections then grow), then short again
	CheckedCollections single(buffer_manager, chunk->GetTypes(), 3);
	Split(single, *chunk, order, size, {1, 40, 3, 200, 2});
	Split(single, *chunk, order, size, {size / 2});
	Split(single, *chunk, order, size, {5, 1});
	single.Check();
	CheckedCollections batched(buffer_manager, chunk->GetTypes(), 3);
	SplitBatch(batched, *chunk, order, size, {1, 40, 3, 200, 2});
	SplitBatch(batched, *chunk, order, size, {size / 2});
	SplitBatch(batched, *chunk, order, size, {5, 1});
	batched.Check();
	// st.x and ls store only their valid rows' elements
	for (auto results : {&single, &batched}) {
		for (auto &collection : results->collections) {
			ColumnDataScanState scan_state;
			DataChunk scanned;
			collection->InitializeScanChunk(scanned);
			collection->InitializeScan(scan_state, ColumnDataScanProperties::DISALLOW_ZERO_COPY);
			while (collection->Scan(scan_state, scanned)) {
				CheckListStorage(StructVector::GetEntries(scanned.data[0])[0], scanned.size());
				CheckListStorage(scanned.data[1], scanned.size());
			}
		}
	}
}

TEST_CASE("ColumnDataCollection appends selected NULL arrays over valid elements inside lists and structs",
          "[column_data_selection_append]") {
	if (STANDARD_VECTOR_SIZE < 1024) {
		return;
	}
	DuckDB db(":memory:");
	Connection con(db);
	auto &buffer_manager = BufferManager::GetBufferManager(*db.instance);
	auto result = con.Query(StringUtil::Format(R"(
SELECT [[i, i + 1]::BIGINT[2], [i, i]::BIGINT[2]] AS la,
       {'x': [i, i + 1]::BIGINT[2], 'y': i} AS sa,
       [[{'p': i}, {'p': i}], [{'p': i}, {'p': i + 1}]]::STRUCT(p INTEGER)[2][2] AS aas
FROM range(%llu) t(i)
)",
	                                           static_cast<unsigned long long>(STANDARD_VECTOR_SIZE)));
	REQUIRE(!result->HasError());
	auto chunk = result->Fetch();
	REQUIRE(chunk);
	const idx_t size = chunk->size();
	// NULL arrays over valid elements: in la's list elements, in sa's field x, and aas's inner arrays
	auto &la_arrays = ListVector::GetChildMutable(chunk->data[0]);
	auto &sa_x = StructVector::GetEntries(chunk->data[1])[0];
	auto &aas_inner = ArrayVector::GetChildMutable(chunk->data[2]);
	for (idx_t row = 1; row < size; row += 7) {
		FlatVector::ValidityMutable(la_arrays).SetInvalid(row * 2);
		FlatVector::ValidityMutable(sa_x).SetInvalid(row);
		FlatVector::ValidityMutable(aas_inner).SetInvalid(row * 2 + 1);
	}
	ValidityMask la_before(FlatVector::Validity(ArrayVector::GetChild(la_arrays)), size * 4);
	ValidityMask sa_before(FlatVector::Validity(ArrayVector::GetChild(sa_x)), size * 2);
	auto order = SelectionInputOrder(size);
	CheckedCollections out(buffer_manager, chunk->GetTypes(), 4);
	Split(out, *chunk, order, size, {1, 3, 32, 5});
	CheckedCollections batched(buffer_manager, chunk->GetTypes(), 4);
	SplitBatch(batched, *chunk, order, size, {1, 3, 32, 5});
	CheckedCollections crossing(buffer_manager, chunk->GetTypes(), 1);
	Split(crossing, *chunk, order, size - 8, {size - 8});
	Split(crossing, *chunk, SelectionInputOrder(size, 3), size, {100});
	// through Append(state, chunk), the second append crossing a chunk boundary at a nonzero list child offset
	CheckedCollections appended(buffer_manager, chunk->GetTypes(), 1);
	DataChunk part;
	part.InitializeEmpty(chunk->GetTypes());
	part.Slice(*chunk, SelectionVector(idx_t(0), idx_t(1000)), 1000);
	appended.AppendChunk(0, part);
	appended.AppendChunk(0, *chunk);
	for (auto results : {&out, &batched, &crossing, &appended}) {
		results->Check();
		for (auto &collection : results->collections) {
			// the elements of every NULL array in la's list and in sa.x are NULL, and those of aas's inner arrays
			ColumnDataScanState scan_state;
			DataChunk scanned;
			collection->InitializeScanChunk(scanned);
			collection->InitializeScan(scan_state, ColumnDataScanProperties::DISALLOW_ZERO_COPY);
			idx_t null_arrays = 0;
			while (collection->Scan(scan_state, scanned)) {
				auto &la = ListVector::GetChildMutable(scanned.data[0]);
				auto &sa_x = StructVector::GetEntries(scanned.data[1])[0];
				for (auto arrays : {std::make_pair(&la, ListVector::GetListSize(scanned.data[0])),
				                    std::make_pair(&sa_x, scanned.size())}) {
					for (idx_t a = 0; a < arrays.second; a++) {
						if (FlatVector::Validity(*arrays.first).RowIsValid(a)) {
							continue;
						}
						null_arrays++;
						for (idx_t j = 0; j < 2; j++) {
							REQUIRE(ElementIsNull(ArrayVector::GetChildMutable(*arrays.first), a * 2 + j));
						}
					}
				}
			}
			REQUIRE(null_arrays > 0);
			CountNullArraysWithNullElements(*collection, 2);
		}
	}
	// the input's elements are as they were
	for (idx_t i = 0; i < size * 4; i++) {
		REQUIRE(FlatVector::Validity(ArrayVector::GetChild(la_arrays)).RowIsValid(i) == la_before.RowIsValid(i));
	}
	for (idx_t i = 0; i < size * 2; i++) {
		REQUIRE(FlatVector::Validity(ArrayVector::GetChild(sa_x)).RowIsValid(i) == sa_before.RowIsValid(i));
	}
}

// The child entry of the list column `col` in the first chunk of `collection`
static VectorMetaData &ListChildEntry(ColumnDataCollection &collection, idx_t col) {
	auto &segment = *collection.GetSegments()[0];
	auto &list_vdata = segment.GetVectorData(segment.chunk_data[0].vector_data[col]);
	return segment.GetVectorData(segment.GetChildIndex(list_vdata.child_index));
}

TEST_CASE("ColumnDataCollection appends selected rows after a large list child", "[column_data_selection_append]") {
	if (STANDARD_VECTOR_SIZE < 1024) {
		return;
	}
	DuckDB db(":memory:");
	Connection con(db);
	auto &buffer_manager = BufferManager::GetBufferManager(*db.instance);
	// lists of 3 integers: the first half of a chunk holds more elements than a vector, so Append(state, chunk) into
	// an empty chain stores them as one LARGE_FLAT entry; the selected rows then go into the same chunk after it
	auto result = con.Query("SELECT [i, i + 1, i * 2] AS l, i::VARCHAR AS s FROM range(" +
	                        to_string(STANDARD_VECTOR_SIZE) + ") t(i)");
	REQUIRE_NO_FAIL(*result);
	auto chunk = result->Fetch();
	REQUIRE(chunk);
	const idx_t size = chunk->size();
	const idx_t head = size / 2;
	REQUIRE(head * 3 > STANDARD_VECTOR_SIZE);
	auto order = SelectionInputOrder(size);
	DataChunk first_half;
	first_half.InitializeEmpty(chunk->GetTypes());
	first_half.Slice(*chunk, *FlatVector::IncrementalSelectionVector(), head);
	CheckedCollections single(buffer_manager, chunk->GetTypes(), 1);
	single.AppendChunk(0, first_half);
	REQUIRE(ListChildEntry(*single.collections[0], 0).representation == ColumnDataRepresentation::LARGE_FLAT);
	single.Append(0, *chunk, order, 0, size - head);
	REQUIRE(single.collections[0]->ChunkCount() == 1);
	REQUIRE(ListChildEntry(*single.collections[0], 0).next_data.IsValid());
	single.Check();
	CheckedCollections batch(buffer_manager, chunk->GetTypes(), 2);
	for (idx_t c = 0; c < 2; c++) {
		batch.AppendChunk(c, first_half);
	}
	SplitBatch(batch, *chunk, order, size - head, {(size - head) / 2});
	batch.Check();
}

} // namespace duckdb
