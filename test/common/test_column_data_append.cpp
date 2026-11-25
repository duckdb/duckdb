#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector/array_vector.hpp"
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

} // namespace duckdb
