#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/vector/constant_vector.hpp"
#include "duckdb/common/vector/dictionary_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "test_helpers.hpp"

// These tests pin the compact representation contract of ColumnDataCollection at the C++ API level:
// the vector types handed out by zero-copy scans, the per-segment dictionary deduplication, and the
// flat materialization under DISALLOW_ZERO_COPY. Query results are identical by design, so SQL tests
// cannot observe any of these invariants.

namespace duckdb {

static void AppendConstantChunk(ColumnDataCollection &collection, ColumnDataAppendState &state, const Value &value,
                                idx_t count) {
	DataChunk chunk;
	collection.InitializeScanChunk(chunk);
	chunk.data[0].Reference(value, count_t(count));
	chunk.SetCardinality(count);
	collection.Append(state, chunk);
}

TEST_CASE("Constant vectors keep their representation through a ColumnDataCollection", "[column_data_compact]") {
	if (STANDARD_VECTOR_SIZE < 1024) {
		// the shapes below assume full-size vectors
		return;
	}
	DuckDB db(":memory:");
	auto &buffer_manager = BufferManager::GetBufferManager(*db.instance);
	ColumnDataCollection collection(buffer_manager, {LogicalType::VARCHAR});

	ColumnDataAppendState append_state;
	collection.InitializeAppend(append_state);
	const string payload = "a_payload_string_longer_than_twelve_bytes";
	AppendConstantChunk(collection, append_state, Value(payload), STANDARD_VECTOR_SIZE);

	// a zero-copy scan hands the constant back out as a constant vector
	ColumnDataScanState scan_state;
	DataChunk chunk;
	collection.InitializeScanChunk(chunk);
	collection.InitializeScan(scan_state, ColumnDataScanProperties::ALLOW_ZERO_COPY);
	REQUIRE(collection.Scan(scan_state, chunk));
	REQUIRE(chunk.data[0].GetVectorType() == VectorType::CONSTANT_VECTOR);
	REQUIRE(ConstantVector::GetData<string_t>(chunk.data[0])[0].GetString() == payload);

	// re-appending the scanned chunk keeps the form across the collection hop
	ColumnDataCollection second(buffer_manager, {LogicalType::VARCHAR});
	ColumnDataAppendState second_append;
	second.InitializeAppend(second_append);
	second.Append(second_append, chunk);
	ColumnDataScanState second_scan;
	DataChunk second_chunk;
	second.InitializeScanChunk(second_chunk);
	second.InitializeScan(second_scan, ColumnDataScanProperties::ALLOW_ZERO_COPY);
	REQUIRE(second.Scan(second_scan, second_chunk));
	REQUIRE(second_chunk.data[0].GetVectorType() == VectorType::CONSTANT_VECTOR);

	// DISALLOW_ZERO_COPY keeps handing out self-contained flat vectors
	ColumnDataScanState flat_scan;
	DataChunk flat_chunk;
	collection.InitializeScanChunk(flat_chunk);
	collection.InitializeScan(flat_scan, ColumnDataScanProperties::DISALLOW_ZERO_COPY);
	REQUIRE(collection.Scan(flat_scan, flat_chunk));
	REQUIRE(flat_chunk.data[0].GetVectorType() == VectorType::FLAT_VECTOR);
	auto flat_strings = FlatVector::GetData<string_t>(flat_chunk.data[0]);
	REQUIRE(flat_strings[0].GetString() == payload);
	REQUIRE(flat_strings[STANDARD_VECTOR_SIZE - 1].GetString() == payload);
}

TEST_CASE("Mixed appends materialize constant entries without losing values", "[column_data_compact]") {
	if (STANDARD_VECTOR_SIZE < 1024) {
		// the shapes below assume full-size vectors
		return;
	}
	DuckDB db(":memory:");
	auto &buffer_manager = BufferManager::GetBufferManager(*db.instance);
	ColumnDataCollection collection(buffer_manager, {LogicalType::BIGINT});

	// a partial constant chunk followed by a flat chunk lands in the same entry
	ColumnDataAppendState append_state;
	collection.InitializeAppend(append_state);
	AppendConstantChunk(collection, append_state, Value::BIGINT(7), 100);
	DataChunk flat_input;
	collection.InitializeScanChunk(flat_input);
	{
		auto writer = FlatVector::Writer<int64_t>(flat_input.data[0], 100);
		for (idx_t i = 0; i < 100; i++) {
			writer.WriteValue(UnsafeNumericCast<int64_t>(i));
		}
	}
	flat_input.SetCardinality(100);
	collection.Append(append_state, flat_input);
	// a constant with a different value also appends after materialization
	AppendConstantChunk(collection, append_state, Value::BIGINT(-1), 100);

	REQUIRE(collection.Count() == 300);
	for (idx_t i = 0; i < 100; i++) {
		REQUIRE(collection.GetValue(0, i) == Value::BIGINT(7));
		REQUIRE(collection.GetValue(0, 100 + i) == Value::BIGINT(UnsafeNumericCast<int64_t>(i)));
		REQUIRE(collection.GetValue(0, 200 + i) == Value::BIGINT(-1));
	}
}

TEST_CASE("Dictionary vectors deduplicate per segment and survive scans", "[column_data_compact]") {
	if (STANDARD_VECTOR_SIZE < 1024) {
		// the shapes below assume full-size vectors
		return;
	}
	DuckDB db(":memory:");
	auto &buffer_manager = BufferManager::GetBufferManager(*db.instance);
	ColumnDataCollection collection(buffer_manager, {LogicalType::VARCHAR});

	// a re-occurring dictionary, as emitted by storage and file scans
	const idx_t dict_size = 100;
	auto dictionary = DictionaryVector::CreateReusableDictionary(LogicalType::VARCHAR, dict_size);
	{
		auto writer = FlatVector::Writer<string_t>(dictionary->data, dict_size);
		for (idx_t i = 0; i < dict_size; i++) {
			if (i == 99) {
				writer.WriteNull();
			} else {
				writer.WriteValue(string_t("dictionary_payload_value_" + to_string(i)));
			}
		}
	}

	ColumnDataAppendState append_state;
	collection.InitializeAppend(append_state);
	DataChunk chunk;
	collection.InitializeScanChunk(chunk);
	SelectionVector sel(STANDARD_VECTOR_SIZE);
	for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
		sel.set_index(i, i % dict_size);
	}
	const idx_t append_rounds = 8;
	for (idx_t round = 0; round < append_rounds; round++) {
		chunk.Reset();
		chunk.data[0].Dictionary(dictionary, sel, STANDARD_VECTOR_SIZE);
		chunk.SetCardinality(STANDARD_VECTOR_SIZE);
		collection.Append(append_state, chunk);
	}
	REQUIRE(collection.Count() == append_rounds * STANDARD_VECTOR_SIZE);

	// the dictionary values are stored once per segment: well below one flat copy per chunk
	REQUIRE(collection.SizeInBytes() < append_rounds * STANDARD_VECTOR_SIZE * sizeof(string_t));

	// a zero-copy scan hands dictionary vectors back out, with the dictionary id preserved
	ColumnDataScanState scan_state;
	DataChunk result;
	collection.InitializeScanChunk(result);
	collection.InitializeScan(scan_state, ColumnDataScanProperties::ALLOW_ZERO_COPY);
	REQUIRE(collection.Scan(scan_state, result));
	REQUIRE(result.data[0].GetVectorType() == VectorType::DICTIONARY_VECTOR);
	REQUIRE(DictionaryVector::DictionaryId(result.data[0]) == dictionary->id);
	REQUIRE(DictionaryVector::DictionarySize(result.data[0]).GetIndex() == dict_size);
	REQUIRE(result.GetValue(0, 1) == Value("dictionary_payload_value_1"));
	REQUIRE(result.GetValue(0, 99).IsNull());

	// re-appending the scanned chunk into another collection keeps the compact form
	ColumnDataCollection second(buffer_manager, {LogicalType::VARCHAR});
	ColumnDataAppendState second_append;
	second.InitializeAppend(second_append);
	second.Append(second_append, result);
	ColumnDataScanState second_scan;
	DataChunk second_result;
	second.InitializeScanChunk(second_result);
	second.InitializeScan(second_scan, ColumnDataScanProperties::ALLOW_ZERO_COPY);
	REQUIRE(second.Scan(second_scan, second_result));
	REQUIRE(second_result.data[0].GetVectorType() == VectorType::DICTIONARY_VECTOR);

	// DISALLOW_ZERO_COPY materializes flat vectors with the same values
	ColumnDataScanState flat_scan;
	DataChunk flat_result;
	collection.InitializeScanChunk(flat_result);
	collection.InitializeScan(flat_scan, ColumnDataScanProperties::DISALLOW_ZERO_COPY);
	REQUIRE(collection.Scan(flat_scan, flat_result));
	REQUIRE(flat_result.data[0].GetVectorType() == VectorType::FLAT_VECTOR);
	REQUIRE(flat_result.GetValue(0, 1) == Value("dictionary_payload_value_1"));
	REQUIRE(flat_result.GetValue(0, 99).IsNull());
}

} // namespace duckdb
