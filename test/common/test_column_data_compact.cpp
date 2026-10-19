#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/column/column_data_consumer.hpp"
#include "duckdb/common/vector/constant_vector.hpp"
#include "duckdb/common/vector/dictionary_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/storage/buffer/block_handle.hpp"
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

	// the dictionary values are stored exactly once per segment
	REQUIRE(collection.GetSegments().size() == 1);
	REQUIRE(collection.GetSegments()[0]->dictionaries.size() == 1);
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

	// every chunk shares the same dictionary and resolves its own codes
	ColumnDataScanState full_scan;
	DataChunk full_result;
	collection.InitializeScanChunk(full_result);
	collection.InitializeScan(full_scan, ColumnDataScanProperties::ALLOW_ZERO_COPY);
	idx_t scanned_chunks = 0;
	while (collection.Scan(full_scan, full_result)) {
		REQUIRE(full_result.data[0].GetVectorType() == VectorType::DICTIONARY_VECTOR);
		REQUIRE(full_result.GetValue(0, 42) == Value("dictionary_payload_value_42"));
		REQUIRE(full_result.GetValue(0, 99).IsNull());
		scanned_chunks++;
	}
	REQUIRE(scanned_chunks == append_rounds);
}

TEST_CASE("ColumnDataConsumer handles chunks without blocks", "[column_data_compact]") {
	if (STANDARD_VECTOR_SIZE < 1024) {
		// the shapes below assume full-size vectors
		return;
	}
	DuckDB db(":memory:");
	auto &buffer_manager = BufferManager::GetBufferManager(*db.instance);

	// all-constant chunks store their values in metadata and reference no blocks
	ColumnDataCollection collection(buffer_manager, {LogicalType::BIGINT});
	ColumnDataAppendState append_state;
	collection.InitializeAppend(append_state);
	AppendConstantChunk(collection, append_state, Value::BIGINT(7), STANDARD_VECTOR_SIZE);
	AppendConstantChunk(collection, append_state, Value::BIGINT(7), STANDARD_VECTOR_SIZE);

	ColumnDataConsumer consumer(collection, {0});
	consumer.InitializeScan();
	ColumnDataConsumerScanState state;
	state.current_chunk_state.properties = ColumnDataScanProperties::ALLOW_ZERO_COPY;
	DataChunk chunk;
	collection.InitializeScanChunk(chunk);
	idx_t total = 0;
	while (consumer.AssignChunk(state)) {
		consumer.ScanChunk(state, chunk);
		REQUIRE(chunk.data[0].GetVectorType() == VectorType::CONSTANT_VECTOR);
		REQUIRE(ConstantVector::GetData<int64_t>(chunk.data[0])[0] == 7);
		total += chunk.size();
		consumer.FinishChunk(state);
	}
	REQUIRE(total == 2 * STANDARD_VECTOR_SIZE);

	// chunks with and without blocks mix in the consumed order
	ColumnDataCollection mixed(buffer_manager, {LogicalType::BIGINT});
	ColumnDataAppendState mixed_append;
	mixed.InitializeAppend(mixed_append);
	AppendConstantChunk(mixed, mixed_append, Value::BIGINT(1), STANDARD_VECTOR_SIZE);
	DataChunk flat_input;
	mixed.InitializeScanChunk(flat_input);
	{
		auto writer = FlatVector::Writer<int64_t>(flat_input.data[0], STANDARD_VECTOR_SIZE);
		for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
			writer.WriteValue(UnsafeNumericCast<int64_t>(i));
		}
	}
	flat_input.SetCardinality(STANDARD_VECTOR_SIZE);
	mixed.Append(mixed_append, flat_input);
	AppendConstantChunk(mixed, mixed_append, Value::BIGINT(2), STANDARD_VECTOR_SIZE);

	ColumnDataConsumer mixed_consumer(mixed, {0});
	mixed_consumer.InitializeScan();
	ColumnDataConsumerScanState mixed_state;
	mixed_state.current_chunk_state.properties = ColumnDataScanProperties::ALLOW_ZERO_COPY;
	DataChunk mixed_chunk;
	mixed.InitializeScanChunk(mixed_chunk);
	idx_t rows = 0;
	int64_t sum = 0;
	while (mixed_consumer.AssignChunk(mixed_state)) {
		mixed_consumer.ScanChunk(mixed_state, mixed_chunk);
		auto values = mixed_chunk.data[0].Values<int64_t>();
		for (auto entry : values) {
			sum += entry.GetValue();
		}
		rows += mixed_chunk.size();
		mixed_consumer.FinishChunk(mixed_state);
	}
	REQUIRE(rows == 3 * STANDARD_VECTOR_SIZE);
	const auto n = int64_t(STANDARD_VECTOR_SIZE);
	REQUIRE(sum == n + 2 * n + n * (n - 1) / 2);
}

TEST_CASE("Compact collections account their stored bytes", "[column_data_compact]") {
	if (STANDARD_VECTOR_SIZE < 1024) {
		// the shapes below assume full-size vectors
		return;
	}
	DuckDB db(":memory:");
	auto &buffer_manager = BufferManager::GetBufferManager(*db.instance);
	ColumnDataCollection collection(buffer_manager, {LogicalType::BIGINT});
	ColumnDataAppendState append_state;
	collection.InitializeAppend(append_state);
	for (idx_t i = 0; i < 3; i++) {
		AppendConstantChunk(collection, append_state, Value::BIGINT(7), STANDARD_VECTOR_SIZE);
	}
	REQUIRE(collection.Count() == 3 * STANDARD_VECTOR_SIZE);
	// a nonempty all-constant collection reports its metadata-resident bytes
	REQUIRE(collection.SizeInBytes() > 0);
	REQUIRE(collection.AllocationSize() > 0);
	REQUIRE(collection.AllocationSize() >= collection.SizeInBytes());
	// and stays far below one flat vector
	REQUIRE(collection.SizeInBytes() < 4096);
	// the per-chunk allocation attribution is nonzero as well
	auto &segment = *collection.GetSegments()[0];
	REQUIRE(segment.GetChunkAllocationSize(0) > 0);

	// materialization retires the constant charge from the per-chunk attribution as well
	ColumnDataCollection mixed(buffer_manager, {LogicalType::BIGINT});
	ColumnDataAppendState mixed_append;
	mixed.InitializeAppend(mixed_append);
	AppendConstantChunk(mixed, mixed_append, Value::BIGINT(7), 100);
	DataChunk flat_input;
	mixed.InitializeScanChunk(flat_input);
	{
		auto writer = FlatVector::Writer<int64_t>(flat_input.data[0], 100);
		for (idx_t i = 0; i < 100; i++) {
			writer.WriteValue(UnsafeNumericCast<int64_t>(i));
		}
	}
	flat_input.SetCardinality(100);
	mixed.Append(mixed_append, flat_input);
	auto &mixed_segment = *mixed.GetSegments()[0];
	const idx_t flat_entry_size =
	    ColumnDataCollectionSegment::GetDataSize(sizeof(int64_t)) + ValidityMask::STANDARD_MASK_SIZE;
	REQUIRE(mixed_segment.constant_metadata_size == 0);
	REQUIRE(mixed.SizeInBytes() == flat_entry_size);
	REQUIRE(mixed_segment.GetChunkAllocationSize(0) == flat_entry_size);
}

TEST_CASE("Repeated string bytes stay shared when compact recognition does not apply", "[column_data_compact]") {
	if (STANDARD_VECTOR_SIZE < 1024) {
		// the shapes below assume full-size vectors
		return;
	}
	DuckDB db(":memory:");
	auto &buffer_manager = BufferManager::GetBufferManager(*db.instance);
	const idx_t payload_size = 131072;
	const string payload(payload_size, 'x');

	// managed result collections keep flat entries but must still share the constant's bytes
	ColumnDataCollection managed(buffer_manager, {LogicalType::VARCHAR},
	                             ColumnDataCollectionLifetime::THROW_ERROR_AFTER_DATABASE_CLOSES);
	ColumnDataAppendState managed_append;
	managed.InitializeAppend(managed_append);
	AppendConstantChunk(managed, managed_append, Value(payload), STANDARD_VECTOR_SIZE);
	ColumnDataScanState scan_state;
	DataChunk chunk;
	managed.InitializeScanChunk(chunk);
	managed.InitializeScan(scan_state, ColumnDataScanProperties::ALLOW_ZERO_COPY);
	REQUIRE(managed.Scan(scan_state, chunk));
	REQUIRE(chunk.data[0].GetVectorType() == VectorType::FLAT_VECTOR);
	auto managed_strings = FlatVector::GetData<string_t>(chunk.data[0]);
	REQUIRE(managed_strings[0].GetString() == payload);
	REQUIRE(managed_strings[STANDARD_VECTOR_SIZE - 1].GetString() == payload);
	// one copy of the bytes plus the entry array, not one copy per row
	REQUIRE(managed.SizeInBytes() < 2 * payload_size);

	// an entry that already holds flat rows cannot become compact but still shares the bytes
	ColumnDataCollection collection(buffer_manager, {LogicalType::VARCHAR});
	ColumnDataAppendState append_state;
	collection.InitializeAppend(append_state);
	DataChunk flat_input;
	collection.InitializeScanChunk(flat_input);
	{
		auto writer = FlatVector::Writer<string_t>(flat_input.data[0], 1);
		writer.WriteValue(string_t("short"));
	}
	flat_input.SetCardinality(1);
	collection.Append(append_state, flat_input);
	AppendConstantChunk(collection, append_state, Value(payload), STANDARD_VECTOR_SIZE - 1);
	REQUIRE(collection.Count() == STANDARD_VECTOR_SIZE);
	REQUIRE(collection.GetValue(0, 0) == Value("short"));
	REQUIRE(collection.GetValue(0, 1) == Value(payload));
	REQUIRE(collection.GetValue(0, STANDARD_VECTOR_SIZE - 1) == Value(payload));
	REQUIRE(collection.SizeInBytes() < 2 * payload_size);
}

static buffer_ptr<DictionaryEntry> MakeStringDictionary(idx_t dict_size, const string &prefix, bool with_null,
                                                        bool with_id = true) {
	auto dictionary = DictionaryVector::CreateReusableDictionary(LogicalType::VARCHAR, dict_size);
	if (!with_id) {
		dictionary->id = string();
	}
	auto writer = FlatVector::Writer<string_t>(dictionary->data, dict_size);
	for (idx_t i = 0; i < dict_size; i++) {
		if (with_null && i + 1 == dict_size) {
			writer.WriteNull();
		} else {
			writer.WriteValue(string_t(prefix + to_string(i)));
		}
	}
	return dictionary;
}

static void AppendDictionaryChunk(ColumnDataCollection &collection, ColumnDataAppendState &state,
                                  buffer_ptr<DictionaryEntry> dictionary, idx_t dict_size, idx_t count) {
	DataChunk chunk;
	collection.InitializeScanChunk(chunk);
	SelectionVector sel(count);
	for (idx_t i = 0; i < count; i++) {
		sel.set_index(i, i % dict_size);
	}
	chunk.data[0].Dictionary(std::move(dictionary), sel, count);
	chunk.SetCardinality(count);
	collection.Append(state, chunk);
}

TEST_CASE("Compact string entries survive block relocation", "[column_data_compact]") {
	if (STANDARD_VECTOR_SIZE < 1024) {
		// the shapes below assume full-size vectors
		return;
	}
	auto config = GetTestConfig();
	config->SetOptionByName("temp_directory", TestCreatePath("compact_relocation.tmp"));
	DuckDB db(nullptr, config.get());
	auto &buffer_manager = BufferManager::GetBufferManager(*db.instance);
	ColumnDataCollection collection(buffer_manager, {LogicalType::VARCHAR, LogicalType::VARCHAR});

	const idx_t dict_size = 50;
	const string prefix = "a_non_inlined_dictionary_payload_";
	auto dictionary = MakeStringDictionary(dict_size, prefix, true);
	const string constant_payload = "a_non_inlined_constant_payload_string";

	ColumnDataAppendState append_state;
	collection.InitializeAppend(append_state);
	DataChunk input;
	collection.InitializeScanChunk(input);
	SelectionVector sel(STANDARD_VECTOR_SIZE);
	for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
		sel.set_index(i, i % dict_size);
	}
	for (idx_t round = 0; round < 2; round++) {
		input.Reset();
		input.data[0].Dictionary(dictionary, sel, STANDARD_VECTOR_SIZE);
		input.data[1].Reference(Value(constant_payload), count_t(STANDARD_VECTOR_SIZE));
		input.SetCardinality(STANDARD_VECTOR_SIZE);
		collection.Append(append_state, input);
	}

	// release the append pins so the blocks can be unloaded
	vector<shared_ptr<BlockHandle>> handles;
	for (const auto &entry : append_state.current_chunk_state.handles) {
		handles.push_back(entry.second.GetBlockHandle());
	}
	REQUIRE(!handles.empty());
	append_state.current_chunk_state.handles.clear();

	for (idx_t round = 0; round < 2; round++) {
		// force every block to a different address, poisoning the old allocations
		vector<unique_ptr<FileBuffer>> old_buffers;
		vector<BufferHandle> pins;
		for (auto &handle : handles) {
			auto lock = handle->GetMemory().GetLock();
			REQUIRE(handle->GetMemory().CanUnload() == CanUnloadResult::CAN_UNLOAD);
			old_buffers.push_back(handle->GetMemory().UnloadAndTakeBlock(lock));
		}
		for (idx_t i = 0; i < handles.size(); i++) {
			pins.push_back(buffer_manager.Pin(handles[i]));
			REQUIRE(pins.back().Ptr() != old_buffers[i]->GetData());
			old_buffers[i]->Clear();
		}

		// the dictionary heap pointers are repaired and the constant pointer reconstructed
		const auto properties =
		    round == 0 ? ColumnDataScanProperties::ALLOW_ZERO_COPY : ColumnDataScanProperties::DISALLOW_ZERO_COPY;
		ColumnDataScanState scan_state;
		DataChunk result;
		collection.InitializeScanChunk(result);
		collection.InitializeScan(scan_state, properties);
		idx_t chunks = 0;
		while (collection.Scan(scan_state, result)) {
			REQUIRE(result.GetValue(0, 1) == Value(prefix + "1"));
			REQUIRE(result.GetValue(0, dict_size - 1).IsNull());
			REQUIRE(result.GetValue(1, 0) == Value(constant_payload));
			REQUIRE(result.GetValue(1, STANDARD_VECTOR_SIZE - 1) == Value(constant_payload));
			chunks++;
		}
		REQUIRE(chunks == 2);
	}
}

TEST_CASE("DISALLOW_ZERO_COPY copies outlive the source collection", "[column_data_compact]") {
	if (STANDARD_VECTOR_SIZE < 1024) {
		// the shapes below assume full-size vectors
		return;
	}
	DuckDB db(":memory:");
	auto &buffer_manager = BufferManager::GetBufferManager(*db.instance);
	const string constant_payload = "a_non_inlined_constant_payload_string";
	const string prefix = "a_non_inlined_dictionary_payload_";
	const idx_t dict_size = 50;

	DataChunk result;
	{
		ColumnDataCollection collection(buffer_manager, {LogicalType::VARCHAR, LogicalType::VARCHAR});
		ColumnDataAppendState append_state;
		collection.InitializeAppend(append_state);
		DataChunk input;
		collection.InitializeScanChunk(input);
		SelectionVector sel(STANDARD_VECTOR_SIZE);
		for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
			sel.set_index(i, i % dict_size);
		}
		input.data[0].Dictionary(MakeStringDictionary(dict_size, prefix, false), sel, STANDARD_VECTOR_SIZE);
		input.data[1].Reference(Value(constant_payload), count_t(STANDARD_VECTOR_SIZE));
		input.SetCardinality(STANDARD_VECTOR_SIZE);
		collection.Append(append_state, input);

		collection.InitializeScanChunk(Allocator::DefaultAllocator(), result);
		ColumnDataScanState scan_state;
		collection.InitializeScan(scan_state, ColumnDataScanProperties::DISALLOW_ZERO_COPY);
		REQUIRE(collection.Scan(scan_state, result));
		// the collection, its blocks, and the scan state are destroyed here
	}
	REQUIRE(result.size() == STANDARD_VECTOR_SIZE);
	REQUIRE(result.GetValue(0, 1) == Value(prefix + "1"));
	REQUIRE(result.GetValue(0, dict_size) == Value(prefix + "0"));
	REQUIRE(result.GetValue(1, 0) == Value(constant_payload));
	REQUIRE(result.GetValue(1, STANDARD_VECTOR_SIZE - 1) == Value(constant_payload));
}

TEST_CASE("Dictionary recognition gate boundaries", "[column_data_compact]") {
	if (STANDARD_VECTOR_SIZE < 1024) {
		// the shapes below assume full-size vectors
		return;
	}
	DuckDB db(":memory:");
	auto &buffer_manager = BufferManager::GetBufferManager(*db.instance);

	auto scan_first = [&](ColumnDataCollection &collection, DataChunk &result) {
		collection.InitializeScanChunk(result);
		ColumnDataScanState scan_state;
		collection.InitializeScan(scan_state, ColumnDataScanProperties::ALLOW_ZERO_COPY);
		REQUIRE(collection.Scan(scan_state, result));
	};

	// a dictionary of exactly STANDARD_VECTOR_SIZE values is recognized
	{
		ColumnDataCollection collection(buffer_manager, {LogicalType::VARCHAR});
		ColumnDataAppendState append_state;
		collection.InitializeAppend(append_state);
		AppendDictionaryChunk(collection, append_state, MakeStringDictionary(STANDARD_VECTOR_SIZE, "max_size_", false),
		                      STANDARD_VECTOR_SIZE, STANDARD_VECTOR_SIZE);
		DataChunk result;
		scan_first(collection, result);
		REQUIRE(result.data[0].GetVectorType() == VectorType::DICTIONARY_VECTOR);
		REQUIRE(result.GetValue(0, 7) == Value("max_size_7"));
	}
	// one value more falls back to flat storage
	{
		ColumnDataCollection collection(buffer_manager, {LogicalType::VARCHAR});
		ColumnDataAppendState append_state;
		collection.InitializeAppend(append_state);
		AppendDictionaryChunk(collection, append_state,
		                      MakeStringDictionary(STANDARD_VECTOR_SIZE + 1, "oversized_", false),
		                      STANDARD_VECTOR_SIZE + 1, STANDARD_VECTOR_SIZE);
		DataChunk result;
		scan_first(collection, result);
		REQUIRE(result.data[0].GetVectorType() == VectorType::FLAT_VECTOR);
		REQUIRE(result.GetValue(0, 7) == Value("oversized_7"));
	}
	// a dictionary without an id is an ad hoc selection and falls back to flat storage
	{
		ColumnDataCollection collection(buffer_manager, {LogicalType::VARCHAR});
		ColumnDataAppendState append_state;
		collection.InitializeAppend(append_state);
		AppendDictionaryChunk(collection, append_state, MakeStringDictionary(100, "no_id_", false, false), 100,
		                      STANDARD_VECTOR_SIZE);
		DataChunk result;
		scan_first(collection, result);
		REQUIRE(result.data[0].GetVectorType() == VectorType::FLAT_VECTOR);
		REQUIRE(result.GetValue(0, 7) == Value("no_id_7"));
	}
	// values narrower than the codes fall back to flat storage
	{
		ColumnDataCollection collection(buffer_manager, {LogicalType::USMALLINT});
		ColumnDataAppendState append_state;
		collection.InitializeAppend(append_state);
		auto dictionary = DictionaryVector::CreateReusableDictionary(LogicalType::USMALLINT, 100);
		{
			auto writer = FlatVector::Writer<uint16_t>(dictionary->data, 100);
			for (idx_t i = 0; i < 100; i++) {
				writer.WriteValue(UnsafeNumericCast<uint16_t>(i));
			}
		}
		AppendDictionaryChunk(collection, append_state, std::move(dictionary), 100, STANDARD_VECTOR_SIZE);
		DataChunk result;
		scan_first(collection, result);
		REQUIRE(result.data[0].GetVectorType() == VectorType::FLAT_VECTOR);
		REQUIRE(result.GetValue(0, 7) == Value::USMALLINT(7));
	}
	// dictionary string bytes beyond one block fall back to flat storage with shared bytes
	{
		ColumnDataCollection collection(buffer_manager, {LogicalType::VARCHAR});
		ColumnDataAppendState append_state;
		collection.InitializeAppend(append_state);
		const idx_t value_size = 200000;
		auto dictionary = DictionaryVector::CreateReusableDictionary(LogicalType::VARCHAR, 2);
		{
			auto writer = FlatVector::Writer<string_t>(dictionary->data, 2);
			writer.WriteValue(string_t(string(value_size, 'a')));
			writer.WriteValue(string_t(string(value_size, 'b')));
		}
		AppendDictionaryChunk(collection, append_state, std::move(dictionary), 2, STANDARD_VECTOR_SIZE);
		DataChunk result;
		scan_first(collection, result);
		REQUIRE(result.data[0].GetVectorType() == VectorType::FLAT_VECTOR);
		REQUIRE(result.GetValue(0, 0) == Value(string(value_size, 'a')));
		REQUIRE(result.GetValue(0, 1) == Value(string(value_size, 'b')));
		// the fallback shares the bytes instead of copying them per row
		REQUIRE(collection.SizeInBytes() < 4 * value_size);
	}
}

TEST_CASE("Compact entries extend and materialize across appends", "[column_data_compact]") {
	if (STANDARD_VECTOR_SIZE < 1024) {
		// the shapes below assume full-size vectors
		return;
	}
	DuckDB db(":memory:");
	auto &buffer_manager = BufferManager::GetBufferManager(*db.instance);
	const string payload = "a_non_inlined_constant_payload_string";
	const string other_payload = "a_different_non_inlined_constant_value";

	// partial appends of the same constant extend one constant entry
	{
		ColumnDataCollection collection(buffer_manager, {LogicalType::VARCHAR});
		ColumnDataAppendState append_state;
		collection.InitializeAppend(append_state);
		AppendConstantChunk(collection, append_state, Value(payload), 100);
		AppendConstantChunk(collection, append_state, Value(payload), 100);
		DataChunk result;
		collection.InitializeScanChunk(result);
		ColumnDataScanState scan_state;
		collection.InitializeScan(scan_state, ColumnDataScanProperties::ALLOW_ZERO_COPY);
		REQUIRE(collection.Scan(scan_state, result));
		REQUIRE(result.data[0].GetVectorType() == VectorType::CONSTANT_VECTOR);
		REQUIRE(result.size() == 200);
		REQUIRE(result.GetValue(0, 199) == Value(payload));
	}
	// a different constant value materializes the entry and continues flat
	{
		ColumnDataCollection collection(buffer_manager, {LogicalType::VARCHAR});
		ColumnDataAppendState append_state;
		collection.InitializeAppend(append_state);
		AppendConstantChunk(collection, append_state, Value(payload), 100);
		AppendConstantChunk(collection, append_state, Value(other_payload), 100);
		REQUIRE(collection.GetValue(0, 0) == Value(payload));
		REQUIRE(collection.GetValue(0, 99) == Value(payload));
		REQUIRE(collection.GetValue(0, 100) == Value(other_payload));
		REQUIRE(collection.GetValue(0, 199) == Value(other_payload));
	}
	// NULL and non-NULL constants do not extend each other
	{
		ColumnDataCollection collection(buffer_manager, {LogicalType::BIGINT});
		ColumnDataAppendState append_state;
		collection.InitializeAppend(append_state);
		AppendConstantChunk(collection, append_state, Value(LogicalType::BIGINT), 100);
		AppendConstantChunk(collection, append_state, Value::BIGINT(5), 100);
		REQUIRE(collection.GetValue(0, 0).IsNull());
		REQUIRE(collection.GetValue(0, 99).IsNull());
		REQUIRE(collection.GetValue(0, 100) == Value::BIGINT(5));
		REQUIRE(collection.GetValue(0, 199) == Value::BIGINT(5));
	}
	// partial appends from the same dictionary extend one dictionary entry
	{
		ColumnDataCollection collection(buffer_manager, {LogicalType::VARCHAR});
		ColumnDataAppendState append_state;
		collection.InitializeAppend(append_state);
		auto dictionary = MakeStringDictionary(50, "extended_dictionary_payload_", false);
		AppendDictionaryChunk(collection, append_state, dictionary, 50, 100);
		AppendDictionaryChunk(collection, append_state, dictionary, 50, 100);
		REQUIRE(collection.GetSegments()[0]->dictionaries.size() == 1);
		DataChunk result;
		collection.InitializeScanChunk(result);
		ColumnDataScanState scan_state;
		collection.InitializeScan(scan_state, ColumnDataScanProperties::ALLOW_ZERO_COPY);
		REQUIRE(collection.Scan(scan_state, result));
		REQUIRE(result.data[0].GetVectorType() == VectorType::DICTIONARY_VECTOR);
		REQUIRE(result.size() == 200);
		REQUIRE(result.GetValue(0, 149) == Value("extended_dictionary_payload_49"));
	}
	// a dictionary with a different id materializes the entry and continues flat
	{
		ColumnDataCollection collection(buffer_manager, {LogicalType::VARCHAR});
		ColumnDataAppendState append_state;
		collection.InitializeAppend(append_state);
		AppendDictionaryChunk(collection, append_state, MakeStringDictionary(50, "first_dictionary_payload_", true), 50,
		                      100);
		AppendDictionaryChunk(collection, append_state, MakeStringDictionary(50, "second_dictionary_payload_", false),
		                      50, 100);
		DataChunk result;
		collection.InitializeScanChunk(result);
		ColumnDataScanState scan_state;
		collection.InitializeScan(scan_state, ColumnDataScanProperties::ALLOW_ZERO_COPY);
		REQUIRE(collection.Scan(scan_state, result));
		REQUIRE(result.data[0].GetVectorType() == VectorType::FLAT_VECTOR);
		REQUIRE(result.GetValue(0, 1) == Value("first_dictionary_payload_1"));
		REQUIRE(result.GetValue(0, 49).IsNull());
		REQUIRE(result.GetValue(0, 101) == Value("second_dictionary_payload_1"));
	}
}

TEST_CASE("Compact entries move with Combine, Swap, and reuse", "[column_data_compact]") {
	if (STANDARD_VECTOR_SIZE < 1024) {
		// the shapes below assume full-size vectors
		return;
	}
	DuckDB db(":memory:");
	auto &buffer_manager = BufferManager::GetBufferManager(*db.instance);
	const string payload = "a_non_inlined_constant_payload_string";

	ColumnDataCollection first(buffer_manager, {LogicalType::VARCHAR});
	ColumnDataAppendState first_append;
	first.InitializeAppend(first_append);
	AppendConstantChunk(first, first_append, Value(payload), STANDARD_VECTOR_SIZE);

	ColumnDataCollection second(buffer_manager, {LogicalType::VARCHAR});
	ColumnDataAppendState second_append;
	second.InitializeAppend(second_append);
	AppendDictionaryChunk(second, second_append, MakeStringDictionary(50, "combined_dictionary_payload_", false), 50,
	                      STANDARD_VECTOR_SIZE);

	first.Combine(second);
	REQUIRE(first.Count() == 2 * STANDARD_VECTOR_SIZE);
	REQUIRE(first.GetValue(0, 0) == Value(payload));
	REQUIRE(first.GetValue(0, STANDARD_VECTOR_SIZE + 1) == Value("combined_dictionary_payload_1"));

	ColumnDataCollection swapped(buffer_manager, {LogicalType::VARCHAR});
	swapped.Swap(first);
	REQUIRE(swapped.Count() == 2 * STANDARD_VECTOR_SIZE);
	REQUIRE(swapped.GetValue(0, 0) == Value(payload));

	// reuse after a reset starts from clean per-segment dictionary state
	swapped.ResetForReuse();
	REQUIRE(swapped.Count() == 0);
	ColumnDataAppendState reuse_append;
	swapped.InitializeAppend(reuse_append);
	AppendConstantChunk(swapped, reuse_append, Value(payload), 100);
	REQUIRE(swapped.Count() == 100);
	REQUIRE(swapped.GetValue(0, 99) == Value(payload));
}

TEST_CASE("Compact representations with in-memory and hybrid allocators", "[column_data_compact]") {
	if (STANDARD_VECTOR_SIZE < 1024) {
		// the shapes below assume full-size vectors
		return;
	}
	DuckDB db(":memory:");
	Connection con(db);
	const string payload = "a_non_inlined_constant_payload_string";
	const string prefix = "a_non_inlined_dictionary_payload_";

	auto in_memory = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(),
	                                                 vector<LogicalType> {LogicalType::VARCHAR, LogicalType::VARCHAR});
	auto hybrid =
	    make_uniq<ColumnDataCollection>(*con.context, vector<LogicalType> {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                    ColumnDataAllocatorType::HYBRID);
	for (auto collection_ptr : {in_memory.get(), hybrid.get()}) {
		auto &collection = *collection_ptr;
		ColumnDataAppendState append_state;
		collection.InitializeAppend(append_state);
		DataChunk input;
		collection.InitializeScanChunk(input);
		SelectionVector sel(STANDARD_VECTOR_SIZE);
		for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
			sel.set_index(i, i % 50);
		}
		input.data[0].Dictionary(MakeStringDictionary(50, prefix, true), sel, STANDARD_VECTOR_SIZE);
		input.data[1].Reference(Value(payload), count_t(STANDARD_VECTOR_SIZE));
		input.SetCardinality(STANDARD_VECTOR_SIZE);
		collection.Append(append_state, input);

		for (const auto properties :
		     {ColumnDataScanProperties::ALLOW_ZERO_COPY, ColumnDataScanProperties::DISALLOW_ZERO_COPY}) {
			ColumnDataScanState scan_state;
			DataChunk result;
			collection.InitializeScanChunk(result);
			collection.InitializeScan(scan_state, properties);
			REQUIRE(collection.Scan(scan_state, result));
			if (properties == ColumnDataScanProperties::ALLOW_ZERO_COPY) {
				REQUIRE(result.data[0].GetVectorType() == VectorType::DICTIONARY_VECTOR);
				REQUIRE(result.data[1].GetVectorType() == VectorType::CONSTANT_VECTOR);
			} else {
				REQUIRE(result.data[0].GetVectorType() == VectorType::FLAT_VECTOR);
				REQUIRE(result.data[1].GetVectorType() == VectorType::FLAT_VECTOR);
			}
			REQUIRE(result.GetValue(0, 1) == Value(prefix + "1"));
			REQUIRE(result.GetValue(0, 49).IsNull());
			REQUIRE(result.GetValue(1, 0) == Value(payload));
			REQUIRE(result.GetValue(1, STANDARD_VECTOR_SIZE - 1) == Value(payload));
		}
	}
}

} // namespace duckdb
