#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/common/types/row/integer_partitioned_tuple_data.hpp"
#include "duckdb/common/types/row/tuple_data_collection.hpp"
#include "duckdb/common/types/row/tuple_data_iterator.hpp"
#include "duckdb/common/vector/vector_writer.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

TEST_CASE("Variable-width tuple appends preserve partition offsets", "[tuple_data_allocation]") {
	if (STANDARD_VECTOR_SIZE < 16) {
		return;
	}
	for (auto copy_rows : {false, true}) {
		DYNAMIC_SECTION((copy_rows ? "Copy rows" : "Scatter vectors")) {
			DuckDB db(nullptr);
			Connection con(db);
			auto &buffer_manager = BufferManager::GetBufferManager(*con.context);
			vector<LogicalType> types {LogicalType::BIGINT, LogicalType::VARCHAR,
			                           LogicalType::LIST(LogicalType::VARCHAR)};
			auto layout = make_shared_ptr<TupleDataLayout>();
			layout->Initialize(types, TupleDataValidityType::CAN_HAVE_NULL_VALUES);
			IntegerPartitionedTupleData target(buffer_manager, layout, MemoryTag::ORDER_BY, 0, Value::BIGINT(0), 3);
			PartitionedTupleDataAppendState target_state;
			target.InitializeAppendState(target_state);
			vector<vector<vector<Value>>> expected(target.GetPartitions().size());
			DataChunk chunk;
			chunk.Initialize(Allocator::DefaultAllocator(), types);
			SelectionVector selection(8);
			const idx_t selected_rows[] = {7, 2, 4, 3, 1, 5, 0, 7};
			for (idx_t i = 0; i < 8; i++) {
				selection.set_index(i, selected_rows[i]);
			}
			for (idx_t batch = 0; batch < 4; batch++) {
				chunk.Reset();
				{
					auto keys = FlatVector::Writer<int64_t>(chunk.data[0], 8);
					auto strings = FlatVector::Writer<string_t>(chunk.data[1], 8);
					auto lists = FlatVector::Writer<VectorListType<string_t>>(chunk.data[2], 8);
					for (idx_t i = 0; i < 8; i++) {
						keys.WriteValue(NumericCast<int64_t>(i % 3));
						// The last batch exceeds a heap block while still fitting in the row block.
						const auto length = batch == 3 ? buffer_manager.GetBlockSize() / 2 : 20 + i * 31 + batch;
						const string payload(length, static_cast<char>('a' + i + batch));
						if (batch == 1 && i % 2 == 0) {
							strings.WriteNull();
							lists.WriteNull();
						} else {
							strings.WriteValue(string_t(payload));
							for (auto &element : lists.WriteList(2)) {
								element.WriteValue(string_t(payload.substr(0, 20 + i)));
							}
						}
					}
				}
				for (auto row : selected_rows) {
					vector<Value> values;
					for (idx_t col = 0; col < types.size(); col++) {
						values.push_back(chunk.GetValue(col, row));
					}
					expected[row % 3].push_back(std::move(values));
				}
				if (copy_rows) {
					TupleDataCollection source(buffer_manager, layout, MemoryTag::ORDER_BY);
					source.Append(chunk);
					TupleDataChunkIterator iterator(source, TupleDataPinProperties::KEEP_EVERYTHING_PINNED, true);
					target.Append(target_state, iterator.GetChunkState(), 8, selection);
				} else {
					target.Append(target_state, chunk, selection, 8);
				}
			}
			target.FlushAppendState(target_state);
			REQUIRE(target.Count() == 32);
			for (idx_t partition_idx = 0; partition_idx < expected.size(); partition_idx++) {
				auto &partition = *target.GetPartitions()[partition_idx];
				partition.Unpin();
				TupleDataScanState scan;
				partition.InitializeScan(scan);
				idx_t row = 0;
				while (partition.Scan(scan, chunk)) {
					for (idx_t i = 0; i < chunk.size(); i++) {
						REQUIRE(row < expected[partition_idx].size());
						for (idx_t col = 0; col < types.size(); col++) {
							REQUIRE(Value::NotDistinctFrom(chunk.GetValue(col, i), expected[partition_idx][row][col]));
						}
						row++;
					}
				}
				REQUIRE(row == expected[partition_idx].size());
			}
		}
	}
}

TEST_CASE("Variable-width tuple appends cross empty heaps", "[tuple_data_allocation]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &buffer_manager = BufferManager::GetBufferManager(*con.context);
	vector<LogicalType> types {LogicalType::VARCHAR};
	auto layout = make_shared_ptr<TupleDataLayout>();
	layout->Initialize(types, TupleDataValidityType::CAN_HAVE_NULL_VALUES);
	TupleDataCollection collection(buffer_manager, layout, MemoryTag::ORDER_BY);
	TupleDataAppendState state;
	collection.InitializeAppend(state);
	vector<Value> expected {Value("inline"), Value(LogicalType::VARCHAR), Value(string(100, 'a')),
	                        Value("inline"), Value(string(200, 'b')),     Value(LogicalType::VARCHAR)};
	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), types);
	for (auto &value : expected) {
		chunk.Reset();
		chunk.data[0].Reference(value, count_t(1));
		collection.Append(state, chunk);
	}
	collection.FinalizePinState(state.pin_state);
	collection.Unpin();
	TupleDataScanState scan;
	collection.InitializeScan(scan);
	idx_t row = 0;
	while (collection.Scan(scan, chunk)) {
		for (idx_t i = 0; i < chunk.size(); i++) {
			REQUIRE(row < expected.size());
			REQUIRE(Value::NotDistinctFrom(chunk.GetValue(0, i), expected[row++]));
		}
	}
	REQUIRE(row == expected.size());
}

TEST_CASE("Variable-width tuple appends after heap eviction", "[tuple_data_allocation]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &buffer_manager = BufferManager::GetBufferManager(*con.context);
	vector<LogicalType> types {LogicalType::VARCHAR};
	auto layout = make_shared_ptr<TupleDataLayout>();
	layout->Initialize(types, TupleDataValidityType::CAN_HAVE_NULL_VALUES);
	TupleDataCollection collection(buffer_manager, layout, MemoryTag::ORDER_BY);
	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), types);
	chunk.data[0].Reference(Value(string(1000, 'a')), count_t(1));
	collection.Append(chunk);
	collection.Unpin();

	// Fill the memory budget with pinned buffers, evicting the collection's row and heap blocks.
	const auto original_limit = buffer_manager.GetMaxMemory();
	const auto metadata_size = buffer_manager.GetUsedMemory() - collection.GetBlockAllocationSize();
	buffer_manager.SetMemoryLimit(metadata_size + 3 * buffer_manager.GetBlockAllocSize());
	vector<BufferHandle> held_buffers;
	for (idx_t i = 0; i < 3; i++) {
		held_buffers.push_back(buffer_manager.Allocate(MemoryTag::ORDER_BY, buffer_manager.GetBlockSize(), false));
	}
	buffer_manager.SetMemoryLimit(original_limit);

	chunk.Reset();
	chunk.data[0].Reference(Value(string(2000, 'b')), count_t(1));
	collection.Append(chunk);
	TupleDataScanState scan;
	collection.InitializeScan(scan);
	idx_t row = 0;
	while (collection.Scan(scan, chunk)) {
		for (idx_t i = 0; i < chunk.size(); i++, row++) {
			REQUIRE(row < 2);
			const auto expected = Value(string(row == 0 ? 1000 : 2000, row == 0 ? 'a' : 'b'));
			REQUIRE(Value::NotDistinctFrom(chunk.GetValue(0, i), expected));
		}
	}
	REQUIRE(row == 2);
}

TEST_CASE("Integer-partitioned row append respects the source selection", "[tuple_data_allocation]") {
	if (STANDARD_VECTOR_SIZE < 6) {
		return;
	}
	DuckDB db(nullptr);
	Connection con(db);
	auto &buffer_manager = BufferManager::GetBufferManager(*con.context);
	vector<LogicalType> types {LogicalType::BIGINT};
	auto layout = make_shared_ptr<TupleDataLayout>();
	layout->Initialize(types, TupleDataValidityType::CAN_HAVE_NULL_VALUES);
	TupleDataCollection source(buffer_manager, layout, MemoryTag::ORDER_BY);
	TupleDataAppendState source_state;
	source.InitializeAppend(source_state, TupleDataPinProperties::KEEP_EVERYTHING_PINNED);
	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), types);
	{
		auto keys = FlatVector::Writer<int64_t>(chunk.data[0], 6);
		keys.WriteValue(0);
		keys.WriteValue(1);
		keys.WriteValue(2);
		keys.WriteNull();
		keys.WriteValue(99);
		keys.WriteValue(-1);
	}
	source.Append(source_state, chunk);

	IntegerPartitionedTupleData target(buffer_manager, layout, MemoryTag::ORDER_BY, 0, Value::BIGINT(0), 3);
	PartitionedTupleDataAppendState target_state;
	target.InitializeAppendState(target_state);
	SelectionVector selection(5);
	const idx_t selected_rows[] = {4, 2, 3, 5, 2};
	for (idx_t i = 0; i < 5; i++) {
		selection.set_index(i, selected_rows[i]);
	}
	target.Append(target_state, source_state.chunk_state, 5, selection);
	target.FlushAppendState(target_state);
	REQUIRE(target.Count() == 5);

	vector<vector<Value>> expected {{},
	                                {},
	                                {Value::BIGINT(2), Value::BIGINT(2)},
	                                {Value(LogicalType::BIGINT)},
	                                {Value::BIGINT(99), Value::BIGINT(-1)}};
	for (idx_t i = 0; i < expected.size(); i++) {
		auto &partition = *target.GetPartitions()[i];
		REQUIRE(partition.Count() == expected[i].size());
		TupleDataScanState scan;
		partition.InitializeScan(scan);
		idx_t row = 0;
		while (partition.Scan(scan, chunk)) {
			for (idx_t j = 0; j < chunk.size(); j++) {
				REQUIRE(Value::NotDistinctFrom(chunk.GetValue(0, j), expected[i][row++]));
			}
		}
		REQUIRE(row == expected[i].size());
	}
}

TEST_CASE("Tuple data allocation accounting follows retained blocks", "[tuple_data_allocation]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &buffer_manager = BufferManager::GetBufferManager(*con.context);
	vector<LogicalType> types {LogicalType::BIGINT, LogicalType::VARCHAR};
	auto layout = make_shared_ptr<TupleDataLayout>();
	layout->Initialize(types, TupleDataValidityType::CAN_HAVE_NULL_VALUES);
	TupleDataCollection first(buffer_manager, layout, MemoryTag::HASH_TABLE);
	TupleDataCollection second(buffer_manager, layout, MemoryTag::HASH_TABLE);
	REQUIRE(first.GetBlockAllocationSize() == 0);

	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), types);
	{
		auto keys = FlatVector::Writer<int64_t>(chunk.data[0], STANDARD_VECTOR_SIZE);
		auto strings = FlatVector::Writer<string_t>(chunk.data[1], STANDARD_VECTOR_SIZE);
		const string payload(4096, 'x');
		for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
			keys.WriteValue(NumericCast<int64_t>(i));
			strings.WriteValue(string_t(payload));
		}
	}
	chunk.CheckCardinality(STANDARD_VECTOR_SIZE);
	first.Append(chunk);
	const auto first_size = first.GetBlockAllocationSize();
	REQUIRE(first_size >= STANDARD_VECTOR_SIZE * (4096 + layout->GetRowWidth()));
	first.Unpin();
	REQUIRE(first.GetBlockAllocationSize() == first_size);

	second.Append(chunk);
	const auto second_size = second.GetBlockAllocationSize();
	first.Combine(second);
	REQUIRE(first.GetBlockAllocationSize() == first_size + second_size);
	REQUIRE(second.GetBlockAllocationSize() == 0);

	// Reusing the source must not release allocations transferred to the destination.
	second.Append(chunk);
	second.Reset();
	REQUIRE(second.GetBlockAllocationSize() == 0);
	REQUIRE(first.GetBlockAllocationSize() == first_size + second_size);

	{
		TupleDataChunkIterator iterator(first, TupleDataPinProperties::DESTROY_AFTER_DONE, true);
		while (!iterator.Done()) {
			iterator.Next();
		}
	}
	REQUIRE(first.GetBlockAllocationSize() == 0);
	first.Reset();
	first.Append(chunk);
	REQUIRE(first.GetBlockAllocationSize() == first_size);
	first.Reset();
	REQUIRE(first.GetBlockAllocationSize() == 0);
}

TEST_CASE("Tuple data heap reclamation crosses chunks without heap references", "[tuple_data_allocation]") {
	struct ReclamationCase {
		string name;
		vector<idx_t> string_lengths;
		idx_t begin;
		idx_t end;
		bool releases_heap;
	};
	vector<ReclamationCase> cases {
	    {"empty boundaries", {0, 1024, 0, 32}, 0, 2, true},
	    {"empty begin", {0, 1024, 32}, 0, 2, true},
	    {"empty end", {1024, 0, 0, 32}, 0, 1, true},
	    {"trailing empty chunks", {0, 1024, 0}, 0, 2, true},
	    {"later incremental range", {32, 0, 1024, 0, 32}, 1, 3, true},
	    {"all-empty range", {0, 0, 32}, 0, 2, false},
	    {"shared heap across a gap", {32, 0, 32, 0}, 0, 2, false},
	};
	vector<idx_t> long_gap(64, 0);
	long_gap.front() = 1024;
	long_gap.back() = 32;
	cases.push_back({"long empty gap", std::move(long_gap), 0, 1, true});

	for (const auto &test_case : cases) {
		DYNAMIC_SECTION(test_case.name) {
			DuckDB db(nullptr);
			Connection con(db);
			auto &buffer_manager = BufferManager::GetBufferManager(*con.context);
			vector<LogicalType> types {LogicalType::VARCHAR};
			auto layout = make_shared_ptr<TupleDataLayout>();
			layout->Initialize(types, TupleDataValidityType::CAN_HAVE_NULL_VALUES);
			TupleDataCollection collection(buffer_manager, layout, MemoryTag::ORDER_BY);
			const auto large_string_size =
			    MaxValue<idx_t>(1024, 4 * buffer_manager.GetBlockSize() / STANDARD_VECTOR_SIZE);
			auto payload_for_chunk = [&](idx_t chunk_idx) {
				const auto length = test_case.string_lengths[chunk_idx];
				return length ? string(length == 1024 ? large_string_size : length,
				                       static_cast<char>('a' + chunk_idx % 26))
				              : "inline";
			};
			TupleDataAppendState append_state;
			collection.InitializeAppend(append_state, TupleDataPinProperties::KEEP_EVERYTHING_PINNED);
			DataChunk chunk;
			chunk.Initialize(Allocator::DefaultAllocator(), types);
			for (idx_t chunk_idx = 0; chunk_idx < test_case.string_lengths.size(); chunk_idx++) {
				chunk.Reset();
				const auto length = test_case.string_lengths[chunk_idx];
				const auto payload = payload_for_chunk(chunk_idx);
				{
					auto writer = FlatVector::Writer<string_t>(chunk.data[0], STANDARD_VECTOR_SIZE);
					for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
						// One small heap value keeps sharing independent of the configured vector size.
						if (!length || (length == 32 && i != 0)) {
							if (i % 2 == 0) {
								writer.WriteNull();
							} else {
								writer.WriteValue(string_t("inline"));
							}
						} else {
							writer.WriteValue(string_t(payload));
						}
					}
				}
				collection.Append(append_state, chunk);
			}
			collection.FinalizePinState(append_state.pin_state);
			REQUIRE(collection.ChunkCount() == test_case.string_lengths.size());
			// Releasing more than all row memory before the end boundary proves that heap memory was released.
			const auto max_row_memory = (test_case.end * STANDARD_VECTOR_SIZE / collection.TuplesPerBlock() + 1) *
			                            buffer_manager.GetBlockAllocSize();
			if (test_case.begin) {
				collection.DestroyChunks(0, test_case.begin);
			}
			const auto before = buffer_manager.GetUsedMemory();
			collection.DestroyChunks(test_case.begin, test_case.end);
			collection.Unpin();
			const auto after = buffer_manager.GetUsedMemory();
			REQUIRE(after <= before);
			if (test_case.releases_heap) {
				REQUIRE(before - after > max_row_memory);
			} else {
				REQUIRE(before - after <= max_row_memory);
			}

			// Fresh pins must still work for later chunks, including a heap shared across an empty gap.
			TupleDataScanState scan_state;
			collection.InitializeScan(scan_state);
			collection.Seek(scan_state, test_case.end);
			idx_t chunk_idx = test_case.end;
			while (collection.Scan(scan_state, chunk)) {
				const auto length = test_case.string_lengths[chunk_idx];
				const auto expected = payload_for_chunk(chunk_idx);
				idx_t row_idx = 0;
				for (auto value : chunk.data[0].Values<string_t>()) {
					const auto inline_row = !length || (length == 32 && row_idx != 0);
					if (inline_row && row_idx % 2 == 0) {
						REQUIRE_FALSE(value.IsValid());
					} else {
						REQUIRE(value.IsValid());
						REQUIRE(value.GetValue().GetString() == (inline_row ? "inline" : expected));
					}
					row_idx++;
				}
				REQUIRE(row_idx == STANDARD_VECTOR_SIZE);
				chunk_idx++;
			}
			REQUIRE(chunk_idx == test_case.string_lengths.size());
		}
	}
}

} // namespace duckdb
