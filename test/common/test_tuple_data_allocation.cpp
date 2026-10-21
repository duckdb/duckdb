#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/common/types/row/tuple_data_collection.hpp"
#include "duckdb/common/types/row/tuple_data_iterator.hpp"
#include "duckdb/common/vector/vector_writer.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

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
