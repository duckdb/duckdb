//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/column_data_consumer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/column_data_collection.hpp"

namespace duckdb {

struct ColumnDataConsumerLocalState {
	ColumnDataAllocator *allocator;
	ChunkManagementState current_chunk_state;
};

//! ColumnDataConsumer can scan a ColumnDataCollection, and consume it in the process, i.e., read blocks are deleted
class ColumnDataConsumer {
public:
	enum class ChunkReferenceState : uint8_t {
		//! Ready to scan
		READY,
		//! Scan in progress
		IN_PROGRESS,
		//! Scanning done
		DONE
	};

	struct ChunkReference {
	public:
		ChunkReference(ColumnDataCollectionSegment *segment_p, uint32_t chunk_index_p)
		    : segment(segment_p), chunk_index_in_segment(chunk_index_p), state(ChunkReferenceState::READY) {
		}

		inline uint32_t GetMinimumBlockID() const {
			const auto &block_ids = segment->chunk_data[chunk_index_in_segment].block_ids;
			return *std::min_element(block_ids.begin(), block_ids.end());
		}

		friend bool operator<(const ChunkReference &lhs, const ChunkReference &rhs) {
			// Sort by allocator first
			if (lhs.segment->allocator.get() != rhs.segment->allocator.get()) {
				return lhs.segment->allocator.get() < rhs.segment->allocator.get();
			}
			// Then by minimum block id
			return lhs.GetMinimumBlockID() < rhs.GetMinimumBlockID();
		}

	public:
		ColumnDataCollectionSegment *segment;
		uint32_t chunk_index_in_segment;
		ChunkReferenceState state;
	};

public:
	explicit ColumnDataConsumer(ColumnDataCollection &collection);

public:
	//! Initialize the scan of the ColumnDataCollection
	void InitializeScan();
	//! Assign a chunk index to scan
	bool Assign(idx_t &assigned_chunk_index);
	//! Scan the chunk given the index
	void Scan(ColumnDataConsumerLocalState &state, idx_t chunk_index, DataChunk &chunk);
	//! Indicate that scanning the chunk is done
	void MarkAsDone(idx_t chunk_index);

private:
	mutex lock;
	//! The collection being scanned
	ColumnDataCollection &collection;
	//! The number of chunk references
	idx_t chunk_count;
	//! The chunks (in order) to be scanned
	vector<ChunkReference> chunk_references;
	//! Current index into "chunks"
	idx_t current_chunk_index;
	//! Of all the chunks that are marked IN_PROGRESS, this is the lowest index
	idx_t minimum_chunk_index_in_progress;
};

} // namespace duckdb
