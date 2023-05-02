#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/storage/table/column_checkpoint_state.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/storage/checkpoint/write_overflow_strings_to_disk.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/storage/checkpoint/table_data_writer.hpp"

#include "duckdb/main/config.hpp"

namespace duckdb {

ColumnCheckpointState::ColumnCheckpointState(RowGroup &row_group, ColumnData &column_data,
                                             PartialBlockManager &partial_block_manager)
    : row_group(row_group), column_data(column_data), partial_block_manager(partial_block_manager) {
}

ColumnCheckpointState::~ColumnCheckpointState() {
}

unique_ptr<BaseStatistics> ColumnCheckpointState::GetStatistics() {
	D_ASSERT(global_stats);
	return std::move(global_stats);
}

struct PartialBlockForCheckpoint : PartialBlock {
	struct PartialColumnSegment {
		PartialColumnSegment(ColumnData &data, ColumnSegment &segment, uint32_t offset_in_block) :
			data(data), segment(segment), offset_in_block(offset_in_block) {}

		ColumnData &data;
		ColumnSegment &segment;
		uint32_t offset_in_block;
	};

public:
	PartialBlockForCheckpoint(ColumnData &data, ColumnSegment &segment, BlockManager &block_manager,
	                          PartialBlockState state)
	    : PartialBlock(state), block(segment.block), block_manager(block_manager) {
		AddSegmentToTail(data, segment, 0);
	}

	~PartialBlockForCheckpoint() override {
		D_ASSERT(IsFlushed() || Exception::UncaughtException());
	}

	// We will copy all segment data into the memory of the shared block.
	// Once the block is full (or checkpoint is complete) we'll invoke Flush().
	// This will cause the block to get written to storage (via BlockManger::ConvertToPersistent),
	// and all segments to have their references updated (via ColumnSegment::ConvertToPersistent)
	shared_ptr<BlockHandle> block;
	BlockManager &block_manager;
	vector<PartialColumnSegment> segments;
	bool is_flushed;

private:
	struct UninitializedRegion {
		idx_t start;
		idx_t end;
	};
	vector<UninitializedRegion> uninitialized_regions;

public:
	bool IsFlushed() {
		// segments are cleared on Flush
		return segments.empty();
	}

	void AddUninitializedRegion(idx_t start, idx_t end) override {
		uninitialized_regions.push_back({start, end});
	}

	void Flush(idx_t free_space_left) override {
		if (IsFlushed()) {
			throw InternalException("Flush called on partial block that was already flushed");
		}
		// if we have any free space or uninitialized regions we need to zero-initialize them
		if (free_space_left > 0 || !uninitialized_regions.empty()) {
			auto handle = block_manager.buffer_manager.Pin(block);
			// memset any uninitialized regions
			for (auto &uninitialized : uninitialized_regions) {
				memset(handle.Ptr() + uninitialized.start, 0, uninitialized.end - uninitialized.start);
			}
			// memset any free space at the end of the block to 0 prior to writing to disk
			memset(handle.Ptr() + Storage::BLOCK_SIZE - free_space_left, 0, free_space_left);
		}
		// At this point, we've already copied all data from tail_segments
		// into the page owned by first_segment. We flush all segment data to
		// disk with the following call.
		// persist the first segment to disk and point the remaining segments to the same block
		for(idx_t i = 0; i < segments.size(); i++) {
			auto &segment = segments[i];
			segment.data.IncrementVersion();
			if (i == 0) {
				// the first segment is converted to persistent - this writes the data for ALL segments to disk
				D_ASSERT(segment.offset_in_block == 0);
				segment.segment.ConvertToPersistent(&block_manager, state.block_id);
				// update the block after it has been converted to a persistent segment
				block = segment.segment.block;
			} else {
				// subsequent segments are MARKED as persistent - they don't need to be rewritten
				segment.segment.MarkAsPersistent(block, segment.offset_in_block);
			}
		}
		block.reset();
		segments.clear();
	}

	void Clear() override {
		block.reset();
		segments.clear();
	}

	void AddSegmentToTail(ColumnData &data, ColumnSegment &segment, uint32_t offset_in_block) {
		segments.emplace_back(data, segment, offset_in_block);
	}
};

void ColumnCheckpointState::FlushSegment(unique_ptr<ColumnSegment> segment, idx_t segment_size) {
	D_ASSERT(segment_size <= Storage::BLOCK_SIZE);
	auto tuple_count = segment->count.load();
	if (tuple_count == 0) { // LCOV_EXCL_START
		return;
	} // LCOV_EXCL_STOP

	// merge the segment stats into the global stats
	global_stats->Merge(segment->stats.statistics);

	// get the buffer of the segment and pin it
	auto &db = column_data.GetDatabase();
	auto &buffer_manager = BufferManager::GetBufferManager(db);
	block_id_t block_id = INVALID_BLOCK;
	uint32_t offset_in_block = 0;

	if (!segment->stats.statistics.IsConstant()) {
		// non-constant block
		PartialBlockAllocation allocation = partial_block_manager.GetBlockAllocation(segment_size);
		block_id = allocation.state.block_id;
		offset_in_block = allocation.state.offset_in_block;

		if (allocation.partial_block) {
			// Use an existing block.
			D_ASSERT(offset_in_block > 0);
			auto &pstate = (PartialBlockForCheckpoint &) *allocation.partial_block;
			// pin the source block
			auto old_handle = buffer_manager.Pin(segment->block);
			// pin the target block
			auto new_handle = buffer_manager.Pin(pstate.block);
			// memcpy the contents of the old block to the new block
			memcpy(new_handle.Ptr() + offset_in_block, old_handle.Ptr(), segment_size);
			pstate.AddSegmentToTail(column_data, *segment, offset_in_block);
		} else {
			// Create a new block for future reuse.
			if (segment->SegmentSize() != Storage::BLOCK_SIZE) {
				// the segment is smaller than the block size
				// allocate a new block and copy the data over
				D_ASSERT(segment->SegmentSize() < Storage::BLOCK_SIZE);
				segment->Resize(Storage::BLOCK_SIZE);
			}
			D_ASSERT(offset_in_block == 0);
			allocation.partial_block = make_uniq<PartialBlockForCheckpoint>(
			    column_data, *segment, *allocation.block_manager, allocation.state);
		}
		// Writer will decide whether to reuse this block.
		partial_block_manager.RegisterPartialBlock(std::move(allocation));
	} else {
		// constant block: no need to write anything to disk besides the stats
		// set up the compression function to constant
		auto &config = DBConfig::GetConfig(db);
		segment->function =
		    *config.GetCompressionFunction(CompressionType::COMPRESSION_CONSTANT, segment->type.InternalType());
		segment->ConvertToPersistent(nullptr, INVALID_BLOCK);
	}

	// construct the data pointer
	DataPointer data_pointer(segment->stats.statistics.Copy());
	data_pointer.block_pointer.block_id = block_id;
	data_pointer.block_pointer.offset = offset_in_block;
	data_pointer.row_start = row_group.start;
	if (!data_pointers.empty()) {
		auto &last_pointer = data_pointers.back();
		data_pointer.row_start = last_pointer.row_start + last_pointer.tuple_count;
	}
	data_pointer.tuple_count = tuple_count;
	data_pointer.compression_type = segment->function.get().type;

	// append the segment to the new segment tree
	new_tree.AppendSegment(std::move(segment));
	data_pointers.push_back(std::move(data_pointer));
}

void ColumnCheckpointState::WriteDataPointers(RowGroupWriter &writer) {
	writer.WriteColumnDataPointers(*this);
}

} // namespace duckdb
