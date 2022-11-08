
#include "duckdb/storage/table/column_data.hpp"

#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/storage/checkpoint/write_overflow_strings_to_disk.hpp"
#include "duckdb/storage/table/validity_column_data.hpp"
#include "duckdb/storage/table/standard_column_data.hpp"
#include "duckdb/storage/table/list_column_data.hpp"
#include "duckdb/transaction/transaction.hpp"
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
	return move(global_stats);
}

struct PartialBlockForCheckpoint : PartialBlock {
	struct PartialColumnSegment {
		ColumnData *data;
		ColumnSegment *segment;
		uint32_t offset_in_block;
	};

public:
	PartialBlockForCheckpoint(ColumnData *first_data, ColumnSegment *first_segment, BlockManager &block_manager,
	                          PartialBlockState state)
	    : PartialBlock(state), first_data(first_data), first_segment(first_segment), block_manager(block_manager) {
	}

	~PartialBlockForCheckpoint() override {
		D_ASSERT(IsFlushed() || Exception::UncaughtException());
	}

	// We will copy all subsequent segment data into the memory corresponding
	// to the first segment. Once the block is full (or checkpoint is complete)
	// we'll invoke Flush(), which will cause
	// the block to get written to storage (via BlockManger::ConvertToPersistent),
	// and all segments to have their references updated
	// (via ColumnSegment::ConvertToPersistent)
	ColumnData *first_data;
	ColumnSegment *first_segment;
	BlockManager &block_manager;
	vector<PartialColumnSegment> tail_segments;

public:
	bool IsFlushed() {
		// first_segment is zeroed on Flush
		return !first_segment;
	}

	void Flush() override {
		// At this point, we've already copied all data from tail_segments
		// into the page owned by first_segment. We flush all segment data to
		// disk with the following call.
		first_data->IncrementVersion();
		first_segment->ConvertToPersistent(&block_manager, state.block_id);
		// Now that the page is persistent, update tail_segments to point to the
		// newly persistent block.
		for (auto e : tail_segments) {
			e.data->IncrementVersion();
			e.segment->MarkAsPersistent(first_segment->block, e.offset_in_block);
		}
		first_segment = nullptr;
		tail_segments.clear();
	}

	void Clear() override {
		first_data = nullptr;
		first_segment = nullptr;
		tail_segments.clear();
	}

	void AddSegmentToTail(ColumnData *data, ColumnSegment *segment, uint32_t offset_in_block) {
		tail_segments.push_back({data, segment, offset_in_block});
	}
};

void ColumnCheckpointState::FlushSegment(unique_ptr<ColumnSegment> segment, idx_t segment_size) {
	D_ASSERT(segment_size <= Storage::BLOCK_SIZE);
	auto tuple_count = segment->count.load();
	if (tuple_count == 0) { // LCOV_EXCL_START
		return;
	} // LCOV_EXCL_STOP

	// merge the segment stats into the global stats
	global_stats->Merge(*segment->stats.statistics);

	// get the buffer of the segment and pin it
	auto &db = column_data.GetDatabase();
	auto &buffer_manager = BufferManager::GetBufferManager(db);
	block_id_t block_id = INVALID_BLOCK;
	uint32_t offset_in_block = 0;

	if (!segment->stats.statistics->IsConstant()) {
		// non-constant block
		PartialBlockAllocation allocation = partial_block_manager.GetBlockAllocation(segment_size);
		block_id = allocation.state.block_id;
		offset_in_block = allocation.state.offset_in_block;

		if (allocation.partial_block) {
			// Use an existing block.
			D_ASSERT(offset_in_block > 0);
			auto pstate = (PartialBlockForCheckpoint *)allocation.partial_block.get();
			// pin the source block
			auto old_handle = buffer_manager.Pin(segment->block);
			// pin the target block
			auto new_handle = buffer_manager.Pin(pstate->first_segment->block);
			// memcpy the contents of the old block to the new block
			memcpy(new_handle.Ptr() + offset_in_block, old_handle.Ptr(), segment_size);
			pstate->AddSegmentToTail(&column_data, segment.get(), offset_in_block);
		} else {
			// Create a new block for future reuse.
			if (segment->SegmentSize() != Storage::BLOCK_SIZE) {
				// the segment is smaller than the block size
				// allocate a new block and copy the data over
				D_ASSERT(segment->SegmentSize() < Storage::BLOCK_SIZE);
				segment->Resize(Storage::BLOCK_SIZE);
			}
			D_ASSERT(offset_in_block == 0);
			allocation.partial_block = make_unique<PartialBlockForCheckpoint>(
			    &column_data, segment.get(), *allocation.block_manager, allocation.state);
		}
		// Writer will decide whether to reuse this block.
		partial_block_manager.RegisterPartialBlock(move(allocation));
	} else {
		// constant block: no need to write anything to disk besides the stats
		// set up the compression function to constant
		auto &config = DBConfig::GetConfig(db);
		segment->function =
		    config.GetCompressionFunction(CompressionType::COMPRESSION_CONSTANT, segment->type.InternalType());
		segment->ConvertToPersistent(nullptr, INVALID_BLOCK);
	}

	// construct the data pointer
	DataPointer data_pointer;
	data_pointer.block_pointer.block_id = block_id;
	data_pointer.block_pointer.offset = offset_in_block;
	data_pointer.row_start = row_group.start;
	if (!data_pointers.empty()) {
		auto &last_pointer = data_pointers.back();
		data_pointer.row_start = last_pointer.row_start + last_pointer.tuple_count;
	}
	data_pointer.tuple_count = tuple_count;
	data_pointer.compression_type = segment->function->type;
	data_pointer.statistics = segment->stats.statistics->Copy();

	// append the segment to the new segment tree
	new_tree.AppendSegment(move(segment));
	data_pointers.push_back(move(data_pointer));
}

void ColumnCheckpointState::WriteDataPointers(RowGroupWriter &writer) {
	writer.WriteColumnDataPointers(*this);
}

void ColumnCheckpointState::GetBlockIds(unordered_set<block_id_t> &result) {
	for (auto &pointer : data_pointers) {
		if (pointer.block_pointer.block_id == INVALID_BLOCK) {
			continue;
		}
		result.insert(pointer.block_pointer.block_id);
	}
}

} // namespace duckdb
