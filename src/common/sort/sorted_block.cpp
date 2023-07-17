#include "duckdb/common/sort/sorted_block.hpp"

#include "duckdb/common/constants.hpp"
#include "duckdb/common/row_operations/row_operations.hpp"
#include "duckdb/common/sort/sort.hpp"
#include "duckdb/common/types/row/row_data_collection.hpp"

#include <numeric>

namespace duckdb {

SortedData::SortedData(SortedDataType type, const RowLayout &layout, BufferManager &buffer_manager,
                       GlobalSortState &state)
    : type(type), layout(layout), swizzled(state.external), buffer_manager(buffer_manager), state(state) {
}

idx_t SortedData::Count() {
	idx_t count = std::accumulate(data_blocks.begin(), data_blocks.end(), (idx_t)0,
	                              [](idx_t a, const unique_ptr<RowDataBlock> &b) { return a + b->count; });
	if (!layout.AllConstant() && state.external) {
		D_ASSERT(count == std::accumulate(heap_blocks.begin(), heap_blocks.end(), (idx_t)0,
		                                  [](idx_t a, const unique_ptr<RowDataBlock> &b) { return a + b->count; }));
	}
	return count;
}

void SortedData::CreateBlock() {
	auto capacity =
	    MaxValue(((idx_t)Storage::BLOCK_SIZE + layout.GetRowWidth() - 1) / layout.GetRowWidth(), state.block_capacity);
	data_blocks.push_back(make_uniq<RowDataBlock>(buffer_manager, capacity, layout.GetRowWidth()));
	if (!layout.AllConstant() && state.external) {
		heap_blocks.push_back(make_uniq<RowDataBlock>(buffer_manager, (idx_t)Storage::BLOCK_SIZE, 1));
		D_ASSERT(data_blocks.size() == heap_blocks.size());
	}
}

unique_ptr<SortedData> SortedData::CreateSlice(idx_t start_block_index, idx_t end_block_index, idx_t end_entry_index) {
	// Add the corresponding blocks to the result
	auto result = make_uniq<SortedData>(type, layout, buffer_manager, state);
	for (idx_t i = start_block_index; i <= end_block_index; i++) {
		result->data_blocks.push_back(data_blocks[i]->Copy());
		if (!layout.AllConstant() && state.external) {
			result->heap_blocks.push_back(heap_blocks[i]->Copy());
		}
	}
	// All of the blocks that come before block with idx = start_block_idx can be reset (other references exist)
	for (idx_t i = 0; i < start_block_index; i++) {
		data_blocks[i]->block = nullptr;
		if (!layout.AllConstant() && state.external) {
			heap_blocks[i]->block = nullptr;
		}
	}
	// Use start and end entry indices to set the boundaries
	D_ASSERT(end_entry_index <= result->data_blocks.back()->count);
	result->data_blocks.back()->count = end_entry_index;
	if (!layout.AllConstant() && state.external) {
		result->heap_blocks.back()->count = end_entry_index;
	}
	return result;
}

void SortedData::Unswizzle() {
	if (layout.AllConstant() || !swizzled) {
		return;
	}
	for (idx_t i = 0; i < data_blocks.size(); i++) {
		auto &data_block = data_blocks[i];
		auto &heap_block = heap_blocks[i];
		D_ASSERT(data_block->block->IsSwizzled());
		auto data_handle_p = buffer_manager.Pin(data_block->block);
		auto heap_handle_p = buffer_manager.Pin(heap_block->block);
		RowOperations::UnswizzlePointers(layout, data_handle_p.Ptr(), heap_handle_p.Ptr(), data_block->count);
		state.heap_blocks.push_back(std::move(heap_block));
		state.pinned_blocks.push_back(std::move(heap_handle_p));
	}
	swizzled = false;
	heap_blocks.clear();
}

SortedBlock::SortedBlock(BufferManager &buffer_manager, GlobalSortState &state)
    : buffer_manager(buffer_manager), state(state), sort_layout(state.sort_layout),
      payload_layout(state.payload_layout) {
	blob_sorting_data = make_uniq<SortedData>(SortedDataType::BLOB, sort_layout.blob_layout, buffer_manager, state);
	payload_data = make_uniq<SortedData>(SortedDataType::PAYLOAD, payload_layout, buffer_manager, state);
}

idx_t SortedBlock::Count() const {
	idx_t count = std::accumulate(radix_sorting_data.begin(), radix_sorting_data.end(), (idx_t)0,
	                              [](idx_t a, const unique_ptr<RowDataBlock> &b) { return a + b->count; });
	if (!sort_layout.all_constant) {
		D_ASSERT(count == blob_sorting_data->Count());
	}
	D_ASSERT(count == payload_data->Count());
	return count;
}

void SortedBlock::InitializeWrite() {
	CreateBlock();
	if (!sort_layout.all_constant) {
		blob_sorting_data->CreateBlock();
	}
	payload_data->CreateBlock();
}

void SortedBlock::CreateBlock() {
	auto capacity = MaxValue(((idx_t)Storage::BLOCK_SIZE + sort_layout.entry_size - 1) / sort_layout.entry_size,
	                         state.block_capacity);
	radix_sorting_data.push_back(make_uniq<RowDataBlock>(buffer_manager, capacity, sort_layout.entry_size));
}

void SortedBlock::AppendSortedBlocks(vector<unique_ptr<SortedBlock>> &sorted_blocks) {
	D_ASSERT(Count() == 0);
	for (auto &sb : sorted_blocks) {
		for (auto &radix_block : sb->radix_sorting_data) {
			radix_sorting_data.push_back(std::move(radix_block));
		}
		if (!sort_layout.all_constant) {
			for (auto &blob_block : sb->blob_sorting_data->data_blocks) {
				blob_sorting_data->data_blocks.push_back(std::move(blob_block));
			}
			for (auto &heap_block : sb->blob_sorting_data->heap_blocks) {
				blob_sorting_data->heap_blocks.push_back(std::move(heap_block));
			}
		}
		for (auto &payload_data_block : sb->payload_data->data_blocks) {
			payload_data->data_blocks.push_back(std::move(payload_data_block));
		}
		if (!payload_data->layout.AllConstant()) {
			for (auto &payload_heap_block : sb->payload_data->heap_blocks) {
				payload_data->heap_blocks.push_back(std::move(payload_heap_block));
			}
		}
	}
}

void SortedBlock::GlobalToLocalIndex(const idx_t &global_idx, idx_t &local_block_index, idx_t &local_entry_index) {
	if (global_idx == Count()) {
		local_block_index = radix_sorting_data.size() - 1;
		local_entry_index = radix_sorting_data.back()->count;
		return;
	}
	D_ASSERT(global_idx < Count());
	local_entry_index = global_idx;
	for (local_block_index = 0; local_block_index < radix_sorting_data.size(); local_block_index++) {
		const idx_t &block_count = radix_sorting_data[local_block_index]->count;
		if (local_entry_index >= block_count) {
			local_entry_index -= block_count;
		} else {
			break;
		}
	}
	D_ASSERT(local_entry_index < radix_sorting_data[local_block_index]->count);
}

unique_ptr<SortedBlock> SortedBlock::CreateSlice(const idx_t start, const idx_t end, idx_t &entry_idx) {
	// Identify blocks/entry indices of this slice
	idx_t start_block_index;
	idx_t start_entry_index;
	GlobalToLocalIndex(start, start_block_index, start_entry_index);
	idx_t end_block_index;
	idx_t end_entry_index;
	GlobalToLocalIndex(end, end_block_index, end_entry_index);
	// Add the corresponding blocks to the result
	auto result = make_uniq<SortedBlock>(buffer_manager, state);
	for (idx_t i = start_block_index; i <= end_block_index; i++) {
		result->radix_sorting_data.push_back(radix_sorting_data[i]->Copy());
	}
	// Reset all blocks that come before block with idx = start_block_idx (slice holds new reference)
	for (idx_t i = 0; i < start_block_index; i++) {
		radix_sorting_data[i]->block = nullptr;
	}
	// Use start and end entry indices to set the boundaries
	entry_idx = start_entry_index;
	D_ASSERT(end_entry_index <= result->radix_sorting_data.back()->count);
	result->radix_sorting_data.back()->count = end_entry_index;
	// Same for the var size sorting data
	if (!sort_layout.all_constant) {
		result->blob_sorting_data = blob_sorting_data->CreateSlice(start_block_index, end_block_index, end_entry_index);
	}
	// And the payload data
	result->payload_data = payload_data->CreateSlice(start_block_index, end_block_index, end_entry_index);
	return result;
}

idx_t SortedBlock::HeapSize() const {
	idx_t result = 0;
	if (!sort_layout.all_constant) {
		for (auto &block : blob_sorting_data->heap_blocks) {
			result += block->capacity;
		}
	}
	if (!payload_layout.AllConstant()) {
		for (auto &block : payload_data->heap_blocks) {
			result += block->capacity;
		}
	}
	return result;
}

idx_t SortedBlock::SizeInBytes() const {
	idx_t bytes = 0;
	for (idx_t i = 0; i < radix_sorting_data.size(); i++) {
		bytes += radix_sorting_data[i]->capacity * sort_layout.entry_size;
		if (!sort_layout.all_constant) {
			bytes += blob_sorting_data->data_blocks[i]->capacity * sort_layout.blob_layout.GetRowWidth();
			bytes += blob_sorting_data->heap_blocks[i]->capacity;
		}
		bytes += payload_data->data_blocks[i]->capacity * payload_layout.GetRowWidth();
		if (!payload_layout.AllConstant()) {
			bytes += payload_data->heap_blocks[i]->capacity;
		}
	}
	return bytes;
}

SBScanState::SBScanState(BufferManager &buffer_manager, GlobalSortState &state)
    : buffer_manager(buffer_manager), sort_layout(state.sort_layout), state(state), block_idx(0), entry_idx(0) {
}

void SBScanState::PinRadix(idx_t block_idx_to) {
	auto &radix_sorting_data = sb->radix_sorting_data;
	D_ASSERT(block_idx_to < radix_sorting_data.size());
	auto &block = radix_sorting_data[block_idx_to];
	if (!radix_handle.IsValid() || radix_handle.GetBlockHandle() != block->block) {
		radix_handle = buffer_manager.Pin(block->block);
	}
}

void SBScanState::PinData(SortedData &sd) {
	D_ASSERT(block_idx < sd.data_blocks.size());
	auto &data_handle = sd.type == SortedDataType::BLOB ? blob_sorting_data_handle : payload_data_handle;
	auto &heap_handle = sd.type == SortedDataType::BLOB ? blob_sorting_heap_handle : payload_heap_handle;

	auto &data_block = sd.data_blocks[block_idx];
	if (!data_handle.IsValid() || data_handle.GetBlockHandle() != data_block->block) {
		data_handle = buffer_manager.Pin(data_block->block);
	}
	if (sd.layout.AllConstant() || !state.external) {
		return;
	}
	auto &heap_block = sd.heap_blocks[block_idx];
	if (!heap_handle.IsValid() || heap_handle.GetBlockHandle() != heap_block->block) {
		heap_handle = buffer_manager.Pin(heap_block->block);
	}
}

data_ptr_t SBScanState::RadixPtr() const {
	return radix_handle.Ptr() + entry_idx * sort_layout.entry_size;
}

data_ptr_t SBScanState::DataPtr(SortedData &sd) const {
	auto &data_handle = sd.type == SortedDataType::BLOB ? blob_sorting_data_handle : payload_data_handle;
	D_ASSERT(sd.data_blocks[block_idx]->block->Readers() != 0 &&
	         data_handle.GetBlockHandle() == sd.data_blocks[block_idx]->block);
	return data_handle.Ptr() + entry_idx * sd.layout.GetRowWidth();
}

data_ptr_t SBScanState::HeapPtr(SortedData &sd) const {
	return BaseHeapPtr(sd) + Load<idx_t>(DataPtr(sd) + sd.layout.GetHeapOffset());
}

data_ptr_t SBScanState::BaseHeapPtr(SortedData &sd) const {
	auto &heap_handle = sd.type == SortedDataType::BLOB ? blob_sorting_heap_handle : payload_heap_handle;
	D_ASSERT(!sd.layout.AllConstant() && state.external);
	D_ASSERT(sd.heap_blocks[block_idx]->block->Readers() != 0 &&
	         heap_handle.GetBlockHandle() == sd.heap_blocks[block_idx]->block);
	return heap_handle.Ptr();
}

idx_t SBScanState::Remaining() const {
	const auto &blocks = sb->radix_sorting_data;
	idx_t remaining = 0;
	if (block_idx < blocks.size()) {
		remaining += blocks[block_idx]->count - entry_idx;
		for (idx_t i = block_idx + 1; i < blocks.size(); i++) {
			remaining += blocks[i]->count;
		}
	}
	return remaining;
}

void SBScanState::SetIndices(idx_t block_idx_to, idx_t entry_idx_to) {
	block_idx = block_idx_to;
	entry_idx = entry_idx_to;
}

PayloadScanner::PayloadScanner(SortedData &sorted_data, GlobalSortState &global_sort_state, bool flush_p) {
	auto count = sorted_data.Count();
	auto &layout = sorted_data.layout;

	// Create collections to put the data into so we can use RowDataCollectionScanner
	rows = make_uniq<RowDataCollection>(global_sort_state.buffer_manager, (idx_t)Storage::BLOCK_SIZE, 1);
	rows->count = count;

	heap = make_uniq<RowDataCollection>(global_sort_state.buffer_manager, (idx_t)Storage::BLOCK_SIZE, 1);
	if (!sorted_data.layout.AllConstant()) {
		heap->count = count;
	}

	if (flush_p) {
		// If we are flushing, we can just move the data
		rows->blocks = std::move(sorted_data.data_blocks);
		if (!layout.AllConstant()) {
			heap->blocks = std::move(sorted_data.heap_blocks);
		}
	} else {
		// Not flushing, create references to the blocks
		for (auto &block : sorted_data.data_blocks) {
			rows->blocks.emplace_back(block->Copy());
		}
		if (!layout.AllConstant()) {
			for (auto &block : sorted_data.heap_blocks) {
				heap->blocks.emplace_back(block->Copy());
			}
		}
	}

	scanner = make_uniq<RowDataCollectionScanner>(*rows, *heap, layout, global_sort_state.external, flush_p);
}

PayloadScanner::PayloadScanner(GlobalSortState &global_sort_state, bool flush_p)
    : PayloadScanner(*global_sort_state.sorted_blocks[0]->payload_data, global_sort_state, flush_p) {
}

PayloadScanner::PayloadScanner(GlobalSortState &global_sort_state, idx_t block_idx, bool flush_p) {
	auto &sorted_data = *global_sort_state.sorted_blocks[0]->payload_data;
	auto count = sorted_data.data_blocks[block_idx]->count;
	auto &layout = sorted_data.layout;

	// Create collections to put the data into so we can use RowDataCollectionScanner
	rows = make_uniq<RowDataCollection>(global_sort_state.buffer_manager, (idx_t)Storage::BLOCK_SIZE, 1);
	if (flush_p) {
		rows->blocks.emplace_back(std::move(sorted_data.data_blocks[block_idx]));
	} else {
		rows->blocks.emplace_back(sorted_data.data_blocks[block_idx]->Copy());
	}
	rows->count = count;

	heap = make_uniq<RowDataCollection>(global_sort_state.buffer_manager, (idx_t)Storage::BLOCK_SIZE, 1);
	if (!sorted_data.layout.AllConstant() && sorted_data.swizzled) {
		if (flush_p) {
			heap->blocks.emplace_back(std::move(sorted_data.heap_blocks[block_idx]));
		} else {
			heap->blocks.emplace_back(sorted_data.heap_blocks[block_idx]->Copy());
		}
		heap->count = count;
	}

	scanner = make_uniq<RowDataCollectionScanner>(*rows, *heap, layout, global_sort_state.external, flush_p);
}

void PayloadScanner::Scan(DataChunk &chunk) {
	scanner->Scan(chunk);
}

int SBIterator::ComparisonValue(ExpressionType comparison) {
	switch (comparison) {
	case ExpressionType::COMPARE_LESSTHAN:
	case ExpressionType::COMPARE_GREATERTHAN:
		return -1;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return 0;
	default:
		throw InternalException("Unimplemented comparison type for IEJoin!");
	}
}

static idx_t GetBlockCountWithEmptyCheck(const GlobalSortState &gss) {
	D_ASSERT(!gss.sorted_blocks.empty());
	return gss.sorted_blocks[0]->radix_sorting_data.size();
}

SBIterator::SBIterator(GlobalSortState &gss, ExpressionType comparison, idx_t entry_idx_p)
    : sort_layout(gss.sort_layout), block_count(GetBlockCountWithEmptyCheck(gss)), block_capacity(gss.block_capacity),
      cmp_size(sort_layout.comparison_size), entry_size(sort_layout.entry_size), all_constant(sort_layout.all_constant),
      external(gss.external), cmp(ComparisonValue(comparison)), scan(gss.buffer_manager, gss), block_ptr(nullptr),
      entry_ptr(nullptr) {

	scan.sb = gss.sorted_blocks[0].get();
	scan.block_idx = block_count;
	SetIndex(entry_idx_p);
}

} // namespace duckdb
