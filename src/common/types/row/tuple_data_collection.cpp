#include "duckdb/common/types/row/tuple_data_collection.hpp"

#include "duckdb/common/types/row/tuple_data_states.hpp"

namespace duckdb {

TupleDataCollection::TupleDataCollection(BufferManager &buffer_manager, vector<LogicalType> types,
                                         vector<AggregateObject> aggregates, bool align)
    : buffer_manager(buffer_manager), count(0) {
	layout.Initialize(types, aggregates, align);
}

TupleDataCollection::TupleDataCollection(BufferManager &buffer_manager, vector<LogicalType> types, bool align)
    : TupleDataCollection(buffer_manager, types, {}, align) {
}

TupleDataCollection::TupleDataCollection(BufferManager &buffer_manager, vector<AggregateObject> aggregates, bool align)
    : TupleDataCollection(buffer_manager, {}, aggregates, align) {
}

void TupleDataCollection::InitializeAppend(TupleDataAppendState &state) {
	state.vector_data.resize(layout.ColumnCount());
	AllocateRowBlock(state);
	if (!layout.AllConstant()) {
		AllocateHeapBlock(state);
	}
}

void TupleDataCollection::Append(TupleDataAppendState &state, DataChunk &chunk, bool unify) {
	D_ASSERT(chunk.ColumnCount() == layout.ColumnCount());
	if (unify) {
		auto chunk_count = chunk.size();
		for (idx_t i = 0; i < layout.ColumnCount(); i++) {
			chunk.data[i].ToUnifiedFormat(chunk_count, state.vector_data[i]);
		}
	}
	D_ASSERT(state.vector_data.size() == layout.ColumnCount());
}

uint32_t TupleDataCollection::AllocateRowBlock(TupleDataAppendState &state) {
	auto &handles = state.management_state.row_handles;
	uint32_t row_block_id = row_blocks.size();
	D_ASSERT(handles.find(row_block_id) == handles.end());

	shared_ptr<BlockHandle> block_handle;
	auto buffer_handle = buffer_manager.Allocate(Storage::BLOCK_SIZE, false, &block_handle);
	row_blocks.emplace_back(move(block_handle), layout);
	handles[row_block_id] = move(buffer_handle);

	return row_block_id;
}

uint32_t TupleDataCollection::AllocateHeapBlock(TupleDataAppendState &state, uint32_t capacity) {
	D_ASSERT(!layout.AllConstant());
	auto &handles = state.management_state.heap_handles;
	uint32_t heap_block_id = heap_blocks.size();
	D_ASSERT(handles.find(heap_block_id) == handles.end());

	shared_ptr<BlockHandle> block_handle;
	auto buffer_handle = buffer_manager.Allocate(capacity, false, &block_handle);
	heap_blocks.emplace_back(move(block_handle), capacity);
	handles[heap_block_id] = move(buffer_handle);

	return heap_block_id;
}

void TupleDataCollection::PinRowBlock(TupleDataManagementState &state, uint32_t row_block_id) {
	D_ASSERT(row_block_id < row_blocks.size());
	if (state.row_handles.find(row_block_id) == state.row_handles.end()) {
		state.row_handles[row_block_id] = buffer_manager.Pin(row_blocks[row_block_id].handle);
	}
}

void TupleDataCollection::PinHeapBlock(TupleDataManagementState &state, uint32_t heap_block_id) {
	D_ASSERT(heap_block_id < heap_blocks.size());
	if (state.heap_handles.find(heap_block_id) == state.heap_handles.end()) {
		state.heap_handles[heap_block_id] = buffer_manager.Pin(heap_blocks[heap_block_id].handle);
	}
}

data_ptr_t TupleDataCollection::GetRowDataPointer(TupleDataManagementState &state, uint32_t row_block_id,
                                                  uint32_t offset) {
	D_ASSERT(state.row_handles.find(row_block_id) != state.row_handles.end());
	return state.row_handles[row_block_id].Ptr() + offset * layout.GetRowWidth();
}

data_ptr_t TupleDataCollection::GetHeapDataPointer(TupleDataManagementState &state, uint32_t heap_block_id,
                                                   uint32_t offset) {
	D_ASSERT(state.heap_handles.find(heap_block_id) != state.heap_handles.end());
	return state.heap_handles[heap_block_id].Ptr() + offset;
}

void TupleDataCollection::Build(TupleDataAppendState &state, DataChunk &chunk) {
	// Compute entry sizes of heap data
	if (!layout.AllConstant()) {
		ComputeEntrySizes(state, chunk);
	}

	// Build out blocks for row data
	idx_t offset = 0;
	idx_t append_count = chunk.size();
	while (offset != append_count) {
		offset += AppendToBlock(state, offset, append_count);
	}
}

idx_t TupleDataCollection::AppendToBlock(TupleDataAppendState &state, idx_t offset, idx_t append_count) {
	uint32_t row_block_id = row_blocks.empty() || row_blocks.back().RemainingCapacity() == 0 ? AllocateRowBlock(state)
	                                                                                         : row_blocks.size() - 1;
	auto &row_block = row_blocks[row_block_id];
	auto remaining_capacity = row_block.RemainingCapacity();
	D_ASSERT(remaining_capacity != 0);

	idx_t append_remaining = append_count - offset;
	if (!layout.AllConstant()) {
		auto entry_sizes = FlatVector::GetData<idx_t>(state.heap_row_sizes);
		
	}

	idx_t next = MinValue<idx_t>(remaining_capacity, append_remaining);
	row_block.count += next;

	PinRowBlock(state.management_state, row_block_id);
	auto row_ptr = GetRowDataPointer(state.management_state, row_block_id, row_block.count);
	if (!layout.AllConstant()) {

	}

	auto row_locations = FlatVector::GetData<data_ptr_t>(state.row_locations);
	for (idx_t i = 0; i < append_count; i++) {
		row_locations[offset + i] = row_ptr;
		row_ptr += layout.GetRowWidth();
	}

	return append_count;
}

void TupleDataCollection::ComputeEntrySizes(TupleDataAppendState &state, DataChunk &chunk) {
	auto entry_sizes = FlatVector::GetData<idx_t>(state.heap_row_sizes);
	std::fill_n(entry_sizes, chunk.size(), sizeof(uint32_t));
	for (idx_t i = 0; i < layout.ColumnCount(); i++) {
		auto type = layout.GetTypes()[i].InternalType();
		if (TypeIsConstantSize(type)) {
			continue;
		}

		switch (type) {
		case PhysicalType::VARCHAR:
			RowOperations::ComputeStringEntrySizes(state.vector_data[i], entry_sizes,
			                                       *FlatVector::IncrementalSelectionVector(), chunk.size());
			break;
		case PhysicalType::LIST:
		case PhysicalType::MAP:
		case PhysicalType::STRUCT:
			RowOperations::ComputeEntrySizes(chunk.data[i], state.vector_data[i], entry_sizes, chunk.size(),
			                                 chunk.size(), *FlatVector::IncrementalSelectionVector());
			break;
		default:
			throw InternalException("Unsupported type for RowOperations::ComputeEntrySizes");
		}
	}
}

} // namespace duckdb
