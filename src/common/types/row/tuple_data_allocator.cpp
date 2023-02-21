#include "duckdb/common/types/row/tuple_data_allocator.hpp"

namespace duckdb {

TupleDataSegment::TupleDataSegment(TupleDataSegment &&other) noexcept {
	std::swap(row_block_index, other.row_block_index);
	std::swap(heap_block_index, other.heap_block_index);
	std::swap(count, other.count);
}

TupleDataSegment &TupleDataSegment::operator=(TupleDataSegment &&other) noexcept {
	std::swap(row_block_index, other.row_block_index);
	std::swap(heap_block_index, other.heap_block_index);
	std::swap(count, other.count);
	return *this;
}

TupleDataBlock::TupleDataBlock(TupleDataBlock &&other) noexcept {
	std::swap(handle, other.handle);
	std::swap(capacity, other.capacity);
	std::swap(size, other.size);
}

TupleDataBlock &TupleDataBlock::operator=(TupleDataBlock &&other) noexcept {
	std::swap(handle, other.handle);
	std::swap(capacity, other.capacity);
	std::swap(size, other.size);
	return *this;
}

TupleDataAllocator::TupleDataAllocator(ClientContext &context, const TupleDataLayout &layout)
    : buffer_manager(BufferManager::GetBufferManager(context)), layout(layout) {
}

void TupleDataAllocator::Build(TupleDataAppendState &append_state, idx_t count, vector<TupleDataSegment> &segments) {
	idx_t offset = 0;
	while (offset != count) {
		// Build the next segment
		segments.emplace_back(BuildSegment(append_state, offset, count));
		const auto &segment = segments.back();
		const auto next = segment.count;

		// Now set the pointers where the row data will be written
		const auto base_row_ptr = GetRowPointer(append_state.management_state, segment);
		auto row_locations = FlatVector::GetData<data_ptr_t>(append_state.row_locations);
		for (idx_t i = 0; i < next; i++) {
			row_locations[offset + i] = base_row_ptr + i * layout.GetRowWidth();
		}

		if (!layout.AllConstant()) {
			// Also set the pointers where the heap data will be written (if needed)
			const auto heap_row_sizes = FlatVector::GetData<idx_t>(append_state.heap_locations);
			auto heap_locations = FlatVector::GetData<data_ptr_t>(append_state.heap_locations);
			heap_locations[offset] = GetHeapPointer(append_state.management_state, segment);
			for (idx_t i = offset + 1; i < offset + next; i++) {
				heap_locations[i] = heap_locations[i - 1] + heap_row_sizes[i - 1];
			}
		}

		offset += next;
	}
}

data_ptr_t TupleDataAllocator::GetRowPointer(TupleDataManagementState &state, const TupleDataSegment &segment) {
	PinRowBlock(state, segment.row_block_index);
	return state.row_handles[segment.row_block_index].Ptr() + segment.row_block_offset;
}

data_ptr_t TupleDataAllocator::GetHeapPointer(TupleDataManagementState &state, const TupleDataSegment &segment) {
	PinHeapBlock(state, segment.heap_block_index);
	return state.heap_handles[segment.heap_block_index].Ptr() + segment.heap_block_offset;
}

TupleDataSegment TupleDataAllocator::BuildSegment(TupleDataAppendState &append_state, idx_t offset, idx_t count) {
	lock_guard<mutex> guard(lock);
	// Allocate row block (if needed)
	if (row_blocks.empty() || row_blocks.back().RemainingCapacity() < layout.GetRowWidth()) {
		row_blocks.emplace_back(Storage::BLOCK_SIZE);
		buffer_manager.Allocate(Storage::BLOCK_SIZE, false, &row_blocks.back().handle);
	}
	auto next = MinValue<idx_t>(row_blocks.back().RemainingCapacity(layout.GetRowWidth()), count - offset);

	idx_t heap_block_offset = 0;
	idx_t last_heap_row_size = 0;
	if (!layout.AllConstant()) {
		const auto heap_row_sizes = FlatVector::GetData<idx_t>(append_state.heap_row_sizes);

		// Allocate heap block (if needed)
		if (heap_blocks.empty() || heap_blocks.back().RemainingCapacity() < heap_row_sizes[offset]) {
			const auto size = MaxValue<idx_t>((idx_t)Storage::BLOCK_SIZE, heap_row_sizes[offset]);
			heap_blocks.emplace_back(size);
			buffer_manager.Allocate(size, false, &heap_blocks.back().handle);
		}
		heap_block_offset = heap_blocks.back().size;
		const auto heap_remaining = heap_blocks.back().RemainingCapacity();

		// Determine how many we can read next
		idx_t total_heap_size = 0;
		for (idx_t i = offset; i < count; i++) {
			const auto &heap_row_size = heap_row_sizes[i];
			if (total_heap_size + heap_row_size > heap_remaining) {
				next = i;
				break;
			}
			total_heap_size += heap_row_size;
		}

		// Set the size of the last heap row (all other sizes can be inferred from the pointer difference)
		last_heap_row_size = heap_row_sizes[offset + next - 1];

		// Mark this portion of the heap block as filled
		heap_blocks.back().size += total_heap_size;
	}

	// Mark this portion of the row block as filled
	row_blocks.back().size += next * layout.GetRowWidth();

	D_ASSERT(next != 0);
	return TupleDataSegment(row_blocks.size(), row_blocks.back().size, heap_blocks.size(), heap_block_offset,
	                        last_heap_row_size, next);
}

void TupleDataAllocator::PinRowBlock(TupleDataManagementState &state, const uint32_t row_block_index) {
	if (state.row_handles.find(row_block_index) == state.row_handles.end()) {
		shared_ptr<BlockHandle> handle;
		{
			lock_guard<mutex> guard(lock);
			handle = row_blocks[row_block_index].handle;
		}
		state.row_handles[row_block_index] = buffer_manager.Pin(handle);
	}
}

void TupleDataAllocator::PinHeapBlock(TupleDataManagementState &state, const uint32_t heap_block_index) {
	if (state.heap_handles.find(heap_block_index) == state.heap_handles.end()) {
		shared_ptr<BlockHandle> handle;
		{
			lock_guard<mutex> guard(lock);
			handle = heap_blocks[heap_block_index].handle;
		}
		state.row_handles[heap_block_index] = buffer_manager.Pin(handle);
	}
}

} // namespace duckdb
