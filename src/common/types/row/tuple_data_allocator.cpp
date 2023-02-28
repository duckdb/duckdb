#include "duckdb/common/types/row/tuple_data_allocator.hpp"

#include "duckdb/common/types/row/tuple_data_segment.hpp"
#include "duckdb/common/types/row/tuple_data_states.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

TupleDataBlock::TupleDataBlock(BufferManager &buffer_manager, idx_t capacity_p) : capacity(capacity_p) {
	buffer_manager.Allocate(capacity, false, &handle);
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

Allocator &TupleDataAllocator::GetAllocator() {
	return buffer_manager.GetBufferAllocator();
}

const TupleDataLayout &TupleDataAllocator::GetLayout() {
	return layout;
}

static void ReleaseHandles(unordered_map<uint32_t, BufferHandle> &handles, const unordered_set<uint32_t> &block_ids,
                           vector<BufferHandle> *pinned_handles) {
	bool found_handle;
	do {
		found_handle = false;
		for (auto it = handles.begin(); it != handles.end(); it++) {
			if (block_ids.find(it->first) != block_ids.end()) {
				// still required: do not release
				continue;
			}
			if (pinned_handles) {
				pinned_handles->emplace_back(std::move(handles.erase(it)->second));
			} else {
				handles.erase(it);
			}
			found_handle = true;
			break;
		}
	} while (found_handle);
}

static void ReleaseHandles(TupleDataManagementState &state, const TupleDataChunk &chunk, bool all_constant,
                           vector<BufferHandle> *pinned_handles) {
	ReleaseHandles(state.row_handles, chunk.row_block_ids, pinned_handles);
	if (!all_constant) {
		ReleaseHandles(state.heap_handles, chunk.heap_block_ids, pinned_handles);
	}
}

void TupleDataAllocator::Build(TupleDataAppendState &append_state, idx_t count, TupleDataSegment &segment) {
	auto &chunks = segment.chunks;
	if (!chunks.empty()) {
		vector<BufferHandle> *pinned_handles;
		if (append_state.properties == TupleDataAppendProperties::KEEP_EVERYTHING_PINNED) {
			// Keep handles pinned
			pinned_handles = &segment.pinned_handles;
		} else if (append_state.properties == TupleDataAppendProperties::UNPIN_AFTER_DONE) {
			// Release any handles that are no longer required
			pinned_handles = nullptr;
		} else {
			throw InternalException("Encountered TupleDataAppendProperties::INVALID");
		}
		ReleaseHandles(append_state.chunk_state, chunks.back(), layout.AllConstant(), pinned_handles);
	}

	auto row_locations = FlatVector::GetData<data_ptr_t>(append_state.chunk_state.row_locations);
	const auto heap_sizes = FlatVector::GetData<idx_t>(append_state.chunk_state.heap_sizes);
	auto heap_locations = FlatVector::GetData<data_ptr_t>(append_state.chunk_state.heap_locations);

	idx_t offset = 0;
	while (offset != count) {
		if (chunks.empty() || chunks.back().count == STANDARD_VECTOR_SIZE) {
			chunks.emplace_back();
		}
		auto &chunk = chunks.back();

		// Build the next part TODO: maybe we can extend parts so we have less metadata
		BuildChunkPart(append_state, offset, count, chunk);
		auto &chunk_part = chunk.parts.back();
		const auto next = chunk.count;
		segment.count += next;

		// Now set the pointers where the row data will be written
		const auto base_row_ptr = GetRowPointer(append_state.chunk_state, chunk_part);
		for (idx_t i = 0; i < next; i++) {
			row_locations[offset + i] = base_row_ptr + i * layout.GetRowWidth();
		}

		if (!layout.AllConstant()) {
			// Also set the pointers where the heap data will be written (if needed)
			const auto base_heap_ptr = GetHeapPointer(append_state.chunk_state, chunk_part);
			heap_locations[offset] = base_heap_ptr;
			for (idx_t i = offset + 1; i < offset + next; i++) {
				heap_locations[i] = heap_locations[i - 1] + heap_sizes[i - 1];
			}

			// Set the offset from the base heap pointer in each row
			for (idx_t i = offset; i < offset + next; i++) {
				Store<uint32_t>(heap_locations[i] - base_heap_ptr, row_locations[i] + layout.GetHeapOffset());
			}
		}

		offset += next;
	}
	segment.Verify();
}

void TupleDataAllocator::BuildChunkPart(TupleDataAppendState &append_state, idx_t offset, idx_t count,
                                        TupleDataChunk &chunk) {
	lock_guard<mutex> guard(lock);
	// Allocate row block (if needed)
	if (row_blocks.empty() || row_blocks.back().RemainingCapacity() < layout.GetRowWidth()) {
		row_blocks.emplace_back(buffer_manager, (idx_t)Storage::BLOCK_SIZE);
	}
	uint32_t row_block_index = row_blocks.size();
	uint32_t row_block_offset = row_blocks.back().size;

	auto next = MinValue<idx_t>(row_blocks.back().RemainingCapacity(layout.GetRowWidth()), count - offset);

	uint32_t heap_block_index = 0;
	uint32_t heap_block_offset = 0;
	data_ptr_t base_heap_ptr = nullptr;
	uint32_t last_heap_size = 0;
	if (!layout.AllConstant()) {
		const auto heap_sizes = FlatVector::GetData<idx_t>(append_state.chunk_state.heap_sizes);

		// Allocate heap block (if needed)
		if (heap_blocks.empty() || heap_blocks.back().RemainingCapacity() < heap_sizes[offset]) {
			const auto size = MaxValue<idx_t>((idx_t)Storage::BLOCK_SIZE, heap_sizes[offset]);
			heap_blocks.emplace_back(buffer_manager, size);
		}
		heap_block_index = heap_blocks.size();
		heap_block_offset = heap_blocks.back().size;
		PinHeapBlock(append_state.chunk_state, heap_block_index);
		base_heap_ptr = append_state.chunk_state.heap_handles[heap_block_index].Ptr() + heap_block_offset;
		const auto heap_remaining = heap_blocks.back().RemainingCapacity();

		// Determine how many we can read next
		uint32_t total_heap_size = 0;
		for (idx_t i = offset; i < count; i++) {
			const auto &heap_size = heap_sizes[i];
			if (total_heap_size + heap_size > heap_remaining) {
				next = i;
				break;
			}
			total_heap_size += heap_size;
		}

		// Set the size of the last heap row (all other sizes can be inferred from the pointer difference)
		last_heap_size = heap_sizes[offset + next - 1];

		// Mark this portion of the heap block as filled
		heap_blocks.back().size += total_heap_size;
	}
	D_ASSERT(next != 0);

	// Mark this portion of the row block as filled
	row_blocks.back().size += next * layout.GetRowWidth();

	// Create the chunk part
	chunk.AddPart(TupleDataChunkPart(row_block_index, row_block_offset, heap_block_index, heap_block_offset,
	                                 base_heap_ptr, last_heap_size, next));
}

static void RecomputeHeapPointers(const data_ptr_t old_base_heap_ptr, const data_ptr_t new_base_heap_ptr,
                                  const data_ptr_t row_locations[], const idx_t offset, const idx_t count,
                                  const TupleDataLayout &layout, const idx_t base_col_offset) {
	for (idx_t col_idx = 0; col_idx < layout.ColumnCount(); col_idx++) {
		const auto &col_offset = base_col_offset + layout.GetOffsets()[col_idx];
		switch (layout.GetTypes()[col_idx].InternalType()) {
		case PhysicalType::VARCHAR: {
			for (idx_t i = 0; i < count; i++) {
				const auto &string_location = row_locations[offset + i] + col_offset;
				if (Load<uint32_t>(string_location) > string_t::INLINE_LENGTH) {
					const auto diff = Load<data_ptr_t>(string_location + string_t::HEADER_SIZE) - old_base_heap_ptr;
					Store<data_ptr_t>(new_base_heap_ptr + diff, string_location + string_t::HEADER_SIZE);
				}
			}
			break;
		}
		case PhysicalType::LIST: {
			for (idx_t i = 0; i < count; i++) {
				const auto &pointer_location = row_locations[offset + i] + col_offset;
				const auto diff = Load<data_ptr_t>(pointer_location) - old_base_heap_ptr;
				Store<data_ptr_t>(new_base_heap_ptr + diff, pointer_location);
			}
			break;
		}
		case PhysicalType::STRUCT: {
			D_ASSERT(layout.GetStructLayouts().find(col_idx) != layout.GetStructLayouts().end());
			const auto &struct_layout = layout.GetStructLayouts().find(col_idx)->second;
			if (!struct_layout.AllConstant()) {
				RecomputeHeapPointers(old_base_heap_ptr, new_base_heap_ptr, row_locations, offset, count, struct_layout,
				                      col_offset);
			}
			break;
		}
		default:
			continue;
		}
	}
}

void TupleDataAllocator::InitializeChunkState(TupleDataManagementState &state, TupleDataChunk &chunk) {
	// Release any handles that are no longer required
	ReleaseHandles(state, chunk, layout.AllConstant(), nullptr);

	idx_t offset = 0;
	auto row_locations = FlatVector::GetData<data_ptr_t>(state.row_locations);
	for (auto &part : chunk.parts) {
		// Set up row locations for the scan
		const auto base_row_ptr = GetRowPointer(state, part);
		for (idx_t i = 0; i < part.count; i++) {
			row_locations[offset + i] = base_row_ptr + i * layout.GetRowWidth();
		}
		if (!layout.AllConstant()) {
			const auto old_base_heap_ptr = part.base_heap_ptr;
			const auto new_base_heap_ptr = GetHeapPointer(state, part);
			if (old_base_heap_ptr != new_base_heap_ptr) {
				// Heap block has changed - re-compute the pointers within each row
				RecomputeHeapPointers(old_base_heap_ptr, new_base_heap_ptr, row_locations, offset, part.count, layout,
				                      0);
				part.base_heap_ptr = new_base_heap_ptr;
			}
		}
		offset += part.count;
	}
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

data_ptr_t TupleDataAllocator::GetRowPointer(TupleDataManagementState &state, const TupleDataChunkPart &part) {
	PinRowBlock(state, part.row_block_index);
	return state.row_handles[part.row_block_index].Ptr() + part.row_block_offset;
}

data_ptr_t TupleDataAllocator::GetHeapPointer(TupleDataManagementState &state, const TupleDataChunkPart &part) {
	PinHeapBlock(state, part.heap_block_index);
	return state.heap_handles[part.heap_block_index].Ptr() + part.heap_block_offset;
}

} // namespace duckdb
