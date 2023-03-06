#include "duckdb/common/types/row/tuple_data_allocator.hpp"

#include "duckdb/common/types/row/tuple_data_segment.hpp"
#include "duckdb/common/types/row/tuple_data_states.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

TupleDataBlock::TupleDataBlock(BufferManager &buffer_manager, idx_t capacity_p) : capacity(capacity_p), size(0) {
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

TupleDataAllocator::TupleDataAllocator(BufferManager &buffer_manager, const TupleDataLayout &layout)
    : buffer_manager(buffer_manager), layout(layout) {
}

Allocator &TupleDataAllocator::GetAllocator() {
	return buffer_manager.GetBufferAllocator();
}

const TupleDataLayout &TupleDataAllocator::GetLayout() {
	return layout;
}

void TupleDataAllocator::Build(TupleDataAppendState &append_state, idx_t count, TupleDataSegment &segment) {
	D_ASSERT(this == segment.allocator.get());
	auto &chunks = segment.chunks;
	if (!chunks.empty()) {
		if (chunks.back().count == STANDARD_VECTOR_SIZE) {
			static TupleDataChunk DUMMY_CHUNK;
			ReleaseOrStoreHandles(append_state.chunk_state, segment, DUMMY_CHUNK);
		} else {
			ReleaseOrStoreHandles(append_state.chunk_state, segment, chunks.back());
		}
	}

	// Build the chunk parts for the incoming data
	vector<pair<idx_t, idx_t>> chunk_part_indices;
	idx_t offset = 0;
	while (offset != count) {
		if (chunks.empty() || chunks.back().count == STANDARD_VECTOR_SIZE) {
			chunks.emplace_back();
		}
		auto &chunk = chunks.back();

		// Build the next part
		chunk.AddPart(BuildChunkPart(append_state.chunk_state, offset, count));
		chunk_part_indices.emplace_back(chunks.size() - 1, chunk.parts.size() - 1);

		auto &chunk_part = chunk.parts.back();
		const auto next = chunk_part.count;
		segment.count += next;

		offset += next;
	}

	// Now initialize the pointers to write the data to
	vector<TupleDataChunkPart *> parts;
	parts.reserve(chunk_part_indices.size());
	for (auto &indices : chunk_part_indices) {
		parts.emplace_back(&segment.chunks[indices.first].parts[indices.second]);
	}
	InitializeChunkStateInternal(append_state.chunk_state, false, true, parts);

	// To reduce metadata, we try to merge chunk parts where possible
	// Due to the way chunk parts are constructed, only the last part of the first chunk is eligible for merging
	segment.chunks[chunk_part_indices[0].first].MergeLastChunkPart();

	segment.Verify();
}

TupleDataChunkPart TupleDataAllocator::BuildChunkPart(TupleDataManagementState &state, idx_t offset, idx_t count) {
	TupleDataChunkPart result;

	lock_guard<mutex> guard(lock);
	// Allocate row block (if needed)
	if (row_blocks.empty() || row_blocks.back().RemainingCapacity() < layout.GetRowWidth()) {
		row_blocks.emplace_back(buffer_manager, (idx_t)Storage::BLOCK_SIZE);
	}
	result.row_block_index = row_blocks.size() - 1;
	result.row_block_offset = row_blocks.back().size;

	// Set count (might be reduced later when checking heap space)
	result.count = MinValue<idx_t>(row_blocks.back().RemainingCapacity(layout.GetRowWidth()), count - offset);
	if (!layout.AllConstant()) {
		const auto heap_sizes = FlatVector::GetData<idx_t>(state.heap_sizes);

		// Allocate heap block (if needed)
		if (heap_blocks.empty() || heap_blocks.back().RemainingCapacity() < heap_sizes[offset]) {
			const auto size = MaxValue<idx_t>((idx_t)Storage::BLOCK_SIZE, heap_sizes[offset]);
			heap_blocks.emplace_back(buffer_manager, size);
		}
		result.heap_block_index = heap_blocks.size() - 1;
		result.heap_block_offset = heap_blocks.back().size;
		result.base_heap_ptr = GetBaseHeapPointer(state, result);

		// Determine how many we can read next
		const auto heap_remaining = heap_blocks.back().RemainingCapacity();
		result.total_heap_size = 0;
		for (idx_t i = offset; i < count; i++) {
			const auto &heap_size = heap_sizes[i];
			if (result.total_heap_size + heap_size > heap_remaining) {
				result.count = i;
				break;
			}
			result.total_heap_size += heap_size;
		}

		// Mark this portion of the heap block as filled
		heap_blocks.back().size += result.total_heap_size;
	}
	D_ASSERT(result.count != 0 && result.count <= STANDARD_VECTOR_SIZE);

	// Mark this portion of the row block as filled
	row_blocks.back().size += result.count * layout.GetRowWidth();

	return result;
}

void TupleDataAllocator::InitializeChunkState(TupleDataManagementState &state, TupleDataSegment &segment,
                                              idx_t chunk_idx, bool init_heap) {
	D_ASSERT(this == segment.allocator.get());
	D_ASSERT(chunk_idx < segment.chunks.size());
	auto &chunk = segment.chunks[chunk_idx];

	// Release or store any handles that are no longer required
	ReleaseOrStoreHandles(state, segment, chunk);

	vector<TupleDataChunkPart *> parts;
	parts.reserve(chunk.parts.size());
	for (auto &part : chunk.parts) {
		parts.emplace_back(&part);
	}

	InitializeChunkStateInternal(state, init_heap, init_heap, parts);
}

void TupleDataAllocator::InitializeChunkStateInternal(TupleDataManagementState &state, bool init_heap_pointers,
                                                      bool init_heap_sizes, vector<TupleDataChunkPart *> &parts) {
	auto row_locations = FlatVector::GetData<data_ptr_t>(state.row_locations);
	auto heap_sizes = FlatVector::GetData<idx_t>(state.heap_sizes);
	auto heap_locations = FlatVector::GetData<data_ptr_t>(state.heap_locations);

	idx_t offset = 0;
	for (auto &part : parts) {
		const auto next = part->count;

		// Set up row locations for the scan
		const auto row_width = layout.GetRowWidth();
		const auto base_row_ptr = GetRowPointer(state, *part);
		for (idx_t i = 0; i < next; i++) {
			row_locations[offset + i] = base_row_ptr + i * row_width;
		}

		if (!layout.AllConstant()) {
			const auto base_heap_ptr = GetBaseHeapPointer(state, *part);
			const auto new_heap_ptr = base_heap_ptr + part->heap_block_offset;

			// Check if heap block has changed - re-compute the pointers within each row if so
			if (state.properties != TupleDataPinProperties::ALREADY_PINNED) {
				const auto old_base_heap_ptr = part->base_heap_ptr;
				if (old_base_heap_ptr != base_heap_ptr) {
					auto old_heap_ptr = old_base_heap_ptr + part->heap_block_offset;
					RecomputeHeapPointers(old_heap_ptr, new_heap_ptr, row_locations, offset, next, layout, 0);
					part->base_heap_ptr = base_heap_ptr;
				}
			}

			if (init_heap_sizes) {
				// Read the heap sizes from the rows
				const auto heap_size_offset = layout.GetHeapSizeOffset();
				for (idx_t i = 0; i < next; i++) {
					auto idx = offset + i;
					heap_sizes[idx] = Load<uint32_t>(row_locations[idx] + heap_size_offset);
				}
			}

			if (init_heap_pointers) {
				// Set the pointers where the heap data will be written (if needed)
				heap_locations[offset] = new_heap_ptr;
				for (idx_t i = 1; i < next; i++) {
					auto idx = offset + i;
					heap_locations[idx] = heap_locations[idx - 1] + heap_sizes[idx - 1];
				}
			}
		}

		offset += next;
	}
	D_ASSERT(offset <= STANDARD_VECTOR_SIZE);
}

void TupleDataAllocator::RecomputeHeapPointers(const data_ptr_t old_heap_ptr, const data_ptr_t new_heap_ptr,
                                               const data_ptr_t row_locations[], const idx_t offset, const idx_t count,
                                               const TupleDataLayout &layout, const idx_t base_col_offset) {
	for (idx_t col_idx = 0; col_idx < layout.ColumnCount(); col_idx++) {
		const auto &col_offset = base_col_offset + layout.GetOffsets()[col_idx];
		switch (layout.GetTypes()[col_idx].InternalType()) {
		case PhysicalType::VARCHAR: {
			for (idx_t i = 0; i < count; i++) {
				const auto &string_location = row_locations[offset + i] + col_offset;
				if (Load<uint32_t>(string_location) > string_t::INLINE_LENGTH) {
					const auto diff = Load<data_ptr_t>(string_location + string_t::HEADER_SIZE) - old_heap_ptr;
					Store<data_ptr_t>(new_heap_ptr + diff, string_location + string_t::HEADER_SIZE);
				}
			}
			break;
		}
		case PhysicalType::LIST: {
			for (idx_t i = 0; i < count; i++) {
				const auto &pointer_location = row_locations[offset + i] + col_offset;
				const auto diff = Load<data_ptr_t>(pointer_location) - old_heap_ptr;
				Store<data_ptr_t>(new_heap_ptr + diff, pointer_location);
			}
			break;
		}
		case PhysicalType::STRUCT: {
			D_ASSERT(layout.GetStructLayouts().find(col_idx) != layout.GetStructLayouts().end());
			const auto &struct_layout = layout.GetStructLayouts().find(col_idx)->second;
			if (!struct_layout.AllConstant()) {
				RecomputeHeapPointers(old_heap_ptr, new_heap_ptr, row_locations, offset, count, struct_layout,
				                      col_offset);
			}
			break;
		}
		default:
			continue;
		}
	}
}

void TupleDataAllocator::ReleaseOrStoreHandles(TupleDataManagementState &state, TupleDataSegment &segment,
                                               TupleDataChunk &chunk) const {
	if (state.properties == TupleDataPinProperties::ALREADY_PINNED) {
		return;
	}
	ReleaseOrStoreHandlesInternal(state.row_handles, chunk.row_block_ids, segment, state.properties);
	if (!layout.AllConstant()) {
		ReleaseOrStoreHandlesInternal(state.heap_handles, chunk.heap_block_ids, segment, state.properties);
	}
}

void TupleDataAllocator::ReleaseOrStoreHandlesInternal(unordered_map<uint32_t, BufferHandle> &handles,
                                                       const unordered_set<uint32_t> &block_ids,
                                                       TupleDataSegment &segment, TupleDataPinProperties properties) {
	D_ASSERT(properties != TupleDataPinProperties::ALREADY_PINNED);
	bool found_handle;
	do {
		found_handle = false;
		for (auto it = handles.begin(); it != handles.end(); it++) {
			if (block_ids.find(it->first) != block_ids.end()) {
				// still required: do not release
				continue;
			}
			switch (properties) {
			case TupleDataPinProperties::KEEP_EVERYTHING_PINNED: {
				lock_guard<mutex> guard(segment.pinned_handles_lock);
				segment.pinned_handles.emplace_back(std::move(it->second));
				break;
			}
			case TupleDataPinProperties::UNPIN_AFTER_DONE:
				break;
			default:
				throw InternalException("Encountered TupleDataPinProperties::INVALID");
			}
			handles.erase(it);
			found_handle = true;
			break;
		}
	} while (found_handle);
}

void TupleDataAllocator::PinRowBlock(TupleDataManagementState &state, const uint32_t row_block_index) {
	if (state.row_handles.find(row_block_index) == state.row_handles.end()) {
		shared_ptr<BlockHandle> handle;
		{
			lock_guard<mutex> guard(lock);
			D_ASSERT(row_block_index < row_blocks.size());
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
			D_ASSERT(heap_block_index < heap_blocks.size());
			handle = heap_blocks[heap_block_index].handle;
		}
		state.row_handles[heap_block_index] = buffer_manager.Pin(handle);
	}
}

data_ptr_t TupleDataAllocator::GetRowPointer(TupleDataManagementState &state, const TupleDataChunkPart &part) {
	PinRowBlock(state, part.row_block_index);
	return state.row_handles[part.row_block_index].Ptr() + part.row_block_offset;
}

data_ptr_t TupleDataAllocator::GetBaseHeapPointer(TupleDataManagementState &state, const TupleDataChunkPart &part) {
	PinHeapBlock(state, part.heap_block_index);
	return state.heap_handles[part.heap_block_index].Ptr();
}

} // namespace duckdb
