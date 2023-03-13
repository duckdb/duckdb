#include "duckdb/common/types/row/tuple_data_allocator.hpp"

#include "duckdb/common/types/row/tuple_data_segment.hpp"
#include "duckdb/common/types/row/tuple_data_states.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

using ValidityBytes = TupleDataLayout::ValidityBytes;

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

TupleDataAllocator::TupleDataAllocator(TupleDataAllocator &allocator)
    : buffer_manager(allocator.buffer_manager), layout(allocator.layout) {
}

Allocator &TupleDataAllocator::GetAllocator() {
	return buffer_manager.GetBufferAllocator();
}

const TupleDataLayout &TupleDataAllocator::GetLayout() {
	return layout;
}

void TupleDataAllocator::Build(TupleDataSegment &segment, TupleDataPinState &pin_state,
                               TupleDataChunkState &chunk_state, const idx_t append_offset, const idx_t append_count) {
	D_ASSERT(this == segment.allocator.get());
	auto &chunks = segment.chunks;
	if (!chunks.empty()) {
		ReleaseOrStoreHandles(pin_state, segment, chunks.back());
	}

	// Build the chunk parts for the incoming data
	vector<pair<idx_t, idx_t>> chunk_part_indices;
	idx_t offset = 0;
	while (offset != append_count) {
		if (chunks.empty() || chunks.back().count == STANDARD_VECTOR_SIZE) {
			chunks.emplace_back();
		}
		auto &chunk = chunks.back();

		// Build the next part
		auto next = MinValue<idx_t>(append_count - offset, STANDARD_VECTOR_SIZE - chunk.count);
		chunk.AddPart(BuildChunkPart(pin_state, chunk_state, append_offset + offset, next), layout);
		chunk_part_indices.emplace_back(chunks.size() - 1, chunk.parts.size() - 1);

		auto &chunk_part = chunk.parts.back();
		next = chunk_part.count;
		segment.count += next;

		offset += next;
	}

	// Now initialize the pointers to write the data to
	vector<TupleDataChunkPart *> parts;
	parts.reserve(chunk_part_indices.size());
	for (auto &indices : chunk_part_indices) {
		parts.emplace_back(&segment.chunks[indices.first].parts[indices.second]);
	}
	InitializeChunkStateInternal(pin_state, chunk_state, append_offset, true, false, parts);

	// To reduce metadata, we try to merge chunk parts where possible
	// Due to the way chunk parts are constructed, only the last part of the first chunk is eligible for merging
	segment.chunks[chunk_part_indices[0].first].MergeLastChunkPart(layout);

	segment.Verify();
}

TupleDataChunkPart TupleDataAllocator::BuildChunkPart(TupleDataPinState &pin_state, TupleDataChunkState &chunk_state,
                                                      const idx_t append_offset, const idx_t append_count) {
	D_ASSERT(append_count != 0);
	TupleDataChunkPart result;

	// Allocate row block (if needed)
	if (row_blocks.empty() || row_blocks.back().RemainingCapacity() < layout.GetRowWidth()) {
		row_blocks.emplace_back(buffer_manager, (idx_t)Storage::BLOCK_SIZE);
	}
	result.row_block_index = row_blocks.size() - 1;
	auto &row_block = row_blocks[result.row_block_index];
	result.row_block_offset = row_block.size;

	// Set count (might be reduced later when checking heap space)
	result.count = MinValue<idx_t>(row_block.RemainingCapacity(layout.GetRowWidth()), append_count);
	if (!layout.AllConstant()) {
		UnifiedVectorFormat heap_size_data;
		chunk_state.heap_sizes.ToUnifiedFormat(append_offset + append_count, heap_size_data);
		const auto heap_sizes = FlatVector::GetData<idx_t>(chunk_state.heap_sizes);

		// Compute total heap size first
		idx_t total_heap_size = 0;
		for (idx_t i = 0; i < result.count; i++) {
			const auto &heap_size = heap_sizes[append_offset + i];
			total_heap_size += heap_size;
		}

		if (total_heap_size == 0) {
			// We don't need a heap at all
			result.heap_block_index = TupleDataChunkPart::INVALID_INDEX;
			result.heap_block_offset = TupleDataChunkPart::INVALID_INDEX;
			result.base_heap_ptr = nullptr;
			result.total_heap_size = 0;
		} else {
			// Allocate heap block (if needed)
			if (heap_blocks.empty() || heap_blocks.back().RemainingCapacity() < heap_sizes[append_offset]) {
				const auto size = MaxValue<idx_t>((idx_t)Storage::BLOCK_SIZE, heap_sizes[append_offset]);
				heap_blocks.emplace_back(buffer_manager, size);
			}
			result.heap_block_index = heap_blocks.size() - 1;
			auto &heap_block = heap_blocks[result.heap_block_index];
			result.heap_block_offset = heap_block.size;
			result.base_heap_ptr = GetBaseHeapPointer(pin_state, result);

			const auto heap_remaining = heap_block.RemainingCapacity();
			if (total_heap_size <= heap_remaining) {
				// Everything fits
				result.total_heap_size = total_heap_size;
			} else {
				// Not everything fits - determine how many we can read next
				result.total_heap_size = 0;
				for (idx_t i = 0; i < result.count; i++) {
					const auto &heap_size = heap_sizes[append_offset + i];
					if (result.total_heap_size + heap_size > heap_remaining) {
						result.count = i;
						break;
					}
					result.total_heap_size += heap_size;
				}
			}

			// Mark this portion of the heap block as filled
			heap_block.size += result.total_heap_size;
		}
	}
	D_ASSERT(result.count != 0 && result.count <= STANDARD_VECTOR_SIZE);

	// Mark this portion of the row block as filled
	row_block.size += result.count * layout.GetRowWidth();

	return result;
}

void TupleDataAllocator::InitializeChunkState(TupleDataSegment &segment, TupleDataPinState &pin_state,
                                              TupleDataChunkState &chunk_state, idx_t chunk_idx, bool init_heap) {
	D_ASSERT(this == segment.allocator.get());
	D_ASSERT(chunk_idx < segment.chunks.size());
	auto &chunk = segment.chunks[chunk_idx];

	// Release or store any handles that are no longer required
	ReleaseOrStoreHandles(pin_state, segment, chunk);

	vector<TupleDataChunkPart *> parts;
	parts.reserve(chunk.parts.size());
	for (auto &part : chunk.parts) {
		parts.emplace_back(&part);
	}

	InitializeChunkStateInternal(pin_state, chunk_state, 0, init_heap, init_heap, parts);
}

void VerifyTotalHeapSize(const idx_t heap_sizes[], const idx_t offset, const idx_t count,
                         const TupleDataChunkPart &part) {
#ifdef DEBUG
	idx_t total_heap_size = 0;
	for (idx_t i = 0; i < count; i++) {
		auto idx = offset + i;
		total_heap_size += heap_sizes[idx];
	}
	D_ASSERT(total_heap_size == part.total_heap_size);
#endif
}

void TupleDataAllocator::InitializeChunkStateInternal(TupleDataPinState &pin_state, TupleDataChunkState &chunk_state,
                                                      idx_t offset, bool init_heap_pointers, bool init_heap_sizes,
                                                      vector<TupleDataChunkPart *> &parts) {
	auto row_locations = FlatVector::GetData<data_ptr_t>(chunk_state.row_locations);
	auto heap_sizes = FlatVector::GetData<idx_t>(chunk_state.heap_sizes);
	auto heap_locations = FlatVector::GetData<data_ptr_t>(chunk_state.heap_locations);

	for (auto &part : parts) {
		const auto next = part->count;

		// Set up row locations for the scan
		const auto row_width = layout.GetRowWidth();
		const auto base_row_ptr = GetRowPointer(pin_state, *part);
		for (idx_t i = 0; i < next; i++) {
			row_locations[offset + i] = base_row_ptr + i * row_width;
		}

		if (!layout.AllConstant() && part->total_heap_size != 0) {
			const auto base_heap_ptr = GetBaseHeapPointer(pin_state, *part);
			const auto new_heap_ptr = base_heap_ptr + part->heap_block_offset;

			// Check if heap block has changed - re-compute the pointers within each row if so
			if (pin_state.properties != TupleDataPinProperties::ALREADY_PINNED) {
				const auto old_base_heap_ptr = part->base_heap_ptr;
				if (old_base_heap_ptr != base_heap_ptr) {
					const auto old_heap_ptr = old_base_heap_ptr + part->heap_block_offset;
					Vector old_heap_ptrs(Value::POINTER((uintptr_t)old_heap_ptr));
					Vector new_heap_ptrs(Value::POINTER((uintptr_t)new_heap_ptr));
					RecomputeHeapPointers(old_heap_ptrs, *ConstantVector::ZeroSelectionVector(), row_locations,
					                      new_heap_ptrs, offset, next, layout, 0);
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
				VerifyTotalHeapSize(heap_sizes, offset, next, *part);
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

static inline void VerifyStrings(const data_ptr_t row_locations[], const idx_t col_idx, const idx_t col_offset,
                                 const idx_t offset, const idx_t count) {
#ifdef DEBUG
	idx_t entry_idx;
	idx_t idx_in_entry;
	ValidityBytes::GetEntryIndex(col_idx, entry_idx, idx_in_entry);
	for (idx_t i = 0; i < count; i++) {
		const auto &row_location = row_locations[offset + i];
		ValidityBytes row_mask(row_location);
		if (row_mask.RowIsValid(row_mask.GetValidityEntry(entry_idx), idx_in_entry)) {
			auto recomputed_string = Load<string_t>(row_location + col_offset);
			recomputed_string.Verify();
		}
	}
#endif
}

void TupleDataAllocator::RecomputeHeapPointers(Vector &old_heap_ptrs, const SelectionVector &old_heap_sel,
                                               const data_ptr_t row_locations[], Vector &new_heap_ptrs,
                                               const idx_t offset, const idx_t count, const TupleDataLayout &layout,
                                               const idx_t base_col_offset) {
	const auto old_heap_locations = FlatVector::GetData<data_ptr_t>(old_heap_ptrs);

	UnifiedVectorFormat new_heap_data;
	new_heap_ptrs.ToUnifiedFormat(offset + count, new_heap_data);
	const auto new_heap_locations = (data_ptr_t *)new_heap_data.data;
	const auto new_heap_sel = *new_heap_data.sel;

	for (idx_t col_idx = 0; col_idx < layout.ColumnCount(); col_idx++) {
		const auto &col_offset = base_col_offset + layout.GetOffsets()[col_idx];
		switch (layout.GetTypes()[col_idx].InternalType()) {
		case PhysicalType::VARCHAR: {
			for (idx_t i = 0; i < count; i++) {
				const auto idx = offset + i;
				const auto &old_heap_ptr = old_heap_locations[old_heap_sel.get_index(idx)];
				const auto &new_heap_ptr = new_heap_locations[new_heap_sel.get_index(idx)];

				const auto string_location = row_locations[idx] + col_offset;
				if (Load<uint32_t>(string_location) > string_t::INLINE_LENGTH) {
					const auto diff = Load<data_ptr_t>(string_location + string_t::HEADER_SIZE) - old_heap_ptr;
					D_ASSERT(diff >= 0);
					Store<data_ptr_t>(new_heap_ptr + diff, string_location + string_t::HEADER_SIZE);
				}
			}
			VerifyStrings(row_locations, col_idx, col_offset, offset, count);
			break;
		}
		case PhysicalType::LIST: {
			for (idx_t i = 0; i < count; i++) {
				const auto idx = offset + i;
				const auto &old_heap_ptr = old_heap_locations[old_heap_sel.get_index(idx)];
				const auto &new_heap_ptr = new_heap_locations[new_heap_sel.get_index(idx)];

				const auto &pointer_location = row_locations[idx] + col_offset;
				const auto diff = Load<data_ptr_t>(pointer_location) - old_heap_ptr;
				D_ASSERT(diff >= 0);
				Store<data_ptr_t>(new_heap_ptr + diff, pointer_location);
			}
			break;
		}
		case PhysicalType::STRUCT: {
			D_ASSERT(layout.GetStructLayouts().find(col_idx) != layout.GetStructLayouts().end());
			const auto &struct_layout = layout.GetStructLayouts().find(col_idx)->second;
			if (!struct_layout.AllConstant()) {
				RecomputeHeapPointers(old_heap_ptrs, old_heap_sel, row_locations, new_heap_ptrs, offset, count,
				                      struct_layout, col_offset);
			}
			break;
		}
		default:
			continue;
		}
	}
}

void TupleDataAllocator::ReleaseOrStoreHandles(TupleDataPinState &pin_state, TupleDataSegment &segment,
                                               TupleDataChunk &chunk) {
	D_ASSERT(this == segment.allocator.get());
	if (pin_state.properties == TupleDataPinProperties::ALREADY_PINNED) {
		return;
	}
	ReleaseOrStoreHandlesInternal(segment, pin_state.row_handles, chunk.row_block_ids, row_blocks,
	                              pin_state.properties);
	if (!layout.AllConstant()) {
		ReleaseOrStoreHandlesInternal(segment, pin_state.heap_handles, chunk.heap_block_ids, heap_blocks,
		                              pin_state.properties);
	}
}

void TupleDataAllocator::ReleaseOrStoreHandles(TupleDataPinState &pin_state, TupleDataSegment &segment) {
	static TupleDataChunk DUMMY_CHUNK;
	ReleaseOrStoreHandles(pin_state, segment, DUMMY_CHUNK);
}

void TupleDataAllocator::ReleaseOrStoreHandlesInternal(TupleDataSegment &segment,
                                                       unordered_map<uint32_t, BufferHandle> &handles,
                                                       const unordered_set<uint32_t> &block_ids,
                                                       vector<TupleDataBlock> &blocks,
                                                       TupleDataPinProperties properties) {
	D_ASSERT(properties != TupleDataPinProperties::ALREADY_PINNED);
	bool found_handle;
	do {
		found_handle = false;
		for (auto it = handles.begin(); it != handles.end(); it++) {
			auto block_id = it->first;
			if (block_ids.find(block_id) != block_ids.end()) {
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
			case TupleDataPinProperties::DESTROY_AFTER_DONE:
				blocks[block_id].handle = nullptr;
				break;
			default:
				D_ASSERT(properties == TupleDataPinProperties::INVALID);
				throw InternalException("Encountered TupleDataPinProperties::INVALID");
			}
			handles.erase(it);
			found_handle = true;
			break;
		}
	} while (found_handle);
}

void TupleDataAllocator::PinRowBlock(TupleDataPinState &pin_state, const uint32_t row_block_index) {
	if (pin_state.row_handles.find(row_block_index) == pin_state.row_handles.end()) {
		pin_state.row_handles[row_block_index] = buffer_manager.Pin(row_blocks[row_block_index].handle);
	}
}

void TupleDataAllocator::PinHeapBlock(TupleDataPinState &pin_state, const uint32_t heap_block_index) {
	if (pin_state.heap_handles.find(heap_block_index) == pin_state.heap_handles.end()) {
		pin_state.heap_handles[heap_block_index] = buffer_manager.Pin(heap_blocks[heap_block_index].handle);
	}
}

data_ptr_t TupleDataAllocator::GetRowPointer(TupleDataPinState &pin_state, const TupleDataChunkPart &part) {
	PinRowBlock(pin_state, part.row_block_index);
	return pin_state.row_handles[part.row_block_index].Ptr() + part.row_block_offset;
}

data_ptr_t TupleDataAllocator::GetBaseHeapPointer(TupleDataPinState &pin_state, const TupleDataChunkPart &part) {
	PinHeapBlock(pin_state, part.heap_block_index);
	return pin_state.heap_handles[part.heap_block_index].Ptr();
}

} // namespace duckdb
