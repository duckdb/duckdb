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
    : buffer_manager(buffer_manager), layout(layout.Copy()) {
}

TupleDataAllocator::TupleDataAllocator(TupleDataAllocator &allocator)
    : buffer_manager(allocator.buffer_manager), layout(allocator.layout.Copy()) {
}

BufferManager &TupleDataAllocator::GetBufferManager() {
	return buffer_manager;
}

Allocator &TupleDataAllocator::GetAllocator() {
	return buffer_manager.GetBufferAllocator();
}

const TupleDataLayout &TupleDataAllocator::GetLayout() const {
	return layout;
}

idx_t TupleDataAllocator::RowBlockCount() const {
	return row_blocks.size();
}

idx_t TupleDataAllocator::HeapBlockCount() const {
	return heap_blocks.size();
}

void TupleDataAllocator::Build(TupleDataSegment &segment, TupleDataPinState &pin_state,
                               TupleDataChunkState &chunk_state, const idx_t append_offset, const idx_t append_count) {
	D_ASSERT(this == segment.allocator.get());
	auto &chunks = segment.chunks;
	if (!chunks.empty()) {
		ReleaseOrStoreHandles(pin_state, segment, chunks.back(), true);
	}

	// Build the chunk parts for the incoming data
	chunk_part_indices.clear();
	idx_t offset = 0;
	while (offset != append_count) {
		if (chunks.empty() || chunks.back().count == STANDARD_VECTOR_SIZE) {
			chunks.emplace_back();
		}
		auto &chunk = chunks.back();

		// Build the next part
		auto next = MinValue<idx_t>(append_count - offset, STANDARD_VECTOR_SIZE - chunk.count);
		chunk.AddPart(BuildChunkPart(pin_state, chunk_state, append_offset + offset, next, chunk), layout);
		auto &chunk_part = chunk.parts.back();
		next = chunk_part.count;

		segment.count += next;
		segment.data_size += chunk_part.count * layout.GetRowWidth();
		if (!layout.AllConstant()) {
			segment.data_size += chunk_part.total_heap_size;
		}

		offset += next;
		chunk_part_indices.emplace_back(chunks.size() - 1, chunk.parts.size() - 1);
	}

	// Now initialize the pointers to write the data to
	chunk_parts.clear();
	for (auto &indices : chunk_part_indices) {
		chunk_parts.emplace_back(segment.chunks[indices.first].parts[indices.second]);
	}
	InitializeChunkStateInternal(pin_state, chunk_state, append_offset, false, true, false, chunk_parts);

	// To reduce metadata, we try to merge chunk parts where possible
	// Due to the way chunk parts are constructed, only the last part of the first chunk is eligible for merging
	segment.chunks[chunk_part_indices[0].first].MergeLastChunkPart(layout);

	segment.Verify();
}

TupleDataChunkPart TupleDataAllocator::BuildChunkPart(TupleDataPinState &pin_state, TupleDataChunkState &chunk_state,
                                                      const idx_t append_offset, const idx_t append_count,
                                                      TupleDataChunk &chunk) {
	D_ASSERT(append_count != 0);
	TupleDataChunkPart result(*chunk.lock);

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
			result.total_heap_size = 0;
			result.base_heap_ptr = nullptr;
		} else {
			// Allocate heap block (if needed)
			if (heap_blocks.empty() || heap_blocks.back().RemainingCapacity() < heap_sizes[append_offset]) {
				const auto size = MaxValue<idx_t>((idx_t)Storage::BLOCK_SIZE, heap_sizes[append_offset]);
				heap_blocks.emplace_back(buffer_manager, size);
			}
			result.heap_block_index = heap_blocks.size() - 1;
			auto &heap_block = heap_blocks[result.heap_block_index];
			result.heap_block_offset = heap_block.size;

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

			// Mark this portion of the heap block as filled and set the pointer
			heap_block.size += result.total_heap_size;
			result.base_heap_ptr = GetBaseHeapPointer(pin_state, result);
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
	D_ASSERT(chunk_idx < segment.ChunkCount());
	auto &chunk = segment.chunks[chunk_idx];

	// Release or store any handles that are no longer required:
	// We can't release the heap here if the current chunk's heap_block_ids is empty, because if we are iterating with
	// PinProperties::DESTROY_AFTER_DONE, we might destroy a heap block that is needed by a later chunk, e.g.,
	// when chunk 0 needs heap block 0, chunk 1 does not need any heap blocks, and chunk 2 needs heap block 0 again
	ReleaseOrStoreHandles(pin_state, segment, chunk, !chunk.heap_block_ids.empty());

	unsafe_vector<reference<TupleDataChunkPart>> parts;
	parts.reserve(chunk.parts.size());
	for (auto &part : chunk.parts) {
		parts.emplace_back(part);
	}

	InitializeChunkStateInternal(pin_state, chunk_state, 0, true, init_heap, init_heap, parts);
}

static inline void InitializeHeapSizes(const data_ptr_t row_locations[], idx_t heap_sizes[], const idx_t offset,
                                       const idx_t next, const TupleDataChunkPart &part, const idx_t heap_size_offset) {
	// Read the heap sizes from the rows
	for (idx_t i = 0; i < next; i++) {
		auto idx = offset + i;
		heap_sizes[idx] = Load<uint32_t>(row_locations[idx] + heap_size_offset);
	}

	// Verify total size
#ifdef DEBUG
	idx_t total_heap_size = 0;
	for (idx_t i = 0; i < next; i++) {
		auto idx = offset + i;
		total_heap_size += heap_sizes[idx];
	}
	D_ASSERT(total_heap_size == part.total_heap_size);
#endif
}

void TupleDataAllocator::InitializeChunkStateInternal(TupleDataPinState &pin_state, TupleDataChunkState &chunk_state,
                                                      idx_t offset, bool recompute, bool init_heap_pointers,
                                                      bool init_heap_sizes,
                                                      unsafe_vector<reference<TupleDataChunkPart>> &parts) {
	auto row_locations = FlatVector::GetData<data_ptr_t>(chunk_state.row_locations);
	auto heap_sizes = FlatVector::GetData<idx_t>(chunk_state.heap_sizes);
	auto heap_locations = FlatVector::GetData<data_ptr_t>(chunk_state.heap_locations);

	for (auto &part_ref : parts) {
		auto &part = part_ref.get();
		const auto next = part.count;

		// Set up row locations for the scan
		const auto row_width = layout.GetRowWidth();
		const auto base_row_ptr = GetRowPointer(pin_state, part);
		for (idx_t i = 0; i < next; i++) {
			row_locations[offset + i] = base_row_ptr + i * row_width;
		}

		if (layout.AllConstant()) { // Can't have a heap
			offset += next;
			continue;
		}

		if (part.total_heap_size == 0) {
			if (init_heap_sizes) { // No heap, but we need the heap sizes
				InitializeHeapSizes(row_locations, heap_sizes, offset, next, part, layout.GetHeapSizeOffset());
			}
			offset += next;
			continue;
		}

		// Check if heap block has changed - re-compute the pointers within each row if so
		if (recompute && pin_state.properties != TupleDataPinProperties::ALREADY_PINNED) {
			const auto new_base_heap_ptr = GetBaseHeapPointer(pin_state, part);
			if (part.base_heap_ptr != new_base_heap_ptr) {
				lock_guard<mutex> guard(part.lock);
				const auto old_base_heap_ptr = part.base_heap_ptr;
				if (old_base_heap_ptr != new_base_heap_ptr) {
					Vector old_heap_ptrs(
					    Value::POINTER(CastPointerToValue(old_base_heap_ptr + part.heap_block_offset)));
					Vector new_heap_ptrs(
					    Value::POINTER(CastPointerToValue(new_base_heap_ptr + part.heap_block_offset)));
					RecomputeHeapPointers(old_heap_ptrs, *ConstantVector::ZeroSelectionVector(), row_locations,
					                      new_heap_ptrs, offset, next, layout, 0);
					part.base_heap_ptr = new_base_heap_ptr;
				}
			}
		}

		if (init_heap_sizes) {
			InitializeHeapSizes(row_locations, heap_sizes, offset, next, part, layout.GetHeapSizeOffset());
		}

		if (init_heap_pointers) {
			// Set the pointers where the heap data will be written (if needed)
			heap_locations[offset] = part.base_heap_ptr + part.heap_block_offset;
			for (idx_t i = 1; i < next; i++) {
				auto idx = offset + i;
				heap_locations[idx] = heap_locations[idx - 1] + heap_sizes[idx - 1];
			}
		}

		offset += next;
	}
	D_ASSERT(offset <= STANDARD_VECTOR_SIZE);
}

static inline void VerifyStrings(const LogicalTypeId type_id, const data_ptr_t row_locations[], const idx_t col_idx,
                                 const idx_t base_col_offset, const idx_t col_offset, const idx_t offset,
                                 const idx_t count) {
#ifdef DEBUG
	if (type_id != LogicalTypeId::VARCHAR) {
		// Make sure we don't verify BLOB / AGGREGATE_STATE
		return;
	}
	idx_t entry_idx;
	idx_t idx_in_entry;
	ValidityBytes::GetEntryIndex(col_idx, entry_idx, idx_in_entry);
	for (idx_t i = 0; i < count; i++) {
		const auto &row_location = row_locations[offset + i] + base_col_offset;
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
	const auto new_heap_locations = UnifiedVectorFormat::GetData<data_ptr_t>(new_heap_data);
	const auto new_heap_sel = *new_heap_data.sel;

	for (idx_t col_idx = 0; col_idx < layout.ColumnCount(); col_idx++) {
		const auto &col_offset = layout.GetOffsets()[col_idx];

		// Precompute mask indexes
		idx_t entry_idx;
		idx_t idx_in_entry;
		ValidityBytes::GetEntryIndex(col_idx, entry_idx, idx_in_entry);

		const auto &type = layout.GetTypes()[col_idx];
		switch (type.InternalType()) {
		case PhysicalType::VARCHAR: {
			for (idx_t i = 0; i < count; i++) {
				const auto idx = offset + i;
				const auto &row_location = row_locations[idx] + base_col_offset;
				ValidityBytes row_mask(row_location);
				if (!row_mask.RowIsValid(row_mask.GetValidityEntry(entry_idx), idx_in_entry)) {
					continue;
				}

				const auto &old_heap_ptr = old_heap_locations[old_heap_sel.get_index(idx)];
				const auto &new_heap_ptr = new_heap_locations[new_heap_sel.get_index(idx)];

				const auto string_location = row_location + col_offset;
				if (Load<uint32_t>(string_location) > string_t::INLINE_LENGTH) {
					const auto string_ptr_location = string_location + string_t::HEADER_SIZE;
					const auto string_ptr = Load<data_ptr_t>(string_ptr_location);
					const auto diff = string_ptr - old_heap_ptr;
					D_ASSERT(diff >= 0);
					Store<data_ptr_t>(new_heap_ptr + diff, string_ptr_location);
				}
			}
			VerifyStrings(type.id(), row_locations, col_idx, base_col_offset, col_offset, offset, count);
			break;
		}
		case PhysicalType::LIST: {
			for (idx_t i = 0; i < count; i++) {
				const auto idx = offset + i;
				const auto &row_location = row_locations[idx] + base_col_offset;
				ValidityBytes row_mask(row_location);
				if (!row_mask.RowIsValid(row_mask.GetValidityEntry(entry_idx), idx_in_entry)) {
					continue;
				}

				const auto &old_heap_ptr = old_heap_locations[old_heap_sel.get_index(idx)];
				const auto &new_heap_ptr = new_heap_locations[new_heap_sel.get_index(idx)];

				const auto &list_ptr_location = row_location + col_offset;
				const auto list_ptr = Load<data_ptr_t>(list_ptr_location);
				const auto diff = list_ptr - old_heap_ptr;
				D_ASSERT(diff >= 0);
				Store<data_ptr_t>(new_heap_ptr + diff, list_ptr_location);
			}
			break;
		}
		case PhysicalType::STRUCT: {
			const auto &struct_layout = layout.GetStructLayout(col_idx);
			if (!struct_layout.AllConstant()) {
				RecomputeHeapPointers(old_heap_ptrs, old_heap_sel, row_locations, new_heap_ptrs, offset, count,
				                      struct_layout, base_col_offset + col_offset);
			}
			break;
		}
		default:
			continue;
		}
	}
}

void TupleDataAllocator::ReleaseOrStoreHandles(TupleDataPinState &pin_state, TupleDataSegment &segment,
                                               TupleDataChunk &chunk, bool release_heap) {
	D_ASSERT(this == segment.allocator.get());
	ReleaseOrStoreHandlesInternal(segment, segment.pinned_row_handles, pin_state.row_handles, chunk.row_block_ids,
	                              row_blocks, pin_state.properties);
	if (!layout.AllConstant() && release_heap) {
		ReleaseOrStoreHandlesInternal(segment, segment.pinned_heap_handles, pin_state.heap_handles,
		                              chunk.heap_block_ids, heap_blocks, pin_state.properties);
	}
}

void TupleDataAllocator::ReleaseOrStoreHandles(TupleDataPinState &pin_state, TupleDataSegment &segment) {
	static TupleDataChunk DUMMY_CHUNK;
	ReleaseOrStoreHandles(pin_state, segment, DUMMY_CHUNK, true);
}

void TupleDataAllocator::ReleaseOrStoreHandlesInternal(
    TupleDataSegment &segment, unsafe_vector<BufferHandle> &pinned_handles, perfect_map_t<BufferHandle> &handles,
    const perfect_set_t &block_ids, unsafe_vector<TupleDataBlock> &blocks, TupleDataPinProperties properties) {
	bool found_handle;
	do {
		found_handle = false;
		for (auto it = handles.begin(); it != handles.end(); it++) {
			const auto block_id = it->first;
			if (block_ids.find(block_id) != block_ids.end()) {
				// still required: do not release
				continue;
			}
			switch (properties) {
			case TupleDataPinProperties::KEEP_EVERYTHING_PINNED: {
				lock_guard<mutex> guard(segment.pinned_handles_lock);
				const auto block_count = block_id + 1;
				if (block_count > pinned_handles.size()) {
					pinned_handles.resize(block_count);
				}
				pinned_handles[block_id] = std::move(it->second);
				break;
			}
			case TupleDataPinProperties::UNPIN_AFTER_DONE:
			case TupleDataPinProperties::ALREADY_PINNED:
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

BufferHandle &TupleDataAllocator::PinRowBlock(TupleDataPinState &pin_state, const TupleDataChunkPart &part) {
	const auto &row_block_index = part.row_block_index;
	auto it = pin_state.row_handles.find(row_block_index);
	if (it == pin_state.row_handles.end()) {
		D_ASSERT(row_block_index < row_blocks.size());
		auto &row_block = row_blocks[row_block_index];
		D_ASSERT(row_block.handle);
		D_ASSERT(part.row_block_offset < row_block.size);
		D_ASSERT(part.row_block_offset + part.count * layout.GetRowWidth() <= row_block.size);
		it = pin_state.row_handles.emplace(row_block_index, buffer_manager.Pin(row_block.handle)).first;
	}
	return it->second;
}

BufferHandle &TupleDataAllocator::PinHeapBlock(TupleDataPinState &pin_state, const TupleDataChunkPart &part) {
	const auto &heap_block_index = part.heap_block_index;
	auto it = pin_state.heap_handles.find(heap_block_index);
	if (it == pin_state.heap_handles.end()) {
		D_ASSERT(heap_block_index < heap_blocks.size());
		auto &heap_block = heap_blocks[heap_block_index];
		D_ASSERT(heap_block.handle);
		D_ASSERT(part.heap_block_offset < heap_block.size);
		D_ASSERT(part.heap_block_offset + part.total_heap_size <= heap_block.size);
		it = pin_state.heap_handles.emplace(heap_block_index, buffer_manager.Pin(heap_block.handle)).first;
	}
	return it->second;
}

data_ptr_t TupleDataAllocator::GetRowPointer(TupleDataPinState &pin_state, const TupleDataChunkPart &part) {
	return PinRowBlock(pin_state, part).Ptr() + part.row_block_offset;
}

data_ptr_t TupleDataAllocator::GetBaseHeapPointer(TupleDataPinState &pin_state, const TupleDataChunkPart &part) {
	return PinHeapBlock(pin_state, part).Ptr();
}

} // namespace duckdb
