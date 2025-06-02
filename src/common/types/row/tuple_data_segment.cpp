#include "duckdb/common/types/row/tuple_data_segment.hpp"

#include "duckdb/common/types/row/tuple_data_allocator.hpp"
#include "duckdb/storage/buffer/buffer_pool.hpp"

namespace duckdb {

TupleDataChunkPart::TupleDataChunkPart(mutex &lock_p) : lock(lock_p) {
}

void TupleDataChunkPart::SetHeapEmpty() {
	heap_block_index = INVALID_INDEX;
	heap_block_offset = INVALID_INDEX;
	total_heap_size = 0;
	base_heap_ptr = nullptr;
}

TupleDataChunk::TupleDataChunk() : count(0), lock(make_unsafe_uniq<mutex>()) {
}

static inline void SwapTupleDataChunk(TupleDataChunk &a, TupleDataChunk &b) noexcept {
	std::swap(a.part_ids, b.part_ids);
	std::swap(a.row_block_ids, b.row_block_ids);
	std::swap(a.heap_block_ids, b.heap_block_ids);
	std::swap(a.count, b.count);
	std::swap(a.lock, b.lock);
}

TupleDataChunk::TupleDataChunk(TupleDataChunk &&other) noexcept : count(0) {
	SwapTupleDataChunk(*this, other);
}

TupleDataChunk &TupleDataChunk::operator=(TupleDataChunk &&other) noexcept {
	SwapTupleDataChunk(*this, other);
	return *this;
}

TupleDataChunkPart &TupleDataChunk::AddPart(TupleDataSegment &segment, TupleDataChunkPart &&part) {
	count += part.count;
	row_block_ids.Insert(part.row_block_index);
	if (!segment.layout.get().AllConstant() && part.total_heap_size > 0) {
		heap_block_ids.Insert(part.heap_block_index);
	}
	part.lock = *lock;
	part_ids.Insert(UnsafeNumericCast<uint32_t>(segment.chunk_parts.size()));
	segment.chunk_parts.emplace_back(std::move(part));
	return segment.chunk_parts.back();
}

void TupleDataChunk::Verify(const TupleDataSegment &segment) const {
#ifdef D_ASSERT_IS_ENABLED
	idx_t total_count = 0;
	for (auto part_id = part_ids.Start(); part_id < part_ids.End(); part_id++) {
		total_count += segment.chunk_parts[part_id].count;
	}
	D_ASSERT(this->count == total_count);
	D_ASSERT(this->count <= STANDARD_VECTOR_SIZE);
#endif
}

void TupleDataChunk::MergeLastChunkPart(TupleDataSegment &segment) {
	if (part_ids.Size() < 2) {
		return;
	}

	auto &second_to_last = segment.chunk_parts[part_ids.End() - 2];
	auto &last = segment.chunk_parts[part_ids.End() - 1];

	auto rows_align = last.row_block_index == second_to_last.row_block_index &&
	                  last.row_block_offset ==
	                      second_to_last.row_block_offset + second_to_last.count * segment.layout.get().GetRowWidth();

	if (!rows_align) { // If rows don't align we can never merge
		return;
	}

	if (segment.layout.get().AllConstant()) { // No heap and rows align - merge
		second_to_last.count += last.count;
		if (segment.chunk_parts.size() == part_ids.End()) {
			// Can only remove if the part we're merging was the last added chunk part
			// If not, we just leave it there (no chunk will reference it anyway)
			segment.chunk_parts.pop_back();
		}
		part_ids.DecrementMax();
		return;
	}

	if (last.heap_block_index == second_to_last.heap_block_index &&
	    last.heap_block_offset == second_to_last.heap_block_index + second_to_last.total_heap_size &&
	    last.base_heap_ptr == second_to_last.base_heap_ptr) { // There is a heap and it aligns - merge
		second_to_last.total_heap_size += last.total_heap_size;
		second_to_last.count += last.count;
		if (segment.chunk_parts.size() == part_ids.End()) {
			segment.chunk_parts.pop_back(); // Same as above
		}
		part_ids.DecrementMax();
	}
}

TupleDataSegment::TupleDataSegment(shared_ptr<TupleDataAllocator> allocator_p)
    : allocator(std::move(allocator_p)), layout(allocator->GetLayout()), count(0), data_size(0) {
	// We initialize these with plenty of room so that we can avoid allocations
	static constexpr idx_t CHUNK_RESERVATION = 64;
	chunks.reserve(CHUNK_RESERVATION);
	chunk_parts.reserve(CHUNK_RESERVATION);
}

TupleDataSegment::~TupleDataSegment() {
	lock_guard<mutex> guard(pinned_handles_lock);
	if (allocator) {
		allocator->SetDestroyBufferUponUnpin(); // Prevent blocks from being added to eviction queue
	}
	pinned_row_handles.clear();
	pinned_heap_handles.clear();
	allocator.reset();
}

void SwapTupleDataSegment(TupleDataSegment &a, TupleDataSegment &b) {
	std::swap(a.allocator, b.allocator);
	std::swap(a.layout, b.layout);
	std::swap(a.chunks, b.chunks);
	std::swap(a.chunk_parts, b.chunk_parts);
	std::swap(a.count, b.count);
	std::swap(a.data_size, b.data_size);
	std::swap(a.pinned_row_handles, b.pinned_row_handles);
	std::swap(a.pinned_heap_handles, b.pinned_heap_handles);
}

TupleDataSegment::TupleDataSegment(TupleDataSegment &&other) noexcept : layout(other.layout) {
	SwapTupleDataSegment(*this, other);
}

TupleDataSegment &TupleDataSegment::operator=(TupleDataSegment &&other) noexcept {
	SwapTupleDataSegment(*this, other);
	return *this;
}

idx_t TupleDataSegment::ChunkCount() const {
	return chunks.size();
}

idx_t TupleDataSegment::SizeInBytes() const {
	return data_size;
}

void TupleDataSegment::Unpin() {
	lock_guard<mutex> guard(pinned_handles_lock);
	pinned_row_handles.clear();
	pinned_heap_handles.clear();
}

void TupleDataSegment::Verify() const {
#ifdef D_ASSERT_IS_ENABLED
	const auto &layout = allocator->GetLayout();

	idx_t total_count = 0;
	idx_t total_size = 0;
	for (const auto &chunk : chunks) {
		chunk.Verify(*this);
		total_count += chunk.count;

		total_size += chunk.count * layout.GetRowWidth();
		if (!layout.AllConstant()) {
			for (auto part_id = chunk.part_ids.Start(); part_id < chunk.part_ids.End(); part_id++) {
				total_size += chunk_parts[part_id].total_heap_size;
			}
		}
	}
	D_ASSERT(total_count == this->count);
	D_ASSERT(total_size == this->data_size);
#endif
}

void TupleDataSegment::VerifyEverythingPinned() const {
#ifdef D_ASSERT_IS_ENABLED
	D_ASSERT(pinned_row_handles.size() == allocator->RowBlockCount());
	D_ASSERT(pinned_heap_handles.size() == allocator->HeapBlockCount());
#endif
}

} // namespace duckdb
