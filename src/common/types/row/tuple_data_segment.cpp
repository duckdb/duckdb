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

void SwapTupleDataChunkPart(TupleDataChunkPart &a, TupleDataChunkPart &b) {
	std::swap(a.row_block_index, b.row_block_index);
	std::swap(a.row_block_offset, b.row_block_offset);
	std::swap(a.heap_block_index, b.heap_block_index);
	std::swap(a.heap_block_offset, b.heap_block_offset);
	std::swap(a.base_heap_ptr, b.base_heap_ptr);
	std::swap(a.total_heap_size, b.total_heap_size);
	std::swap(a.count, b.count);
	std::swap(a.lock, b.lock);
}

TupleDataChunkPart::TupleDataChunkPart(TupleDataChunkPart &&other) noexcept : lock((other.lock)) {
	SwapTupleDataChunkPart(*this, other);
}

TupleDataChunkPart &TupleDataChunkPart::operator=(TupleDataChunkPart &&other) noexcept {
	SwapTupleDataChunkPart(*this, other);
	return *this;
}

TupleDataChunk::TupleDataChunk() : count(0), lock(make_unsafe_uniq<mutex>()) {
	parts.reserve(2);
}

static inline void SwapTupleDataChunk(TupleDataChunk &a, TupleDataChunk &b) noexcept {
	std::swap(a.parts, b.parts);
	std::swap(a.row_block_ids, b.row_block_ids);
	std::swap(a.heap_block_ids, b.heap_block_ids);
	std::swap(a.count, b.count);
	std::swap(a.lock, b.lock);
}

TupleDataChunk::TupleDataChunk(TupleDataChunk &&other) noexcept {
	SwapTupleDataChunk(*this, other);
}

TupleDataChunk &TupleDataChunk::operator=(TupleDataChunk &&other) noexcept {
	SwapTupleDataChunk(*this, other);
	return *this;
}

void TupleDataChunk::AddPart(TupleDataChunkPart &&part, const TupleDataLayout &layout) {
	count += part.count;
	row_block_ids.insert(part.row_block_index);
	if (!layout.AllConstant() && part.total_heap_size > 0) {
		heap_block_ids.insert(part.heap_block_index);
	}
	part.lock = *lock;
	parts.emplace_back(std::move(part));
}

void TupleDataChunk::Verify() const {
#ifdef DEBUG
	idx_t total_count = 0;
	for (const auto &part : parts) {
		total_count += part.count;
	}
	D_ASSERT(this->count == total_count);
	D_ASSERT(this->count <= STANDARD_VECTOR_SIZE);
#endif
}

void TupleDataChunk::MergeLastChunkPart(const TupleDataLayout &layout) {
	if (parts.size() < 2) {
		return;
	}

	auto &second_to_last = parts[parts.size() - 2];
	auto &last = parts[parts.size() - 1];

	auto rows_align =
	    last.row_block_index == second_to_last.row_block_index &&
	    last.row_block_offset == second_to_last.row_block_offset + second_to_last.count * layout.GetRowWidth();

	if (!rows_align) { // If rows don't align we can never merge
		return;
	}

	if (layout.AllConstant()) { // No heap and rows align - merge
		second_to_last.count += last.count;
		parts.pop_back();
		return;
	}

	if (last.heap_block_index == second_to_last.heap_block_index &&
	    last.heap_block_offset == second_to_last.heap_block_index + second_to_last.total_heap_size &&
	    last.base_heap_ptr == second_to_last.base_heap_ptr) { // There is a heap and it aligns - merge
		second_to_last.total_heap_size += last.total_heap_size;
		second_to_last.count += last.count;
		parts.pop_back();
	}
}

TupleDataSegment::TupleDataSegment(shared_ptr<TupleDataAllocator> allocator_p)
    : allocator(std::move(allocator_p)), count(0), data_size(0) {
}

TupleDataSegment::~TupleDataSegment() {
	lock_guard<mutex> guard(pinned_handles_lock);
	if (allocator) {
		allocator->SetDestroyBufferUponUnpin(); // Prevent blocks from being added to eviction queue
	}
	pinned_row_handles.clear();
	pinned_heap_handles.clear();
	if (Allocator::SupportsFlush() && allocator &&
	    data_size > allocator->GetBufferManager().GetBufferPool().GetAllocatorBulkDeallocationFlushThreshold()) {
		Allocator::FlushAll();
	}
	allocator.reset();
}

void SwapTupleDataSegment(TupleDataSegment &a, TupleDataSegment &b) {
	std::swap(a.allocator, b.allocator);
	std::swap(a.chunks, b.chunks);
	std::swap(a.count, b.count);
	std::swap(a.data_size, b.data_size);
	std::swap(a.pinned_row_handles, b.pinned_row_handles);
	std::swap(a.pinned_heap_handles, b.pinned_heap_handles);
}

TupleDataSegment::TupleDataSegment(TupleDataSegment &&other) noexcept {
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
#ifdef DEBUG
	const auto &layout = allocator->GetLayout();

	idx_t total_count = 0;
	idx_t total_size = 0;
	for (const auto &chunk : chunks) {
		chunk.Verify();
		total_count += chunk.count;

		total_size += chunk.count * layout.GetRowWidth();
		if (!layout.AllConstant()) {
			for (const auto &part : chunk.parts) {
				total_size += part.total_heap_size;
			}
		}
	}
	D_ASSERT(total_count == this->count);
	D_ASSERT(total_size == this->data_size);
#endif
}

void TupleDataSegment::VerifyEverythingPinned() const {
#ifdef DEBUG
	D_ASSERT(pinned_row_handles.size() == allocator->RowBlockCount());
	D_ASSERT(pinned_heap_handles.size() == allocator->HeapBlockCount());
#endif
}

} // namespace duckdb
