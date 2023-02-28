#include "duckdb/common/types/row/tuple_data_segment.hpp"

namespace duckdb {

TupleDataChunkPart::TupleDataChunkPart(uint32_t row_block_index_p, uint32_t row_block_offset_p,
                                       uint32_t heap_block_index_p, uint32_t heap_block_offset_p,
                                       data_ptr_t base_heap_ptr_p, idx_t last_heap_row_size_p, uint32_t count_p)
    : row_block_index(row_block_index_p), row_block_offset(row_block_offset_p), heap_block_index(heap_block_index_p),
      heap_block_offset(heap_block_offset_p), base_heap_ptr(base_heap_ptr_p), last_heap_size(last_heap_row_size_p),
      count(count_p) {
}

TupleDataChunkPart::TupleDataChunkPart(TupleDataChunkPart &&other) noexcept {
	std::swap(row_block_index, other.row_block_index);
	std::swap(heap_block_index, other.heap_block_index);
	std::swap(count, other.count);
}

TupleDataChunkPart &TupleDataChunkPart::operator=(TupleDataChunkPart &&other) noexcept {
	std::swap(row_block_index, other.row_block_index);
	std::swap(heap_block_index, other.heap_block_index);
	std::swap(count, other.count);
	return *this;
}

TupleDataChunk::TupleDataChunk() {
}

TupleDataChunk::TupleDataChunk(TupleDataChunk &&other) noexcept {
	std::swap(parts, other.parts);
	std::swap(count, other.count);
}

TupleDataChunk &TupleDataChunk::operator=(TupleDataChunk &&other) noexcept {
	std::swap(parts, other.parts);
	std::swap(count, other.count);
	return *this;
}

void TupleDataChunk::AddPart(TupleDataChunkPart &&part) {
	row_block_ids.insert(part.row_block_index);
	heap_block_ids.insert(part.heap_block_index);
	parts.emplace_back(std::move(part));
}

void TupleDataChunk::Verify() const {
#ifdef DEBUG
	idx_t total_count = 0;
	for (const auto &part : parts) {
		total_count += part.count;
	}
	D_ASSERT(total_count = this->count);
	D_ASSERT(this->count <= STANDARD_VECTOR_SIZE);
#endif
}

TupleDataSegment::TupleDataSegment(shared_ptr<TupleDataAllocator> allocator_p) : allocator(allocator_p), count(0) {
}

TupleDataSegment::TupleDataSegment(TupleDataSegment &&other) noexcept {
	std::swap(allocator, other.allocator);
	std::swap(chunks, other.chunks);
}

TupleDataSegment &TupleDataSegment::operator=(TupleDataSegment &&other) noexcept {
	std::swap(allocator, other.allocator);
	std::swap(chunks, other.chunks);
	return *this;
}

idx_t TupleDataSegment::ChunkCount() const {
	return chunks.size();
}

void TupleDataSegment::Verify() const {
#ifdef DEBUG
	idx_t total_count = 0;
	for (const auto &chunk : chunks) {
		chunk.Verify();
		total_count += chunk.count;
	}
	D_ASSERT(total_count == this->count);
#endif
}

} // namespace duckdb
