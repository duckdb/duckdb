#include "duckdb/common/types/row/tuple_data_segment.hpp"

namespace duckdb {

TupleDataChunk::TupleDataChunk(uint32_t row_block_index_p, idx_t row_block_offset_p, uint32_t heap_block_index_p,
                               idx_t heap_block_offset_p, idx_t last_heap_row_size_p, uint32_t count_p)
    : row_block_index(row_block_index_p), row_block_offset(row_block_offset_p), heap_block_index(heap_block_index_p),
      heap_block_offset(heap_block_offset_p), last_heap_row_size(last_heap_row_size_p), count(count_p) {
}

TupleDataChunk::TupleDataChunk(TupleDataChunk &&other) noexcept {
	std::swap(row_block_index, other.row_block_index);
	std::swap(heap_block_index, other.heap_block_index);
	std::swap(count, other.count);
}

TupleDataChunk &TupleDataChunk::operator=(TupleDataChunk &&other) noexcept {
	std::swap(row_block_index, other.row_block_index);
	std::swap(heap_block_index, other.heap_block_index);
	std::swap(count, other.count);
	return *this;
}

TupleDataSegment::TupleDataSegment(shared_ptr<TupleDataAllocator> allocator_p) : allocator(allocator_p) {
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

} // namespace duckdb
