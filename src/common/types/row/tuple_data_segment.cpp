#include "duckdb/common/types/row/tuple_data_segment.hpp"

namespace duckdb {

TupleDataChunkPart::TupleDataChunkPart(uint32_t row_block_index_p, uint32_t row_block_offset_p,
                                       uint32_t heap_block_index_p, uint32_t heap_block_offset_p,
                                       data_ptr_t base_heap_ptr_p, uint32_t total_heap_size_p,
                                       uint32_t last_heap_size_p, uint32_t count_p)
    : row_block_index(row_block_index_p), row_block_offset(row_block_offset_p), heap_block_index(heap_block_index_p),
      heap_block_offset(heap_block_offset_p), base_heap_ptr(base_heap_ptr_p), total_heap_size(total_heap_size_p),
      last_heap_size(last_heap_size_p), count(count_p) {
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

idx_t TupleDataSegment::SizeInBytes() const {
	const auto &layout = allocator->GetLayout();
	idx_t total_size = 0;
	for (const auto &chunk : chunks) {
		total_size += chunk.count * layout.GetRowWidth();
		if (!layout.AllConstant()) {
			for (const auto &part : chunk.parts) {
				total_size += part.total_heap_size;
			}
		}
	}
	return total_size;
}

void TupleDataSegment::Combine(TupleDataSegment &other) {
	D_ASSERT(allocator.get() == other.allocator.get());

	// Merge the chunks in order - should slightly improve sequential scans
	const idx_t total_chunks = this->chunks.size() + other.chunks.size();
	vector<TupleDataChunk> combined_chunks;
	combined_chunks.reserve(total_chunks);

	auto this_it = this->chunks.begin();
	auto other_it = other.chunks.begin();
	while (combined_chunks.size() != total_chunks) {
		if (this_it == chunks.end()) {
			combined_chunks.push_back(std::move(*other_it));
			other_it++;
		} else if (other_it == other.chunks.end()) {
			combined_chunks.push_back(std::move(*this_it));
			this_it++;
		} else if (this_it->parts[0].row_block_index < other_it->parts[0].row_block_index) {
			combined_chunks.push_back(std::move(*this_it));
			this_it++;
		} else {
			combined_chunks.push_back(std::move(*other_it));
			other_it++;
		}
	}
	other.chunks.clear();
	other.count = 0;

	this->chunks = std::move(combined_chunks);
	this->count += other.count;

	this->pinned_handles.reserve(this->pinned_handles.size() + other.pinned_handles.size());
	for (auto &other_pinned_handle : other.pinned_handles) {
		this->pinned_handles.push_back(std::move(other_pinned_handle));
	}
	other.pinned_handles.clear();
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
