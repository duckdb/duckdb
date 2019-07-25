#include "storage/table/transient_segment.hpp"

using namespace duckdb;
using namespace std;

TransientSegment::TransientSegment(TypeId type, index_t start) :
	ColumnSegment(type, ColumnSegmentType::TRANSIENT, start), block(INVALID_BLOCK) {

}

void TransientSegment::Scan(ColumnPointer &pointer, Vector &result, index_t count) {
	data_ptr_t dataptr = block.buffer + pointer.offset * type_size;
	Vector source(type, dataptr);
	source.count = count;
	// FIXME: use ::Copy instead of ::AppendFromStorage if there are no null values in this segment
	VectorOperations::AppendFromStorage(source, result);
	pointer.offset += count;
}

void TransientSegment::Scan(ColumnPointer &pointer, Vector &result, index_t count, sel_t *sel_vector, index_t sel_count) {
	data_ptr_t dataptr = block.buffer + pointer.offset * type_size;
	Vector source(type, dataptr);
	source.count = sel_count;
	source.sel_vector = sel_vector;
	// FIXME: use ::Copy instead of ::AppendFromStorage if there are no null values in this segment
	VectorOperations::AppendFromStorage(source, result);
	pointer.offset += count;
}

index_t TransientSegment::Append(Vector &data, index_t offset, index_t count) {
	assert(count > 0);

	// check how many elements we can fit in this segment
	data_ptr_t target = block.buffer + this->count * type_size;
	index_t elements_to_copy = std::min(BLOCK_SIZE / type_size - this->count, count);
	if (elements_to_copy > 0) {
		// we can fit elements in the current column segment: copy them there
		VectorOperations::CopyToStorage(data, target, offset, elements_to_copy);
		this->count += elements_to_copy;
	}
	return elements_to_copy;
}

void TransientSegment::Fetch(Vector &result, index_t row_id) {
	assert(row_id >= start);
	if (row_id >= start + count) {
		assert(next);
		auto &next_segment = (ColumnSegment&) *next;
		next_segment.Fetch(result, row_id);
		return;
	}
	data_ptr_t dataptr = block.buffer + (row_id - start) * type_size;
	Vector source(type, dataptr);
	source.count = 1;
	VectorOperations::AppendFromStorage(source, result);
}

void TransientSegment::Update(index_t row_id, data_ptr_t new_data) {
	assert(row_id >= start);
	if (row_id >= start + count) {
		assert(next);
		auto &next_segment = (TransientSegment&) *next;
		next_segment.Update(row_id, new_data);
		return;
	}
	data_ptr_t dataptr = block.buffer + (row_id - start) * type_size;
	memcpy(dataptr, new_data, type_size);
}
