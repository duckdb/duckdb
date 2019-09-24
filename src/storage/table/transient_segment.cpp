#include "storage/table/transient_segment.hpp"
#include "common/types/null_value.hpp"
#include "common/types/vector.hpp"
#include "common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

TransientSegment::TransientSegment(BufferManager &manager, TypeId type, index_t start)
    : ColumnSegment(type, ColumnSegmentType::TRANSIENT, start), manager(manager), data(manager, type) {
}

void TransientSegment::InitializeScan(TransientScanState &state) {
	// initialize the uncompressed segment scan state
	data.InitializeScan(state.state);
}

void TransientSegment::Scan(Transaction &transaction, TransientScanState &state, Vector &result) {
	data.Scan(transaction, state.state, state.vector_index, result);
}

void TransientSegment::InitializeAppend(TransientAppendState &state) {
	state.lock = data.lock.GetExclusiveLock();
	state.handle = manager.PinBuffer(data.block_id);
}

index_t TransientSegment::Append(TransientAppendState &state, Vector &append_data, index_t offset, index_t count) {
	index_t appended = data.Append(stats, state, append_data, offset, count);
	count += appended;
	return appended;
}

// void TransientSegment::Scan(ColumnPointer &pointer, Vector &result, index_t count) {
// 	auto handle = manager.PinBuffer(block_id);

// 	data_ptr_t dataptr = handle->buffer->data.get() + pointer.offset * type_size;
// 	Vector source(type, dataptr);
// 	source.count = count;
// 	VectorOperations::AppendFromStorage(source, result, stats.has_null);
// 	pointer.offset += count;
// }

// index_t TransientSegment::Append(Vector &data, index_t offset, index_t count) {
// 	assert(count > 0);

// 	auto handle = manager.PinBuffer(block_id);

// 	// check how many elements we can fit in this segment
// 	data_ptr_t target = handle->buffer->data.get() + this->count * type_size;
// 	index_t elements_to_copy = std::min(vector_count * STANDARD_VECTOR_SIZE - this->count, count);
// 	if (elements_to_copy > 0) {
// 		// we can fit elements in the current column segment: copy them there
// 		AppendToBlock(stats, data, target, offset, elements_to_copy);
// 		this->count += elements_to_copy;
// 	}
// 	return elements_to_copy;
// }

// void TransientSegment::Fetch(Vector &result, index_t row_id) {
// 	assert(row_id >= start);

// 	auto handle = manager.PinBuffer(block_id);
// 	if (row_id >= start + count) {
// 		assert(next);
// 		auto &next_segment = (ColumnSegment &)*next;
// 		next_segment.Fetch(result, row_id);
// 		return;
// 	}
// 	data_ptr_t dataptr = handle->buffer->data.get() + (row_id - start) * type_size;
// 	Vector source(type, dataptr);
// 	source.count = 1;
// 	VectorOperations::AppendFromStorage(source, result, stats.has_null);
// }

// void TransientSegment::Update(index_t row_id, data_ptr_t new_data) {
// 	assert(row_id >= start);

// 	auto handle = manager.PinBuffer(block_id);
// 	if (row_id >= start + count) {
// 		assert(next);
// 		auto &next_segment = (TransientSegment &)*next;
// 		next_segment.Update(row_id, new_data);
// 		return;
// 	}
// 	data_ptr_t dataptr = handle->buffer->data.get() + (row_id - start) * type_size;
// 	UpdateValue(type, stats, new_data, dataptr);
// }

// template <class T> static void update_min_max(T value, T *__restrict min, T *__restrict max) {
// 	if (value < *min) {
// 		*min = value;
// 	}
// 	if (value > *max) {
// 		*max = value;
// 	}
// }

// template <class T> static void update_value(SegmentStatistics &stats, data_ptr_t source_data, data_ptr_t target_data) {
// 	auto source = (T *)source_data;
// 	auto target = (T *)target_data;
// 	auto min = (T *)stats.minimum.get();
// 	auto max = (T *)stats.maximum.get();
// 	if (IsNullValue<T>(*source)) {
// 		stats.has_null = true;
// 	}
// 	update_min_max(*source, min, max);

// 	*target = *source;
// }

// static void UpdateValue(TypeId type, SegmentStatistics &stats, data_ptr_t source, data_ptr_t target) {
// 	switch (type) {
// 	case TypeId::BOOLEAN:
// 	case TypeId::TINYINT:
// 		update_value<int8_t>(stats, source, target);
// 		break;
// 	case TypeId::SMALLINT:
// 		update_value<int16_t>(stats, source, target);
// 		break;
// 	case TypeId::INTEGER:
// 		update_value<int32_t>(stats, source, target);
// 		break;
// 	case TypeId::BIGINT:
// 		update_value<int64_t>(stats, source, target);
// 		break;
// 	case TypeId::FLOAT:
// 		update_value<float>(stats, source, target);
// 		break;
// 	case TypeId::DOUBLE:
// 		update_value<double>(stats, source, target);
// 		break;
// 	case TypeId::VARCHAR:
// 		update_value<const char *>(stats, source, target);
// 		break;
// 	default:
// 		throw NotImplementedException("Unimplemented type for append");
// 	}
// }
