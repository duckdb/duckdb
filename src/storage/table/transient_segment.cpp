#include "storage/table/transient_segment.hpp"
#include "common/types/null_value.hpp"
#include "common/types/vector.hpp"
#include "common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

static void AppendToBlock(SegmentStatistics &stats, Vector &source, void *target, index_t offset,
                          index_t element_count);
static void UpdateValue(TypeId type, SegmentStatistics &stats, data_ptr_t source, data_ptr_t target);

TransientSegment::TransientSegment(TypeId type, index_t start)
    : ColumnSegment(type, ColumnSegmentType::TRANSIENT, start), block(INVALID_BLOCK) {
}

void TransientSegment::Scan(ColumnPointer &pointer, Vector &result, index_t count) {
	data_ptr_t dataptr = block.buffer + pointer.offset * type_size;
	Vector source(type, dataptr);
	source.count = count;
	VectorOperations::AppendFromStorage(source, result, stats.has_null);
	pointer.offset += count;
}

void TransientSegment::Scan(ColumnPointer &pointer, Vector &result, index_t count, sel_t *sel_vector,
                            index_t sel_count) {
	data_ptr_t dataptr = block.buffer + pointer.offset * type_size;
	Vector source(type, dataptr);
	source.count = sel_count;
	source.sel_vector = sel_vector;
	VectorOperations::AppendFromStorage(source, result, stats.has_null);
	pointer.offset += count;
}

index_t TransientSegment::Append(Vector &data, index_t offset, index_t count) {
	assert(count > 0);

	// check how many elements we can fit in this segment
	data_ptr_t target = block.buffer + this->count * type_size;
	index_t elements_to_copy = std::min(BLOCK_SIZE / type_size - this->count, count);
	if (elements_to_copy > 0) {
		// we can fit elements in the current column segment: copy them there
		AppendToBlock(stats, data, target, offset, elements_to_copy);
		this->count += elements_to_copy;
	}
	return elements_to_copy;
}

void TransientSegment::Fetch(Vector &result, index_t row_id) {
	assert(row_id >= start);
	if (row_id >= start + count) {
		assert(next);
		auto &next_segment = (ColumnSegment &)*next;
		next_segment.Fetch(result, row_id);
		return;
	}
	data_ptr_t dataptr = block.buffer + (row_id - start) * type_size;
	Vector source(type, dataptr);
	source.count = 1;
	VectorOperations::AppendFromStorage(source, result, stats.has_null);
}

void TransientSegment::Update(index_t row_id, data_ptr_t new_data) {
	assert(row_id >= start);
	if (row_id >= start + count) {
		assert(next);
		auto &next_segment = (TransientSegment &)*next;
		next_segment.Update(row_id, new_data);
		return;
	}
	data_ptr_t dataptr = block.buffer + (row_id - start) * type_size;
	UpdateValue(type, stats, new_data, dataptr);
}

template <class T> static void update_min_max(T value, T *__restrict min, T *__restrict max) {
	if (value < *min) {
		*min = value;
	}
	if (value > *max) {
		*max = value;
	}
}

template <class T>
static void append_loop_null(T *__restrict source, T *__restrict target, index_t offset, index_t count,
                             sel_t *__restrict sel_vector, nullmask_t &nullmask, T *__restrict min, T *__restrict max,
                             bool &has_null) {
	// null values, have to check the NULL values in the mask
	VectorOperations::Exec(
	    sel_vector, count + offset,
	    [&](index_t i, index_t k) {
		    if (nullmask[i]) {
			    target[k - offset] = NullValue<T>();
			    has_null = true;
		    } else {
			    update_min_max(source[i], min, max);
			    target[k - offset] = source[i];
		    }
	    },
	    offset);
}

template <class T>
static void append_loop_no_null(T *__restrict source, T *__restrict target, index_t offset, index_t count,
                                sel_t *__restrict sel_vector, T *__restrict min, T *__restrict max) {
	VectorOperations::Exec(
	    sel_vector, count + offset,
	    [&](index_t i, index_t k) {
		    update_min_max(source[i], min, max);
		    target[k - offset] = source[i];
	    },
	    offset);
}

template <class T>
static void append_loop(SegmentStatistics &stats, Vector &input, void *target, index_t offset, index_t element_count) {
	auto ldata = (T *)input.data;
	auto result_data = (T *)target;
	auto min = (T *)stats.minimum.get();
	auto max = (T *)stats.maximum.get();
	if (input.nullmask.any()) {
		append_loop_null<T>(ldata, result_data, offset, element_count, input.sel_vector, input.nullmask, min, max,
		                    stats.has_null);
	} else {
		append_loop_no_null<T>(ldata, result_data, offset, element_count, input.sel_vector, min, max);
	}
}

static void AppendToBlock(SegmentStatistics &stats, Vector &source, void *target, index_t offset,
                          index_t element_count) {
	assert(offset + element_count <= source.count);

	switch (source.type) {
	case TypeId::BOOLEAN:
	case TypeId::TINYINT:
		append_loop<int8_t>(stats, source, target, offset, element_count);
		break;
	case TypeId::SMALLINT:
		append_loop<int16_t>(stats, source, target, offset, element_count);
		break;
	case TypeId::INTEGER:
		append_loop<int32_t>(stats, source, target, offset, element_count);
		break;
	case TypeId::BIGINT:
		append_loop<int64_t>(stats, source, target, offset, element_count);
		break;
	case TypeId::FLOAT:
		append_loop<float>(stats, source, target, offset, element_count);
		break;
	case TypeId::DOUBLE:
		append_loop<double>(stats, source, target, offset, element_count);
		break;
	case TypeId::VARCHAR:
		append_loop<const char *>(stats, source, target, offset, element_count);
		break;
	default:
		throw NotImplementedException("Unimplemented type for append");
	}
}

template <class T> static void update_value(SegmentStatistics &stats, data_ptr_t source_data, data_ptr_t target_data) {
	auto source = (T *)source_data;
	auto target = (T *)target_data;
	auto min = (T *)stats.minimum.get();
	auto max = (T *)stats.maximum.get();
	if (IsNullValue<T>(*source)) {
		stats.has_null = true;
	}
	update_min_max(*source, min, max);

	*target = *source;
}

static void UpdateValue(TypeId type, SegmentStatistics &stats, data_ptr_t source, data_ptr_t target) {
	switch (type) {
	case TypeId::BOOLEAN:
	case TypeId::TINYINT:
		update_value<int8_t>(stats, source, target);
		break;
	case TypeId::SMALLINT:
		update_value<int16_t>(stats, source, target);
		break;
	case TypeId::INTEGER:
		update_value<int32_t>(stats, source, target);
		break;
	case TypeId::BIGINT:
		update_value<int64_t>(stats, source, target);
		break;
	case TypeId::FLOAT:
		update_value<float>(stats, source, target);
		break;
	case TypeId::DOUBLE:
		update_value<double>(stats, source, target);
		break;
	case TypeId::VARCHAR:
		update_value<const char *>(stats, source, target);
		break;
	default:
		throw NotImplementedException("Unimplemented type for append");
	}
}
