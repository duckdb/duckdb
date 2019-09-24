#include "storage/uncompressed_segment.hpp"
#include "storage/buffer_manager.hpp"
#include "common/types/vector.hpp"
#include "storage/table/append_state.hpp"

using namespace duckdb;
using namespace std;

static UncompressedSegment::append_function_t GetAppendFunction(TypeId type);

UncompressedSegment::UncompressedSegment(BufferManager &manager, TypeId type) :
	manager(manager), type(type), block_id(INVALID_BLOCK), dirty(true), tuple_count(0) {
	// transient segment
	// figure out how many vectors we want to store in this block
	this->append_function = GetAppendFunction(type);
	this->type_size = GetTypeIdSize(type);
	this->vector_size = sizeof(nullmask_t) + type_size * STANDARD_VECTOR_SIZE;
	this->max_vector_count = (BLOCK_SIZE / vector_size) + 1;
	// allocate space for the vectors
	auto block = manager.Allocate(max_vector_count * vector_size);
	this->block_id = block->block_id;
	// initialize nullmasks to 0 for all vectors
	for(index_t i = 0; i < max_vector_count; i++) {
		auto mask = (nullmask_t*) (block->buffer->data.get() + (i * vector_size));
		mask->reset();
	}
	this->versions = nullptr;
}


void UncompressedSegment::InitializeScan(UncompressedSegmentScanState &state) {
	// obtain a read lock and pin the buffer for this segment
	state.handle = manager.PinBuffer(block_id);
}

void UncompressedSegment::Scan(Transaction &transaction, UncompressedSegmentScanState &state, index_t vector_index, Vector &result) {
	auto handle = lock.GetSharedLock();

	assert(vector_index < max_vector_count);
	assert(vector_index * STANDARD_VECTOR_SIZE <= tuple_count);


	auto data = state.handle->buffer->data.get();

	auto offset = vector_index * vector_size;

	index_t count = std::min((index_t) STANDARD_VECTOR_SIZE, tuple_count - vector_index * STANDARD_VECTOR_SIZE);
	if (!versions || !versions[vector_index]) {
		// no version information: simply set up the data pointer and read the nullmask
		result.nullmask = *((nullmask_t*) (data + offset));
		memcpy(result.data, data + offset + sizeof(nullmask_t), count * type_size);
		result.count = count;
	} else {
		throw Exception("FIXME: versions not supported yet");
	}
}

index_t UncompressedSegment::Append(SegmentStatistics &stats, TransientAppendState &state, Vector &data, index_t offset, index_t count) {
	assert(data.type == type);

	index_t initial_count = tuple_count;
	while(count > 0) {
		// get the vector index of the vector to append to and see how many tuples we can append to that vector
		index_t vector_index = tuple_count / STANDARD_VECTOR_SIZE;
		if (vector_index == max_vector_count) {
			break;
		}
		index_t current_tuple_count = tuple_count - vector_index * STANDARD_VECTOR_SIZE;
		index_t append_count = std::min(STANDARD_VECTOR_SIZE - current_tuple_count, count);

		// now perform the actual append
		append_function(stats, state.handle->buffer->data.get() + vector_size * vector_index, current_tuple_count, data, offset, append_count);

		count -= append_count;
		offset += append_count;
		tuple_count += append_count;
	}
	return tuple_count - initial_count;
}

void UncompressedSegment::Update(Transaction &transaction, Vector &update, Vector &row_ids) {
	auto handle = lock.GetExclusiveLock();
	if (!versions) {
		this->versions = unique_ptr<VersionChain*[]>(new VersionChain*[max_vector_count]);
		for(index_t i = 0; i < max_vector_count; i++) {
			this->versions[i] = nullptr;
		}
	}
	throw Exception("FIXME: updates not supported yet");
}

//===--------------------------------------------------------------------===//
// Append
//===--------------------------------------------------------------------===//
template <class T> static void update_min_max(T value, T *__restrict min, T *__restrict max) {
	if (value < *min) {
		*min = value;
	}
	if (value > *max) {
		*max = value;
	}
}

template <class T>
static void append_loop_null(T *__restrict source, nullmask_t &result_nullmask, T *__restrict target, index_t target_offset, index_t offset, index_t count,
                             sel_t *__restrict sel_vector, nullmask_t &nullmask, T *__restrict min, T *__restrict max,
                             bool &has_null) {
	// null values, have to check the NULL values in the mask
	VectorOperations::Exec(
	    sel_vector, count + offset,
	    [&](index_t i, index_t k) {
		    if (nullmask[i]) {
				result_nullmask[k - offset + target_offset] = true;
			    has_null = true;
		    } else {
			    update_min_max(source[i], min, max);
			    target[k - offset + target_offset] = source[i];
		    }
	    },
	    offset);
}

template <class T>
static void append_loop_no_null(T *__restrict source, T *__restrict target, index_t target_offset, index_t offset, index_t count,
                                sel_t *__restrict sel_vector, T *__restrict min, T *__restrict max) {
	VectorOperations::Exec(
	    sel_vector, count + offset,
	    [&](index_t i, index_t k) {
		    update_min_max(source[i], min, max);
		    target[k - offset + target_offset] = source[i];
	    },
	    offset);
}

template <class T>
static void append_loop(SegmentStatistics &stats, data_ptr_t target, index_t target_offset, Vector &source, index_t offset, index_t count) {
	assert(offset + count <= source.count);
	auto ldata = (T *)source.data;
	auto result_data = (T *) (target + sizeof(nullmask_t));
	auto nullmask = (nullmask_t*) target;
	auto min = (T *)stats.minimum.get();
	auto max = (T *)stats.maximum.get();
	if (source.nullmask.any()) {
		append_loop_null<T>(ldata, *nullmask, result_data, target_offset, offset, count, source.sel_vector, source.nullmask, min, max,
		                    stats.has_null);
	} else {
		append_loop_no_null<T>(ldata, result_data, target_offset, offset, count, source.sel_vector, min, max);
	}
}

static UncompressedSegment::append_function_t GetAppendFunction(TypeId type) {
	switch (type) {
	case TypeId::BOOLEAN:
	case TypeId::TINYINT:
		return append_loop<int8_t>;
	case TypeId::SMALLINT:
		return append_loop<int16_t>;
	case TypeId::INTEGER:
		return append_loop<int32_t>;
	case TypeId::BIGINT:
		return append_loop<int64_t>;
	case TypeId::FLOAT:
		return append_loop<float>;
	case TypeId::DOUBLE:
		return append_loop<double>;
	default:
		throw NotImplementedException("Unimplemented type for uncompressed segment");
	}
}
