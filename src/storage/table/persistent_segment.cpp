#include "storage/table/persistent_segment.hpp"
#include "common/exception.hpp"
#include "common/types/vector.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include "common/types/null_value.hpp"
#include "storage/checkpoint/table_data_writer.hpp"
#include "storage/meta_block_reader.hpp"

using namespace duckdb;
using namespace std;

PersistentSegment::PersistentSegment(BlockManager &manager, block_id_t id, index_t offset, TypeId type, index_t start,
                                     index_t count)
    : ColumnSegment(type, ColumnSegmentType::PERSISTENT, start, count), manager(manager), block_id(id), offset(offset) {
	// FIXME
	stats.has_null = true;
}

void PersistentSegment::LoadBlock() {
	if (block) {
		// already loaded
		return;
	}
	// load the block
	lock_guard<mutex> lock(load_lock);
	if (block) {
		// loaded in the meantime
		return;
	}
	block = make_unique<Block>(block_id);
	manager.Read(*block);
	if (type == TypeId::VARCHAR) {
		int32_t dictionary_offset = *((int32_t *)(block->buffer + offset));
		dictionary = block->buffer + offset + dictionary_offset;
		offset += sizeof(int32_t);
		type_size = sizeof(int32_t);
	}
}

void PersistentSegment::Scan(ColumnPointer &pointer, Vector &result, index_t count) {
	LoadBlock();

	data_ptr_t dataptr = block->buffer + offset + pointer.offset * type_size;
	Vector source(type, dataptr);
	source.count = count;
	AppendFromStorage(source, result, stats.has_null);
	pointer.offset += count;
}

void PersistentSegment::Scan(ColumnPointer &pointer, Vector &result, index_t count, sel_t *sel_vector,
                             index_t sel_count) {
	LoadBlock();

	data_ptr_t dataptr = block->buffer + offset + pointer.offset * type_size;
	Vector source(type, dataptr);
	source.count = sel_count;
	source.sel_vector = sel_vector;
	AppendFromStorage(source, result, stats.has_null);
	pointer.offset += count;
}

void PersistentSegment::Fetch(Vector &result, index_t row_id) {
	LoadBlock();

	assert(row_id >= start);
	if (row_id >= start + count) {
		assert(next);
		auto &next_segment = (ColumnSegment &)*next;
		next_segment.Fetch(result, row_id);
		return;
	}
	data_ptr_t dataptr = block->buffer + (row_id - start) * type_size;
	Vector source(type, dataptr);
	source.count = 1;
	AppendFromStorage(source, result, stats.has_null);
}

template <class T, bool HAS_NULL>
static void append_function(T *__restrict source, T *__restrict target, index_t count, sel_t *__restrict sel_vector,
                            nullmask_t &nullmask, index_t right_offset) {
	target += right_offset;
	VectorOperations::Exec(sel_vector, count, [&](index_t i, index_t k) {
		target[k] = source[i];
		if (HAS_NULL && IsNullValue<T>(target[k])) {
			nullmask[right_offset + k] = true;
		}
	});
}

template <bool HAS_NULL> void PersistentSegment::AppendStrings(Vector &source, Vector &target) {
	auto offsets = (int32_t *)source.data;
	auto target_strings = (const char **)target.data;
	VectorOperations::Exec(source, [&](index_t i, index_t k) {
		const char *str_val = (const char *)(dictionary + offsets[i]);
		if (*str_val == TableDataWriter::BIG_STRING_MARKER[0]) {
			// big string, load from block if not loaded yet
			auto block_id = *((block_id_t *)(str_val + 2 * sizeof(char)));
			target_strings[target.count + k] = GetBigString(block_id);
		} else if (HAS_NULL && IsNullValue<const char *>(str_val)) {
			target.nullmask[target.count + k] = true;
		} else {
			target_strings[target.count + k] = str_val;
		}
	});
	target.count += source.count;
}

void PersistentSegment::AppendFromStorage(Vector &source, Vector &target, bool has_null) {
	if (source.type == TypeId::VARCHAR) {
		// varchar vector: load data from dictionary
		if (has_null) {
			AppendStrings<true>(source, target);
		} else {
			AppendStrings<false>(source, target);
		}
	} else {
		VectorOperations::AppendFromStorage(source, target, has_null);
	}
}

const char *PersistentSegment::GetBigString(block_id_t block_id) {
	lock_guard<mutex> lock(load_lock);

	// check if the big string was already read from disk
	auto entry = big_strings.find(block_id);
	if (entry != big_strings.end()) {
		return entry->second;
	}
	// the big string was not read yet: read it from disk
	MetaBlockReader reader(manager, block_id);
	auto read_string = reader.Read<string>();
	// add it to the string heap
	auto big_string = heap.AddString(read_string);

	// place a reference to the big string in the map
	big_strings[block_id] = big_string;

	return big_string;
}
