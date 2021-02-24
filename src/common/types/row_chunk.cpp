#include "duckdb/common/types/row_chunk.hpp"

#include "duckdb/common/types/null_value.hpp"

namespace duckdb {

RowChunk::RowChunk(BufferManager &buffer_manager)
    : buffer_manager(buffer_manager), entry_size(0), block_capacity(0), count(0) {
}

template <class T>
static void TemplatedSerializeVData(VectorData &vdata, const SelectionVector &sel, idx_t count, idx_t col_idx,
                                    data_ptr_t *key_locations, data_ptr_t *nullmask_locations) {
	auto source = (T *)vdata.data;
	for (idx_t i = 0; i < count; i++) {
		auto idx = sel.get_index(i);
		auto source_idx = vdata.sel->get_index(idx);

		auto target = (T *)key_locations[i];
		Store<T>(source[source_idx], (data_ptr_t)target);
		key_locations[i] += sizeof(T);

		// set the nullmask
		*nullmask_locations[i] |= !vdata.validity.RowIsValid(i) << col_idx;
	}
}

void RowChunk::SerializeVectorData(VectorData &vdata, PhysicalType type, const SelectionVector &sel, idx_t ser_count,
                                   idx_t col_idx, data_ptr_t key_locations[], data_ptr_t nullmask_locations[]) {
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		TemplatedSerializeVData<int8_t>(vdata, sel, ser_count, col_idx, key_locations, nullmask_locations);
		break;
	case PhysicalType::INT16:
		TemplatedSerializeVData<int16_t>(vdata, sel, ser_count, col_idx, key_locations, nullmask_locations);
		break;
	case PhysicalType::INT32:
		TemplatedSerializeVData<int32_t>(vdata, sel, ser_count, col_idx, key_locations, nullmask_locations);
		break;
	case PhysicalType::INT64:
		TemplatedSerializeVData<int64_t>(vdata, sel, ser_count, col_idx, key_locations, nullmask_locations);
		break;
	case PhysicalType::UINT8:
		TemplatedSerializeVData<uint8_t>(vdata, sel, ser_count, col_idx, key_locations, nullmask_locations);
		break;
	case PhysicalType::UINT16:
		TemplatedSerializeVData<uint16_t>(vdata, sel, ser_count, col_idx, key_locations, nullmask_locations);
		break;
	case PhysicalType::UINT32:
		TemplatedSerializeVData<uint32_t>(vdata, sel, ser_count, col_idx, key_locations, nullmask_locations);
		break;
	case PhysicalType::UINT64:
		TemplatedSerializeVData<uint64_t>(vdata, sel, ser_count, col_idx, key_locations, nullmask_locations);
		break;
	case PhysicalType::INT128:
		TemplatedSerializeVData<hugeint_t>(vdata, sel, ser_count, col_idx, key_locations, nullmask_locations);
		break;
	case PhysicalType::FLOAT:
		TemplatedSerializeVData<float>(vdata, sel, ser_count, col_idx, key_locations, nullmask_locations);
		break;
	case PhysicalType::DOUBLE:
		TemplatedSerializeVData<double>(vdata, sel, ser_count, col_idx, key_locations, nullmask_locations);
		break;
	case PhysicalType::HASH:
		TemplatedSerializeVData<hash_t>(vdata, sel, ser_count, col_idx, key_locations, nullmask_locations);
		break;
	case PhysicalType::INTERVAL:
		TemplatedSerializeVData<interval_t>(vdata, sel, ser_count, col_idx, key_locations, nullmask_locations);
		break;
	case PhysicalType::VARCHAR: {
		StringHeap local_heap;
		auto source = (string_t *)vdata.data;
		for (idx_t i = 0; i < ser_count; i++) {
			auto idx = sel.get_index(i);
			auto source_idx = vdata.sel->get_index(idx);

			string_t new_val;
			if ((!vdata.validity.RowIsValid(source_idx))) {
				new_val = NullValue<string_t>();
			} else if (source[source_idx].IsInlined()) {
				new_val = source[source_idx];
			} else {
				new_val = local_heap.AddBlob(source[source_idx].GetDataUnsafe(), source[source_idx].GetSize());
			}
			Store<string_t>(new_val, key_locations[i]);
			key_locations[i] += sizeof(string_t);
		}
		lock_guard<mutex> append_lock(rc_lock);
		string_heap.MergeHeap(local_heap);
		break;
	}
	default:
		throw NotImplementedException("FIXME: unimplemented serialize");
	}
}

void RowChunk::SerializeVector(Vector &v, idx_t vcount, const SelectionVector &sel, idx_t ser_count, idx_t col_idx,
                               data_ptr_t key_locations[], data_ptr_t nullmask_locations[]) {
	VectorData vdata;
	v.Orrify(vcount, vdata);

	SerializeVectorData(vdata, v.GetType().InternalType(), sel, ser_count, col_idx, key_locations, nullmask_locations);
}

idx_t RowChunk::AppendToBlock(RowDataBlock &block, BufferHandle &handle, vector<BlockAppendEntry> &append_entries,
                              idx_t remaining) {
	idx_t append_count = MinValue<idx_t>(remaining, block.capacity - block.count);
	auto dataptr = handle.node->buffer + block.count * entry_size;
	append_entries.emplace_back(dataptr, append_count);
	block.count += append_count;
	return append_count;
}

void RowChunk::Build(idx_t added_count, data_ptr_t key_locations[]) {
	count += added_count;

	vector<unique_ptr<BufferHandle>> handles;
	vector<BlockAppendEntry> append_entries;
	// first allocate space of where to serialize the chunk and payload columns
	idx_t remaining = added_count;
	{
		// first append to the last block (if any)
		lock_guard<mutex> append_lock(rc_lock);
		if (!blocks.empty()) {
			auto &last_block = blocks.back();
			if (last_block.count < last_block.capacity) {
				// last block has space: pin the buffer of this block
				auto handle = buffer_manager.Pin(last_block.block);
				// now append to the block
				idx_t append_count = AppendToBlock(last_block, *handle, append_entries, remaining);
				remaining -= append_count;
				handles.push_back(move(handle));
			}
		}
		while (remaining > 0) {
			// now for the remaining data, allocate new buffers to store the data and append there
			RowDataBlock new_block(buffer_manager, block_capacity, entry_size);
			auto handle = buffer_manager.Pin(new_block.block);

			idx_t append_count = AppendToBlock(new_block, *handle, append_entries, remaining);
			remaining -= append_count;
			handles.push_back(move(handle));
			blocks.push_back(move(new_block));
		}
	}
	// now set up the key_locations based on the append entries
	idx_t append_idx = 0;
	for (auto &append_entry : append_entries) {
		idx_t next = append_idx + append_entry.count;
		for (; append_idx < next; append_idx++) {
			key_locations[append_idx] = append_entry.baseptr;
			append_entry.baseptr += entry_size;
		}
	}
}

template <class T>
static void TemplatedDeserializeIntoVector(VectorData &vdata, idx_t count, idx_t col_idx, data_ptr_t *key_locations,
                                           data_ptr_t *nullmask_locations) {
	auto target = (T *)vdata.data;
	for (idx_t i = 0; i < count; i++) {
		target[i] = Load<T>(key_locations[i]);
		key_locations[i] += sizeof(T);
		vdata.validity.Set(i, *nullmask_locations[i] & (1 << col_idx));
	}
}

void RowChunk::DeserializeIntoVectorData(VectorData &vdata, PhysicalType type, idx_t vcount, idx_t col_idx,
                                         data_ptr_t key_locations[], data_ptr_t nullmask_locations[]) {
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		TemplatedDeserializeIntoVector<int8_t>(vdata, vcount, col_idx, key_locations, nullmask_locations);
		break;
	case PhysicalType::INT16:
		TemplatedDeserializeIntoVector<int16_t>(vdata, vcount, col_idx, key_locations, nullmask_locations);
		break;
	case PhysicalType::INT32:
		TemplatedDeserializeIntoVector<int32_t>(vdata, vcount, col_idx, key_locations, nullmask_locations);
		break;
	case PhysicalType::INT64:
		TemplatedDeserializeIntoVector<int64_t>(vdata, vcount, col_idx, key_locations, nullmask_locations);
		break;
	case PhysicalType::UINT8:
		TemplatedDeserializeIntoVector<uint8_t>(vdata, vcount, col_idx, key_locations, nullmask_locations);
		break;
	case PhysicalType::UINT16:
		TemplatedDeserializeIntoVector<uint16_t>(vdata, vcount, col_idx, key_locations, nullmask_locations);
		break;
	case PhysicalType::UINT32:
		TemplatedDeserializeIntoVector<uint32_t>(vdata, vcount, col_idx, key_locations, nullmask_locations);
		break;
	case PhysicalType::UINT64:
		TemplatedDeserializeIntoVector<uint64_t>(vdata, vcount, col_idx, key_locations, nullmask_locations);
		break;
	case PhysicalType::INT128:
		TemplatedDeserializeIntoVector<hugeint_t>(vdata, vcount, col_idx, key_locations, nullmask_locations);
		break;
	case PhysicalType::FLOAT:
		TemplatedDeserializeIntoVector<float>(vdata, vcount, col_idx, key_locations, nullmask_locations);
		break;
	case PhysicalType::DOUBLE:
		TemplatedDeserializeIntoVector<double>(vdata, vcount, col_idx, key_locations, nullmask_locations);
		break;
	case PhysicalType::HASH:
		TemplatedDeserializeIntoVector<hash_t>(vdata, vcount, col_idx, key_locations, nullmask_locations);
		break;
	case PhysicalType::INTERVAL:
		TemplatedDeserializeIntoVector<interval_t>(vdata, vcount, col_idx, key_locations, nullmask_locations);
		break;
	case PhysicalType::VARCHAR:
		TemplatedDeserializeIntoVector<string_t>(vdata, vcount, col_idx, key_locations, nullmask_locations);
		break;
	default:
		throw NotImplementedException("FIXME: unimplemented deserialize");
	}
}

void RowChunk::DeserializeIntoVector(Vector &v, idx_t vcount, idx_t col_idx, data_ptr_t key_locations[],
                                     data_ptr_t nullmask_locations[]) {
	VectorData vdata;
	v.Orrify(vcount, vdata);

	DeserializeIntoVectorData(vdata, v.GetType().InternalType(), vcount, col_idx, key_locations, nullmask_locations);
}

void RowChunk::Append(RowChunk &chunk) {
	if (count == 0) {
		// this chunk is empty - initialize with appended chunk
		entry_size = chunk.entry_size;
		block_capacity = chunk.block_capacity;
	} else {
		// not empty - assert compatibility
		D_ASSERT(entry_size == chunk.entry_size);
		D_ASSERT(block_capacity == chunk.block_capacity);
	}

	// now we append
	string_heap.MergeHeap(chunk.string_heap);
	for (auto &block : chunk.blocks) {
		count += block.count;
		blocks.push_back(block);
	}
}

} // namespace duckdb
