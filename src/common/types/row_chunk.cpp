#include "duckdb/common/types/row_chunk.hpp"

#include "duckdb/common/types/null_value.hpp"

namespace duckdb {

RowChunk::RowChunk(BufferManager &buffer_manager)
    : buffer_manager(buffer_manager), entry_size(0), block_capacity(0), count(0) {
}

template <class T>
static void templated_serialize_vdata(VectorData &vdata, const SelectionVector &sel, idx_t count,
                                      data_ptr_t key_locations[]) {
	auto source = (T *)vdata.data;
	if (vdata.nullmask->any()) {
		for (idx_t i = 0; i < count; i++) {
			auto idx = sel.get_index(i);
			auto source_idx = vdata.sel->get_index(idx);

			auto target = (T *)key_locations[i];
			T value = (*vdata.nullmask)[source_idx] ? NullValue<T>() : source[source_idx];
			Store<T>(value, (data_ptr_t)target);
			key_locations[i] += sizeof(T);
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			auto idx = sel.get_index(i);
			auto source_idx = vdata.sel->get_index(idx);

			auto target = (T *)key_locations[i];
			Store<T>(source[source_idx], (data_ptr_t)target);
			key_locations[i] += sizeof(T);
		}
	}
}

void RowChunk::SerializeVectorData(VectorData &vdata, PhysicalType type, const SelectionVector &sel, idx_t count,
                                   data_ptr_t key_locations[]) {
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		templated_serialize_vdata<int8_t>(vdata, sel, count, key_locations);
		break;
	case PhysicalType::INT16:
		templated_serialize_vdata<int16_t>(vdata, sel, count, key_locations);
		break;
	case PhysicalType::INT32:
		templated_serialize_vdata<int32_t>(vdata, sel, count, key_locations);
		break;
	case PhysicalType::INT64:
		templated_serialize_vdata<int64_t>(vdata, sel, count, key_locations);
		break;
	case PhysicalType::UINT8:
		templated_serialize_vdata<uint8_t>(vdata, sel, count, key_locations);
		break;
	case PhysicalType::UINT16:
		templated_serialize_vdata<uint16_t>(vdata, sel, count, key_locations);
		break;
	case PhysicalType::UINT32:
		templated_serialize_vdata<uint32_t>(vdata, sel, count, key_locations);
		break;
	case PhysicalType::UINT64:
		templated_serialize_vdata<uint64_t>(vdata, sel, count, key_locations);
		break;
	case PhysicalType::INT128:
		templated_serialize_vdata<hugeint_t>(vdata, sel, count, key_locations);
		break;
	case PhysicalType::FLOAT:
		templated_serialize_vdata<float>(vdata, sel, count, key_locations);
		break;
	case PhysicalType::DOUBLE:
		templated_serialize_vdata<double>(vdata, sel, count, key_locations);
		break;
	case PhysicalType::HASH:
		templated_serialize_vdata<hash_t>(vdata, sel, count, key_locations);
		break;
	case PhysicalType::INTERVAL:
		templated_serialize_vdata<interval_t>(vdata, sel, count, key_locations);
		break;
	case PhysicalType::VARCHAR: {
		StringHeap local_heap;
		auto source = (string_t *)vdata.data;
		for (idx_t i = 0; i < count; i++) {
			auto idx = sel.get_index(i);
			auto source_idx = vdata.sel->get_index(idx);

			string_t new_val;
			if ((*vdata.nullmask)[source_idx]) {
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

void RowChunk::SerializeVector(Vector &v, idx_t vcount, const SelectionVector &sel, idx_t count,
                               data_ptr_t key_locations[]) {
	VectorData vdata;
	v.Orrify(vcount, vdata);

	SerializeVectorData(vdata, v.type.InternalType(), sel, count, key_locations);
}

idx_t RowChunk::AppendToBlock(RowDataBlock &block, BufferHandle &handle, vector<BlockAppendEntry> &append_entries,
                              idx_t remaining) {
	idx_t append_count = MinValue<idx_t>(remaining, block.capacity - block.count);
	auto dataptr = handle.node->buffer + block.count * entry_size;
	append_entries.push_back(BlockAppendEntry(dataptr, append_count));
	block.count += append_count;
	return append_count;
}

void RowChunk::Build(idx_t added_count, data_ptr_t *key_locations) {
	count += added_count;

	vector<unique_ptr<BufferHandle>> handles;
	vector<BlockAppendEntry> append_entries;
	// first allocate space of where to serialize the chunk and payload columns
	idx_t remaining = added_count;
	{
		// first append to the last block (if any)
		lock_guard<mutex> append_lock(rc_lock);
		if (blocks.size() != 0) {
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
			auto block = buffer_manager.RegisterMemory(block_capacity * entry_size, false);
			auto handle = buffer_manager.Pin(block);

			RowDataBlock new_block;
			new_block.count = 0;
			new_block.capacity = block_capacity;
			new_block.block = move(block);

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
static void templated_deserialize_into_vector(VectorData &vdata, idx_t count, data_ptr_t key_locations[]) {
	auto target = (T *)vdata.data;
	for (idx_t i = 0; i < count; i++) {
        target[i * sizeof(T)] = Load<T>(key_locations[i]);
	}
}

void RowChunk::DeserializeIntoVectorData(VectorData &vdata, PhysicalType type, idx_t count, data_ptr_t key_locations[]) {
    switch (type) {
    case PhysicalType::BOOL:
    case PhysicalType::INT8:
        templated_deserialize_into_vector<int8_t>(vdata, count, key_locations);
        break;
    case PhysicalType::INT16:
        templated_deserialize_into_vector<int16_t>(vdata, count, key_locations);
        break;
    case PhysicalType::INT32:
        templated_deserialize_into_vector<int32_t>(vdata, count, key_locations);
        break;
    case PhysicalType::INT64:
        templated_deserialize_into_vector<int64_t>(vdata, count, key_locations);
        break;
    case PhysicalType::UINT8:
        templated_deserialize_into_vector<uint8_t>(vdata, count, key_locations);
        break;
    case PhysicalType::UINT16:
        templated_deserialize_into_vector<uint16_t>(vdata, count, key_locations);
        break;
    case PhysicalType::UINT32:
        templated_deserialize_into_vector<uint32_t>(vdata, count, key_locations);
        break;
    case PhysicalType::UINT64:
        templated_deserialize_into_vector<uint64_t>(vdata, count, key_locations);
        break;
    case PhysicalType::INT128:
        templated_deserialize_into_vector<hugeint_t>(vdata, count, key_locations);
        break;
    case PhysicalType::FLOAT:
        templated_deserialize_into_vector<float>(vdata, count, key_locations);
        break;
    case PhysicalType::DOUBLE:
        templated_deserialize_into_vector<double>(vdata, count, key_locations);
        break;
    case PhysicalType::HASH:
        templated_deserialize_into_vector<hash_t>(vdata, count, key_locations);
        break;
    case PhysicalType::INTERVAL:
        templated_deserialize_into_vector<interval_t>(vdata, count, key_locations);
        break;
    case PhysicalType::VARCHAR: {
		// TODO:

//        StringHeap local_heap;
//        auto source = (string_t *)vdata.data;
//        for (idx_t i = 0; i < count; i++) {
//            auto idx = sel.get_index(i);
//            auto source_idx = vdata.sel->get_index(idx);
//
//            string_t new_val;
//            if ((*vdata.nullmask)[source_idx]) {
//                new_val = NullValue<string_t>();
//            } else if (source[source_idx].IsInlined()) {
//                new_val = source[source_idx];
//            } else {
//                new_val = local_heap.AddBlob(source[source_idx].GetDataUnsafe(), source[source_idx].GetSize());
//            }
//            Store<string_t>(new_val, key_locations[i]);
//            key_locations[i] += sizeof(string_t);
//        }
//        lock_guard<mutex> append_lock(rc_lock);
//        string_heap.MergeHeap(local_heap);
//        break;
    }
    default:
        throw NotImplementedException("FIXME: unimplemented deserialize");
    }
}

void RowChunk::DeserializeIntoVector(Vector &v, idx_t count, data_ptr_t key_locations[]) {
    VectorData vdata;
	v.Orrify(count, vdata);

	DeserializeIntoVectorData(vdata, v.type.InternalType(), count, key_locations);
}

void RowChunk::DeserializeRowBlock(DataChunk &chunk, RowDataBlock &block, idx_t entry) {
    data_ptr_t key_locations[STANDARD_VECTOR_SIZE];
	
    auto handle = buffer_manager.Pin(block.block);
    auto dataptr = handle->node->buffer;

    // fetch the next vector of entries from the blocks
    idx_t next = MinValue<idx_t>(STANDARD_VECTOR_SIZE, block.count - entry);
    for (idx_t i = 0; i < next; i++) {
        key_locations[i] = dataptr;
        dataptr += entry_size;
    }
    // now insert into the data chunk
	chunk.SetCardinality(next);
    for (idx_t i = 0; i < types.size(); i++) {
        DeserializeIntoVector(chunk.data[i], next, key_locations);
    }
}

} // namespace duckdb
