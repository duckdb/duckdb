#include "duckdb/common/types/row_chunk.hpp"

#include "duckdb/common/types/null_value.hpp"

namespace duckdb {

RowChunk::RowChunk(BufferManager &buffer_manager) : buffer_manager(buffer_manager), block_capacity(0) {
}

template <class T>
static void TemplatedSerializeVData(VectorData &vdata, const SelectionVector &sel, idx_t count, idx_t col_idx,
                                    data_ptr_t *key_locations, data_ptr_t *validitymask_locations) {
	auto source = (T *)vdata.data;
    auto byte_offset = col_idx / 8;
    auto offset_in_byte = col_idx % 8;
	for (idx_t i = 0; i < count; i++) {
		auto idx = sel.get_index(i);
		auto source_idx = vdata.sel->get_index(idx);

		auto target = (T *)key_locations[i];
		Store<T>(source[source_idx], (data_ptr_t)target);
		key_locations[i] += sizeof(T);

		// set the validitymask
		int validity_bit = vdata.validity.RowIsValid(i) ? 1 : 0;
		*(validitymask_locations[i] + byte_offset) |= validity_bit << offset_in_byte;
	}
}

void RowChunk::SerializeVectorData(VectorData &vdata, PhysicalType type, const SelectionVector &sel, idx_t ser_count,
                                   idx_t col_idx, data_ptr_t key_locations[], data_ptr_t validitymask_locations[]) {
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		TemplatedSerializeVData<int8_t>(vdata, sel, ser_count, col_idx, key_locations, validitymask_locations);
		break;
	case PhysicalType::INT16:
		TemplatedSerializeVData<int16_t>(vdata, sel, ser_count, col_idx, key_locations, validitymask_locations);
		break;
	case PhysicalType::INT32:
		TemplatedSerializeVData<int32_t>(vdata, sel, ser_count, col_idx, key_locations, validitymask_locations);
		break;
	case PhysicalType::INT64:
		TemplatedSerializeVData<int64_t>(vdata, sel, ser_count, col_idx, key_locations, validitymask_locations);
		break;
	case PhysicalType::UINT8:
		TemplatedSerializeVData<uint8_t>(vdata, sel, ser_count, col_idx, key_locations, validitymask_locations);
		break;
	case PhysicalType::UINT16:
		TemplatedSerializeVData<uint16_t>(vdata, sel, ser_count, col_idx, key_locations, validitymask_locations);
		break;
	case PhysicalType::UINT32:
		TemplatedSerializeVData<uint32_t>(vdata, sel, ser_count, col_idx, key_locations, validitymask_locations);
		break;
	case PhysicalType::UINT64:
		TemplatedSerializeVData<uint64_t>(vdata, sel, ser_count, col_idx, key_locations, validitymask_locations);
		break;
	case PhysicalType::INT128:
		TemplatedSerializeVData<hugeint_t>(vdata, sel, ser_count, col_idx, key_locations, validitymask_locations);
		break;
	case PhysicalType::FLOAT:
		TemplatedSerializeVData<float>(vdata, sel, ser_count, col_idx, key_locations, validitymask_locations);
		break;
	case PhysicalType::DOUBLE:
		TemplatedSerializeVData<double>(vdata, sel, ser_count, col_idx, key_locations, validitymask_locations);
		break;
	case PhysicalType::HASH:
		TemplatedSerializeVData<hash_t>(vdata, sel, ser_count, col_idx, key_locations, validitymask_locations);
		break;
	case PhysicalType::INTERVAL:
		TemplatedSerializeVData<interval_t>(vdata, sel, ser_count, col_idx, key_locations, validitymask_locations);
		break;
	case PhysicalType::VARCHAR: {
		auto strings = (string_t *)vdata.data;
        auto byte_offset = col_idx / 8;
        auto offset_in_byte = col_idx % 8;
		for (idx_t i = 0; i < ser_count; i++) {
			auto &string_entry = strings[vdata.sel->get_index(i)];

			// store string size
			Store<idx_t>(string_entry.GetSize(), key_locations[i]);
			key_locations[i] += string_t::PREFIX_LENGTH;

			// store the string
			memcpy(key_locations[i], string_entry.GetDataUnsafe(), string_entry.GetSize());
			key_locations[i] += string_entry.GetSize();

            // set the validitymask
            int validity_bit = vdata.validity.RowIsValid(i) ? 1 : 0;
            *(validitymask_locations[i] + byte_offset) |= validity_bit << offset_in_byte;
		}
		break;
	}
	default:
		throw NotImplementedException("FIXME: unimplemented serialize");
	}
}

void RowChunk::SerializeVector(Vector &v, idx_t vcount, const SelectionVector &sel, idx_t ser_count, idx_t col_idx,
                               data_ptr_t key_locations[], data_ptr_t validitymask_locations[]) {
	VectorData vdata;
	v.Orrify(vcount, vdata);

	SerializeVectorData(vdata, v.GetType().InternalType(), sel, ser_count, col_idx, key_locations, validitymask_locations);
}

idx_t RowChunk::AppendToBlock(RowDataBlock &block, BufferHandle &handle, vector<BlockAppendEntry> &append_entries,
                              idx_t remaining, idx_t entry_sizes[], BufferHandle *endings_handle) {
	// find out how many entries
	idx_t append_count = 0;
	idx_t append_bytes = 0;
	if (block.constant_entry_size) {
		append_count = MinValue<idx_t>(remaining, block.entry_capacity - block.count);
		append_bytes = append_count * block.constant_entry_size;
	} else {
		const idx_t remaining_capacity = block.byte_capacity - block.byte_offset;
		for (idx_t i = 0; i < remaining; i++) {
			if (block.byte_offset + append_bytes + entry_sizes[i] > remaining_capacity) {
				break;
			}
			append_count++;
			append_bytes += entry_sizes[i];
		}
	}
	auto dataptr = handle.node->buffer + block.byte_offset;
	if (block.constant_entry_size) {
		append_entries.emplace_back(dataptr, append_count, nullptr);
	} else {
		idx_t *entry_positions;
		if (block.count > 0) {
			entry_positions = ((idx_t *)endings_handle->node->buffer) + block.count;
		} else {
            entry_positions = ((idx_t *)endings_handle->node->buffer) + block.count;
			*entry_positions = 0;
			entry_positions++;
		}
		append_entries.emplace_back(dataptr, append_count, entry_positions);
	}
	block.count += append_count;
	block.byte_offset += append_bytes;
	return append_count;
}

void RowChunk::Build(idx_t added_count, data_ptr_t key_locations[], idx_t entry_sizes[],
                     const idx_t &constant_entry_size, const idx_t &positions_blocksize) {
	vector<unique_ptr<BufferHandle>> handles;
	vector<BlockAppendEntry> append_entries;
	// first allocate space of where to serialize the chunk and payload columns
	idx_t remaining = added_count;
	{
		// first append to the last block (if any)
		lock_guard<mutex> append_lock(rc_lock);
		if (!blocks.empty()) {
			auto &last_block = blocks.back();
			if (last_block.byte_offset < last_block.byte_capacity) {
				// last block has space: pin the buffer of this block
				auto handle = buffer_manager.Pin(last_block.block);
				// now append to the block
				idx_t append_count;
				if (constant_entry_size) {
					append_count = AppendToBlock(last_block, *handle, append_entries, remaining, entry_sizes, nullptr);
				} else {
					auto positions_handle = buffer_manager.Pin(last_block.entry_positions);
					append_count = AppendToBlock(last_block, *handle, append_entries, remaining, entry_sizes, positions_handle.get());
					handles.push_back(move(positions_handle));
				}
				remaining -= append_count;
				handles.push_back(move(handle));
			}
		}
		while (remaining > 0) {
			// now for the remaining data, allocate new buffers to store the data and append there
			RowDataBlock new_block(buffer_manager, block_capacity, constant_entry_size, positions_blocksize);
			auto handle = buffer_manager.Pin(new_block.block);
			// append to the new block
			idx_t append_count;
			if (constant_entry_size) {
				append_count = AppendToBlock(new_block, *handle, append_entries, remaining, entry_sizes, nullptr);
			} else {
				auto positions_handle = buffer_manager.Pin(new_block.entry_positions);
				append_count = AppendToBlock(new_block, *handle, append_entries, remaining, entry_sizes, positions_handle.get());
				handles.push_back(move(positions_handle));
			}
			remaining -= append_count;
			handles.push_back(move(handle));
			blocks.push_back(move(new_block));
		}
	}
	// now set up the key_locations based on the append entries
	idx_t append_idx = 0;
	for (auto &append_entry : append_entries) {
		idx_t next = append_idx + append_entry.count;
		if (constant_entry_size) {
			for (; append_idx < next; append_idx++) {
				key_locations[append_idx] = append_entry.baseptr;
				append_entry.baseptr += constant_entry_size;
			}
		} else {
			for (; append_idx < next; append_idx++) {
				key_locations[append_idx] = append_entry.baseptr;
				append_entry.baseptr += entry_sizes[append_idx];
				// entry positions
				*append_entry.entry_positions = *(append_entry.entry_positions - 1) + entry_sizes[append_idx];
				append_entry.entry_positions++;
			}
		}
	}
}

template <class T>
static void TemplatedDeserializeIntoVector(VectorData &vdata, idx_t count, idx_t col_idx, data_ptr_t *key_locations,
                                           data_ptr_t *validitymask_locations) {
	auto target = (T *)vdata.data;
    auto byte_offset = col_idx / 8;
    auto offset_in_byte = col_idx % 8;
	for (idx_t i = 0; i < count; i++) {
		target[i] = Load<T>(key_locations[i]);
		key_locations[i] += sizeof(T);
		vdata.validity.Set(i, *(validitymask_locations[i] + byte_offset) & (1 << offset_in_byte));
	}
}

void RowChunk::DeserializeIntoVectorData(Vector &v, VectorData &vdata, PhysicalType type, idx_t vcount, idx_t col_idx,
                                         data_ptr_t key_locations[], data_ptr_t validitymask_locations[]) {
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		TemplatedDeserializeIntoVector<int8_t>(vdata, vcount, col_idx, key_locations, validitymask_locations);
		break;
	case PhysicalType::INT16:
		TemplatedDeserializeIntoVector<int16_t>(vdata, vcount, col_idx, key_locations, validitymask_locations);
		break;
	case PhysicalType::INT32:
		TemplatedDeserializeIntoVector<int32_t>(vdata, vcount, col_idx, key_locations, validitymask_locations);
		break;
	case PhysicalType::INT64:
		TemplatedDeserializeIntoVector<int64_t>(vdata, vcount, col_idx, key_locations, validitymask_locations);
		break;
	case PhysicalType::UINT8:
		TemplatedDeserializeIntoVector<uint8_t>(vdata, vcount, col_idx, key_locations, validitymask_locations);
		break;
	case PhysicalType::UINT16:
		TemplatedDeserializeIntoVector<uint16_t>(vdata, vcount, col_idx, key_locations, validitymask_locations);
		break;
	case PhysicalType::UINT32:
		TemplatedDeserializeIntoVector<uint32_t>(vdata, vcount, col_idx, key_locations, validitymask_locations);
		break;
	case PhysicalType::UINT64:
		TemplatedDeserializeIntoVector<uint64_t>(vdata, vcount, col_idx, key_locations, validitymask_locations);
		break;
	case PhysicalType::INT128:
		TemplatedDeserializeIntoVector<hugeint_t>(vdata, vcount, col_idx, key_locations, validitymask_locations);
		break;
	case PhysicalType::FLOAT:
		TemplatedDeserializeIntoVector<float>(vdata, vcount, col_idx, key_locations, validitymask_locations);
		break;
	case PhysicalType::DOUBLE:
		TemplatedDeserializeIntoVector<double>(vdata, vcount, col_idx, key_locations, validitymask_locations);
		break;
	case PhysicalType::HASH:
		TemplatedDeserializeIntoVector<hash_t>(vdata, vcount, col_idx, key_locations, validitymask_locations);
		break;
	case PhysicalType::INTERVAL:
		TemplatedDeserializeIntoVector<interval_t>(vdata, vcount, col_idx, key_locations, validitymask_locations);
		break;
	case PhysicalType::VARCHAR: {
		idx_t len;
        auto byte_offset = col_idx / 8;
        auto offset_in_byte = col_idx % 8;
		for (idx_t i = 0; i < vcount; i++) {
			// deserialize string length
			len = Load<idx_t>(key_locations[i]);
			key_locations[i] += string_t::PREFIX_LENGTH;
			// deserialize string
			StringVector::AddString(v, (const char *)key_locations[i], len);
			key_locations[i] += len;
			// set validitymask
            vdata.validity.Set(i, *(validitymask_locations[i] + byte_offset) & (1 << offset_in_byte));
		}
		break;
	}
	default:
		throw NotImplementedException("FIXME: unimplemented deserialize");
	}
}

void RowChunk::DeserializeIntoVector(Vector &v, idx_t vcount, idx_t col_idx, data_ptr_t key_locations[],
                                     data_ptr_t validitymask_locations[]) {
	VectorData vdata;
	v.Orrify(vcount, vdata);
	DeserializeIntoVectorData(v, vdata, v.GetType().InternalType(), vcount, col_idx, key_locations, validitymask_locations);
}

} // namespace duckdb
