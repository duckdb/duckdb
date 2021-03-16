#include "duckdb/common/types/row_chunk.hpp"

#include <cfloat>
#include <limits.h>

namespace duckdb {

//! these are optimized and assume a particular byte order
#define BSWAP16(x) ((uint16_t)((((uint16_t)(x)&0xff00) >> 8) | (((uint16_t)(x)&0x00ff) << 8)))

#define BSWAP32(x)                                                                                                     \
	((uint32_t)((((uint32_t)(x)&0xff000000) >> 24) | (((uint32_t)(x)&0x00ff0000) >> 8) |                               \
	            (((uint32_t)(x)&0x0000ff00) << 8) | (((uint32_t)(x)&0x000000ff) << 24)))

#define BSWAP64(x)                                                                                                     \
	((uint64_t)((((uint64_t)(x)&0xff00000000000000ull) >> 56) | (((uint64_t)(x)&0x00ff000000000000ull) >> 40) |        \
	            (((uint64_t)(x)&0x0000ff0000000000ull) >> 24) | (((uint64_t)(x)&0x000000ff00000000ull) >> 8) |         \
	            (((uint64_t)(x)&0x00000000ff000000ull) << 8) | (((uint64_t)(x)&0x0000000000ff0000ull) << 24) |         \
	            (((uint64_t)(x)&0x000000000000ff00ull) << 40) | (((uint64_t)(x)&0x00000000000000ffull) << 56)))

RowChunk::RowChunk(BufferManager &buffer_manager, idx_t block_capacity, idx_t entry_size)
    : buffer_manager(buffer_manager), count(0), block_capacity(block_capacity), entry_size(entry_size) {
	int n = 1;
	//! little endian if true
	if (*(char *)&n == 1) {
		is_little_endian = true;
	} else {
		is_little_endian = false;
	}
}

RowChunk::RowChunk(RowChunk &other)
    : buffer_manager(other.buffer_manager), count(0), block_capacity(other.block_capacity),
      entry_size(other.entry_size), is_little_endian(other.is_little_endian) {
}

static uint8_t FlipSign(uint8_t key_byte) {
	return key_byte ^ 128;
}

static uint32_t EncodeFloat(float x) {
	uint64_t buff;

	//! zero
	if (x == 0) {
		buff = 0;
		buff |= (1u << 31);
		return buff;
	}
	//! infinity
	if (x > FLT_MAX) {
		return UINT_MAX;
	}
	//! -infinity
	if (x < -FLT_MAX) {
		return 0;
	}
	buff = Load<uint32_t>((const_data_ptr_t)&x);
	if ((buff & (1u << 31)) == 0) { //! +0 and positive numbers
		buff |= (1u << 31);
	} else {          //! negative numbers
		buff = ~buff; //! complement 1
	}

	return buff;
}

static uint64_t EncodeDouble(double x) {
	uint64_t buff;
	//! zero
	if (x == 0) {
		buff = 0;
		buff += (1ull << 63);
		return buff;
	}
	//! infinity
	if (x > DBL_MAX) {
		return ULLONG_MAX;
	}
	//! -infinity
	if (x < -DBL_MAX) {
		return 0;
	}
	buff = Load<uint64_t>((const_data_ptr_t)&x);
	if (buff < (1ull << 63)) { //! +0 and positive numbers
		buff += (1ull << 63);
	} else {          //! negative numbers
		buff = ~buff; //! complement 1
	}
	return buff;
}

template <>
void RowChunk::EncodeData(data_ptr_t dataptr, bool value) {
	Store(value ? 1 : 0, dataptr);
}

template <>
void RowChunk::EncodeData(data_ptr_t dataptr, int8_t value) {
	Store<uint8_t>(value, dataptr);
	dataptr[0] = FlipSign(dataptr[0]);
}

template <>
void RowChunk::EncodeData(data_ptr_t dataptr, int16_t value) {
	Store<uint16_t>(is_little_endian ? BSWAP16(value) : value, dataptr);
	dataptr[0] = FlipSign(dataptr[0]);
}

template <>
void RowChunk::EncodeData(data_ptr_t dataptr, int32_t value) {
	Store<uint32_t>(is_little_endian ? BSWAP32(value) : value, dataptr);
	dataptr[0] = FlipSign(dataptr[0]);
}

template <>
void RowChunk::EncodeData(data_ptr_t dataptr, int64_t value) {
	Store<uint64_t>(is_little_endian ? BSWAP64(value) : value, dataptr);
	dataptr[0] = FlipSign(dataptr[0]);
}

template <>
void RowChunk::EncodeData(data_ptr_t dataptr, uint8_t value) {
	Store<uint8_t>(value, dataptr);
}

template <>
void RowChunk::EncodeData(data_ptr_t dataptr, uint16_t value) {
	Store<uint16_t>(is_little_endian ? BSWAP16(value) : value, dataptr);
}

template <>
void RowChunk::EncodeData(data_ptr_t dataptr, uint32_t value) {
	Store<uint32_t>(is_little_endian ? BSWAP32(value) : value, dataptr);
}

template <>
void RowChunk::EncodeData(data_ptr_t dataptr, uint64_t value) {
	Store<uint64_t>(is_little_endian ? BSWAP64(value) : value, dataptr);
}

template <>
void RowChunk::EncodeData(data_ptr_t dataptr, hugeint_t value) {
	EncodeData(dataptr, value.upper);
	EncodeData(dataptr + sizeof(value.upper), value.lower);
}

template <>
void RowChunk::EncodeData(data_ptr_t dataptr, float value) {
	uint32_t converted_value = EncodeFloat(value);
	Store<uint32_t>(is_little_endian ? BSWAP32(converted_value) : converted_value, dataptr);
}

template <>
void RowChunk::EncodeData(data_ptr_t dataptr, double value) {
	uint64_t converted_value = EncodeDouble(value);
	Store<uint64_t>(is_little_endian ? BSWAP64(converted_value) : converted_value, dataptr);
}

void RowChunk::EncodeStringData(data_ptr_t dataptr, string_t value, idx_t prefix_len) {
	idx_t len = value.GetSize();
	memcpy(dataptr, value.GetDataUnsafe(), MinValue(len, prefix_len));
	if (len < prefix_len) {
		memset(dataptr + len, '\0', prefix_len - len);
	}
}

template <class T>
void RowChunk::TemplatedSerializeVectorSortable(VectorData &vdata, const SelectionVector &sel, idx_t add_count,
                                                data_ptr_t key_locations[], const bool desc, const bool has_null,
                                                const bool nulls_first) {
	auto source = (T *)vdata.data;
	if (has_null) {
		auto &validity = vdata.validity;
		const data_t valid = nulls_first ? 1 : 0;
		const data_t invalid = nulls_first ? 0 : 1;

		for (idx_t i = 0; i < add_count; i++) {
			auto idx = sel.get_index(i);
			auto source_idx = vdata.sel->get_index(idx);
			// write validity and according value
			if (validity.RowIsValid(source_idx)) {
				key_locations[i][0] = valid;
				key_locations[i]++;
				EncodeData(key_locations[i], source[source_idx]);
			} else {
				key_locations[i][0] = invalid;
				key_locations[i]++;
				memset(key_locations[i], 0, sizeof(T));
			}
			// invert bits if desc
			if (desc) {
				for (idx_t s = 0; s < sizeof(T); s++) {
					*(key_locations[i] + s) = ~*(key_locations[i] + s);
				}
			}
			key_locations[i] += sizeof(T);
		}
	} else {
		for (idx_t i = 0; i < add_count; i++) {
			auto idx = sel.get_index(i);
			auto source_idx = vdata.sel->get_index(idx);
			// write value
			EncodeData(key_locations[i], source[source_idx]);
			// invert bits if desc
			if (desc) {
				for (idx_t s = 0; s < sizeof(T); s++) {
					*(key_locations[i] + s) = ~*(key_locations[i] + s);
				}
			}
			key_locations[i] += sizeof(T);
		}
	}
}

void RowChunk::SerializeStringVectorSortable(VectorData &vdata, const SelectionVector &sel, idx_t add_count,
                                             data_ptr_t key_locations[], const bool desc, const bool has_null,
                                             const bool nulls_first, const idx_t prefix_len) {
	auto source = (string_t *)vdata.data;
	if (has_null) {
		auto &validity = vdata.validity;
		const data_t valid = nulls_first ? 1 : 0;
		const data_t invalid = nulls_first ? 0 : 1;

		for (idx_t i = 0; i < add_count; i++) {
			auto idx = sel.get_index(i);
			auto source_idx = vdata.sel->get_index(idx);
			// write validity and according value
			if (validity.RowIsValid(source_idx)) {
				key_locations[i][0] = valid;
				key_locations[i]++;
				EncodeStringData(key_locations[i], source[source_idx], prefix_len);
			} else {
				key_locations[i][0] = invalid;
				key_locations[i]++;
				memset(key_locations[i], '\0', prefix_len);
			}
			// invert bits if desc
			if (desc) {
				for (idx_t s = 0; s < prefix_len; s++) {
					*(key_locations[i] + s) = ~*(key_locations[i] + s);
				}
			}
			key_locations[i] += prefix_len;
		}
	} else {
		for (idx_t i = 0; i < add_count; i++) {
			auto idx = sel.get_index(i);
			auto source_idx = vdata.sel->get_index(idx);
			// write value
			EncodeStringData(key_locations[i], source[source_idx], prefix_len);
			// invert bits if desc
			if (desc) {
				for (idx_t s = 0; s < prefix_len; s++) {
					*(key_locations[i] + s) = ~*(key_locations[i] + s);
				}
			}
			key_locations[i] += prefix_len;
		}
	}
}

void RowChunk::SerializeVectorSortable(Vector &v, idx_t vcount, const SelectionVector &sel, idx_t ser_count,
                                       data_ptr_t key_locations[], bool desc, bool has_null, bool nulls_first,
                                       idx_t prefix_len) {
	VectorData vdata;
	v.Orrify(vcount, vdata);
	switch (v.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		TemplatedSerializeVectorSortable<int8_t>(vdata, sel, ser_count, key_locations, desc, has_null, nulls_first);
		break;
	case PhysicalType::INT16:
		TemplatedSerializeVectorSortable<int16_t>(vdata, sel, ser_count, key_locations, desc, has_null, nulls_first);
		break;
	case PhysicalType::INT32:
		TemplatedSerializeVectorSortable<int32_t>(vdata, sel, ser_count, key_locations, desc, has_null, nulls_first);
		break;
	case PhysicalType::INT64:
		TemplatedSerializeVectorSortable<int64_t>(vdata, sel, ser_count, key_locations, desc, has_null, nulls_first);
		break;
	case PhysicalType::UINT8:
		TemplatedSerializeVectorSortable<uint8_t>(vdata, sel, ser_count, key_locations, desc, has_null, nulls_first);
		break;
	case PhysicalType::UINT16:
		TemplatedSerializeVectorSortable<uint16_t>(vdata, sel, ser_count, key_locations, desc, has_null, nulls_first);
		break;
	case PhysicalType::UINT32:
		TemplatedSerializeVectorSortable<uint32_t>(vdata, sel, ser_count, key_locations, desc, has_null, nulls_first);
		break;
	case PhysicalType::UINT64:
		TemplatedSerializeVectorSortable<uint64_t>(vdata, sel, ser_count, key_locations, desc, has_null, nulls_first);
		break;
	case PhysicalType::INT128:
		TemplatedSerializeVectorSortable<hugeint_t>(vdata, sel, ser_count, key_locations, desc, has_null, nulls_first);
		break;
	case PhysicalType::FLOAT:
		TemplatedSerializeVectorSortable<float>(vdata, sel, ser_count, key_locations, desc, has_null, nulls_first);
		break;
	case PhysicalType::DOUBLE:
		TemplatedSerializeVectorSortable<double>(vdata, sel, ser_count, key_locations, desc, has_null, nulls_first);
		break;
	case PhysicalType::HASH:
		TemplatedSerializeVectorSortable<hash_t>(vdata, sel, ser_count, key_locations, desc, has_null, nulls_first);
		break;
	case PhysicalType::INTERVAL:
		TemplatedSerializeVectorSortable<interval_t>(vdata, sel, ser_count, key_locations, desc, has_null, nulls_first);
		break;
	case PhysicalType::VARCHAR:
		SerializeStringVectorSortable(vdata, sel, ser_count, key_locations, desc, has_null, nulls_first, prefix_len);
		break;
	default:
		throw NotImplementedException("FIXME: unimplemented deserialize");
	}
}

template <class T>
static void TemplatedSerializeVData(VectorData &vdata, const SelectionVector &sel, idx_t count, idx_t col_idx,
                                    data_ptr_t *key_locations, data_ptr_t *validitymask_locations) {
	auto source = (T *)vdata.data;
	if (!validitymask_locations) {
		for (idx_t i = 0; i < count; i++) {
			auto idx = sel.get_index(i);
			auto source_idx = vdata.sel->get_index(idx);

			auto target = (T *)key_locations[i];
			Store<T>(source[source_idx], (data_ptr_t)target);
			key_locations[i] += sizeof(T);
		}
	} else {
		auto byte_offset = col_idx / 8;
		auto offset_in_byte = col_idx % 8;
		for (idx_t i = 0; i < count; i++) {
			auto idx = sel.get_index(i);
			auto source_idx = vdata.sel->get_index(idx);

			auto target = (T *)key_locations[i];
			Store<T>(source[source_idx], (data_ptr_t)target);
			key_locations[i] += sizeof(T);

			// set the validitymask
			if (!vdata.validity.RowIsValid(i)) {
				*(validitymask_locations[i] + byte_offset) &= ~(1UL << offset_in_byte);
			}
		}
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
			Store<uint32_t>(string_entry.GetSize(), key_locations[i]);
			key_locations[i] += string_t::PREFIX_LENGTH;

			// store the string
			memcpy(key_locations[i], string_entry.GetDataUnsafe(), string_entry.GetSize());
			key_locations[i] += string_entry.GetSize();

			// set the validitymask
			if (!vdata.validity.RowIsValid(i)) {
				*(validitymask_locations[i] + byte_offset) &= ~(1UL << offset_in_byte);
			}
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
	SerializeVectorData(vdata, v.GetType().InternalType(), sel, ser_count, col_idx, key_locations,
	                    validitymask_locations);
}

idx_t RowChunk::AppendToBlock(RowDataBlock &block, BufferHandle &handle, vector<BlockAppendEntry> &append_entries,
                              idx_t remaining, idx_t entry_sizes[]) {
	idx_t append_count = 0;
	data_ptr_t dataptr;
	if (entry_sizes) {
		// compute how many entries fit if entry size if variable
		dataptr = handle.node->buffer + block.byte_offset;
		for (idx_t i = 0; i < remaining; i++) {
			if (block.byte_offset + entry_sizes[i] > block_capacity * entry_size) {
				break;
			}
			append_count++;
			block.byte_offset += entry_sizes[i];
		}
	} else {
		append_count = MinValue<idx_t>(remaining, block.CAPACITY - block.count);
		dataptr = handle.node->buffer + block.count * entry_size;
	}
	append_entries.emplace_back(dataptr, append_count);
	block.count += append_count;
	return append_count;
}

void RowChunk::Build(idx_t added_count, data_ptr_t key_locations[], idx_t entry_sizes[]) {
	vector<unique_ptr<BufferHandle>> handles;
	vector<BlockAppendEntry> append_entries;

	// first allocate space of where to serialize the keys and payload columns
	idx_t remaining = added_count;
	{
		// first append to the last block (if any)
		lock_guard<mutex> append_lock(rc_lock);
		count += added_count;
		if (!blocks.empty()) {
			auto &last_block = blocks.back();
			if (last_block.count < last_block.CAPACITY) {
				// last block has space: pin the buffer of this block
				auto handle = buffer_manager.Pin(last_block.block);
				// now append to the block
				idx_t append_count = AppendToBlock(last_block, *handle, append_entries, remaining, entry_sizes);
				remaining -= append_count;
				handles.push_back(move(handle));
			}
		}
		while (remaining > 0) {
			// now for the remaining data, allocate new buffers to store the data and append there
			RowDataBlock new_block(buffer_manager, block_capacity, entry_size);
			auto handle = buffer_manager.Pin(new_block.block);

			// offset the entry sizes array if we have added entries already
			idx_t *offset_entry_sizes = entry_sizes ? entry_sizes + added_count - remaining : nullptr;

			idx_t append_count = AppendToBlock(new_block, *handle, append_entries, remaining, offset_entry_sizes);
			remaining -= append_count;

			blocks.push_back(move(new_block));
			handles.push_back(move(handle));
		}
	}
	// now set up the key_locations based on the append entries
	idx_t append_idx = 0;
	for (auto &append_entry : append_entries) {
		idx_t next = append_idx + append_entry.count;
		if (entry_sizes) {
			for (; append_idx < next; append_idx++) {
				key_locations[append_idx] = append_entry.baseptr;
				append_entry.baseptr += entry_sizes[append_idx];
			}
		} else {
			for (; append_idx < next; append_idx++) {
				key_locations[append_idx] = append_entry.baseptr;
				append_entry.baseptr += entry_size;
			}
		}
	}
}

template <class T>
static void TemplatedDeserializeIntoVector(Vector &v, idx_t count, idx_t col_idx, data_ptr_t *key_locations,
                                           data_ptr_t *validitymask_locations) {
	auto target = FlatVector::GetData<T>(v);
	auto byte_offset = col_idx / 8;
	auto offset_in_byte = col_idx % 8;
	auto &validity = FlatVector::Validity(v);
	for (idx_t i = 0; i < count; i++) {
		target[i] = Load<T>(key_locations[i]);
		key_locations[i] += sizeof(T);
		validity.Set(i, *(validitymask_locations[i] + byte_offset) & (1 << offset_in_byte));
	}
}

void RowChunk::DeserializeIntoVectorData(Vector &v, PhysicalType type, idx_t vcount, idx_t col_idx,
                                         data_ptr_t key_locations[], data_ptr_t validitymask_locations[]) {
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		TemplatedDeserializeIntoVector<int8_t>(v, vcount, col_idx, key_locations, validitymask_locations);
		break;
	case PhysicalType::INT16:
		TemplatedDeserializeIntoVector<int16_t>(v, vcount, col_idx, key_locations, validitymask_locations);
		break;
	case PhysicalType::INT32:
		TemplatedDeserializeIntoVector<int32_t>(v, vcount, col_idx, key_locations, validitymask_locations);
		break;
	case PhysicalType::INT64:
		TemplatedDeserializeIntoVector<int64_t>(v, vcount, col_idx, key_locations, validitymask_locations);
		break;
	case PhysicalType::UINT8:
		TemplatedDeserializeIntoVector<uint8_t>(v, vcount, col_idx, key_locations, validitymask_locations);
		break;
	case PhysicalType::UINT16:
		TemplatedDeserializeIntoVector<uint16_t>(v, vcount, col_idx, key_locations, validitymask_locations);
		break;
	case PhysicalType::UINT32:
		TemplatedDeserializeIntoVector<uint32_t>(v, vcount, col_idx, key_locations, validitymask_locations);
		break;
	case PhysicalType::UINT64:
		TemplatedDeserializeIntoVector<uint64_t>(v, vcount, col_idx, key_locations, validitymask_locations);
		break;
	case PhysicalType::INT128:
		TemplatedDeserializeIntoVector<hugeint_t>(v, vcount, col_idx, key_locations, validitymask_locations);
		break;
	case PhysicalType::FLOAT:
		TemplatedDeserializeIntoVector<float>(v, vcount, col_idx, key_locations, validitymask_locations);
		break;
	case PhysicalType::DOUBLE:
		TemplatedDeserializeIntoVector<double>(v, vcount, col_idx, key_locations, validitymask_locations);
		break;
	case PhysicalType::HASH:
		TemplatedDeserializeIntoVector<hash_t>(v, vcount, col_idx, key_locations, validitymask_locations);
		break;
	case PhysicalType::INTERVAL:
		TemplatedDeserializeIntoVector<interval_t>(v, vcount, col_idx, key_locations, validitymask_locations);
		break;
	case PhysicalType::VARCHAR: {
		idx_t len;
		auto target = FlatVector::GetData<string_t>(v);
		auto byte_offset = col_idx / 8;
		auto offset_in_byte = col_idx % 8;
		auto &validity = FlatVector::Validity(v);
		for (idx_t i = 0; i < vcount; i++) {
			// deserialize string length
			len = Load<uint32_t>(key_locations[i]);
			key_locations[i] += string_t::PREFIX_LENGTH;
			// deserialize string
			target[i] = StringVector::AddString(v, (const char *)key_locations[i], len);
			key_locations[i] += len;
			// set validitymask
			validity.Set(i, *(validitymask_locations[i] + byte_offset) & (1 << offset_in_byte));
		}
		break;
	}
	default:
		throw NotImplementedException("FIXME: unimplemented deserialize");
	}
}

void RowChunk::DeserializeIntoVector(Vector &v, const idx_t &vcount, const idx_t &col_idx, data_ptr_t key_locations[],
                                     data_ptr_t validitymask_locations[]) {
	DeserializeIntoVectorData(v, v.GetType().InternalType(), vcount, col_idx, key_locations, validitymask_locations);
}

} // namespace duckdb
