#include "duckdb/execution/index/art/art_key.hpp"

namespace duckdb {

ARTKey::ARTKey() : len(0) {
}

ARTKey::ARTKey(const data_ptr_t &data, const uint32_t len) : len(len), data(data) {
}

ARTKey::ARTKey(ArenaAllocator &allocator, const uint32_t len) : len(len) {
	data = allocator.Allocate(len);
}

template <>
ARTKey ARTKey::CreateARTKey(ArenaAllocator &allocator, const LogicalType &type, string_t value) {
	auto string_data = const_data_ptr_cast(value.GetData());
	auto string_len = value.GetSize();
	// we need to escape \00 and \01
	idx_t escape_count = 0;
	for (idx_t r = 0; r < string_len; r++) {
		if (string_data[r] <= 1) {
			escape_count++;
		}
	}
	idx_t len = string_len + escape_count + 1;
	auto data = allocator.Allocate(len);
	// copy over the data and add in escapes
	idx_t pos = 0;
	for (idx_t r = 0; r < string_len; r++) {
		if (string_data[r] <= 1) {
			// escape
			data[pos++] = '\01';
		}
		data[pos++] = string_data[r];
	}
	// end with a null-terminator
	data[pos] = '\0';
	return ARTKey(data, UnsafeNumericCast<uint32_t>(len));
}

template <>
ARTKey ARTKey::CreateARTKey(ArenaAllocator &allocator, const LogicalType &type, const char *value) {
	return ARTKey::CreateARTKey(allocator, type, string_t(value, UnsafeNumericCast<uint32_t>(strlen(value))));
}

template <>
void ARTKey::CreateARTKey(ArenaAllocator &allocator, const LogicalType &type, ARTKey &key, string_t value) {
	key = ARTKey::CreateARTKey<string_t>(allocator, type, value);
}

template <>
void ARTKey::CreateARTKey(ArenaAllocator &allocator, const LogicalType &type, ARTKey &key, const char *value) {
	ARTKey::CreateARTKey(allocator, type, key, string_t(value, UnsafeNumericCast<uint32_t>(strlen(value))));
}

ARTKey ARTKey::CreateKey(ArenaAllocator &allocator, PhysicalType type, Value &value) {
	D_ASSERT(type == value.type().InternalType());
	switch (type) {
	case PhysicalType::BOOL:
		return ARTKey::CreateARTKey<bool>(allocator, value.type(), value);
	case PhysicalType::INT8:
		return ARTKey::CreateARTKey<int8_t>(allocator, value.type(), value);
	case PhysicalType::INT16:
		return ARTKey::CreateARTKey<int16_t>(allocator, value.type(), value);
	case PhysicalType::INT32:
		return ARTKey::CreateARTKey<int32_t>(allocator, value.type(), value);
	case PhysicalType::INT64:
		return ARTKey::CreateARTKey<int64_t>(allocator, value.type(), value);
	case PhysicalType::UINT8:
		return ARTKey::CreateARTKey<uint8_t>(allocator, value.type(), value);
	case PhysicalType::UINT16:
		return ARTKey::CreateARTKey<uint16_t>(allocator, value.type(), value);
	case PhysicalType::UINT32:
		return ARTKey::CreateARTKey<uint32_t>(allocator, value.type(), value);
	case PhysicalType::UINT64:
		return ARTKey::CreateARTKey<uint64_t>(allocator, value.type(), value);
	case PhysicalType::INT128:
		return ARTKey::CreateARTKey<hugeint_t>(allocator, value.type(), value);
	case PhysicalType::UINT128:
		return ARTKey::CreateARTKey<uhugeint_t>(allocator, value.type(), value);
	case PhysicalType::FLOAT:
		return ARTKey::CreateARTKey<float>(allocator, value.type(), value);
	case PhysicalType::DOUBLE:
		return ARTKey::CreateARTKey<double>(allocator, value.type(), value);
	case PhysicalType::VARCHAR:
		return ARTKey::CreateARTKey<string_t>(allocator, value.type(), value);
	default:
		throw InternalException("Invalid type for the ART key");
	}
}

bool ARTKey::operator>(const ARTKey &k) const {
	for (uint32_t i = 0; i < MinValue<uint32_t>(len, k.len); i++) {
		if (data[i] > k.data[i]) {
			return true;
		} else if (data[i] < k.data[i]) {
			return false;
		}
	}
	return len > k.len;
}

bool ARTKey::operator>=(const ARTKey &k) const {
	for (uint32_t i = 0; i < MinValue<uint32_t>(len, k.len); i++) {
		if (data[i] > k.data[i]) {
			return true;
		} else if (data[i] < k.data[i]) {
			return false;
		}
	}
	return len >= k.len;
}

bool ARTKey::operator==(const ARTKey &k) const {
	if (len != k.len) {
		return false;
	}
	for (uint32_t i = 0; i < len; i++) {
		if (data[i] != k.data[i]) {
			return false;
		}
	}
	return true;
}

void ARTKey::ConcatenateARTKey(ArenaAllocator &allocator, ARTKey &other_key) {

	auto compound_data = allocator.Allocate(len + other_key.len);
	memcpy(compound_data, data, len);
	memcpy(compound_data + len, other_key.data, other_key.len);
	len += other_key.len;
	data = compound_data;
}

row_t ARTKey::GetRowID() const {
	D_ASSERT(len == sizeof(row_t));
	row_t row_id = 0;
	for (idx_t i = 0; i < 4; i++) {
		row_id |= data[i];
		row_id <<= 8;
	}
	return row_id;
}

} // namespace duckdb
