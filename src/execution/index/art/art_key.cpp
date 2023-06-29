#include "duckdb/execution/index/art/art_key.hpp"

namespace duckdb {

ARTKey::ARTKey() : len(0) {
}

ARTKey::ARTKey(const data_ptr_t &data, const uint32_t &len) : len(len), data(data) {
}

ARTKey::ARTKey(ArenaAllocator &allocator, const uint32_t &len) : len(len) {
	data = allocator.Allocate(len);
}

template <>
ARTKey ARTKey::CreateARTKey(ArenaAllocator &allocator, const LogicalType &type, string_t value) {
	uint32_t len = value.GetSize() + 1;
	auto data = allocator.Allocate(len);
	memcpy(data, value.GetData(), len - 1);

	// FIXME: rethink this
	if (type == LogicalType::BLOB || type == LogicalType::VARCHAR) {
		// indexes cannot contain BLOBs (or BLOBs cast to VARCHARs) that contain null-terminated bytes
		for (uint32_t i = 0; i < len - 1; i++) {
			if (data[i] == '\0') {
				throw NotImplementedException("Indexes cannot contain BLOBs that contain null-terminated bytes.");
			}
		}
	}

	data[len - 1] = '\0';
	return ARTKey(data, len);
}

template <>
ARTKey ARTKey::CreateARTKey(ArenaAllocator &allocator, const LogicalType &type, const char *value) {
	return ARTKey::CreateARTKey(allocator, type, string_t(value, strlen(value)));
}

template <>
void ARTKey::CreateARTKey(ArenaAllocator &allocator, const LogicalType &type, ARTKey &key, string_t value) {
	key.len = value.GetSize() + 1;
	key.data = allocator.Allocate(key.len);
	memcpy(key.data, value.GetData(), key.len - 1);

	// FIXME: rethink this
	if (type == LogicalType::BLOB || type == LogicalType::VARCHAR) {
		// indexes cannot contain BLOBs (or BLOBs cast to VARCHARs) that contain null-terminated bytes
		for (uint32_t i = 0; i < key.len - 1; i++) {
			if (key.data[i] == '\0') {
				throw NotImplementedException("Indexes cannot contain BLOBs that contain null-terminated bytes.");
			}
		}
	}

	key.data[key.len - 1] = '\0';
}

template <>
void ARTKey::CreateARTKey(ArenaAllocator &allocator, const LogicalType &type, ARTKey &key, const char *value) {
	ARTKey::CreateARTKey(allocator, type, key, string_t(value, strlen(value)));
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
} // namespace duckdb
