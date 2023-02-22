#include "duckdb/execution/index/art/art_key.hpp"

#include "duckdb/execution/index/art/art.hpp"

namespace duckdb {

Key::Key() : len(0) {
}

Key::Key(data_ptr_t data, idx_t len) : len(len), data(data) {
}

Key::Key(ArenaAllocator &allocator, idx_t len) : len(len) {
	data = allocator.Allocate(len);
}

template <>
Key Key::CreateKey(ArenaAllocator &allocator, const LogicalType &type, string_t value) {
	idx_t len = value.GetSize() + 1;
	auto data = allocator.Allocate(len);
	memcpy(data, value.GetDataUnsafe(), len - 1);

	// FIXME: rethink this
	if (type == LogicalType::BLOB || type == LogicalType::VARCHAR) {
		// indexes cannot contain BLOBs (or BLOBs cast to VARCHARs) that contain null-terminated bytes
		for (idx_t i = 0; i < len - 1; i++) {
			if (data[i] == '\0') {
				throw NotImplementedException("Indexes cannot contain BLOBs that contain null-terminated bytes.");
			}
		}
	}

	data[len - 1] = '\0';
	return Key(data, len);
}

template <>
Key Key::CreateKey(ArenaAllocator &allocator, const LogicalType &type, const char *value) {
	return Key::CreateKey(allocator, type, string_t(value, strlen(value)));
}

template <>
void Key::CreateKey(ArenaAllocator &allocator, const LogicalType &type, Key &key, string_t value) {
	key.len = value.GetSize() + 1;
	key.data = allocator.Allocate(key.len);
	memcpy(key.data, value.GetDataUnsafe(), key.len - 1);

	// FIXME: rethink this
	if (type == LogicalType::BLOB || type == LogicalType::VARCHAR) {
		// indexes cannot contain BLOBs (or BLOBs cast to VARCHARs) that contain null-terminated bytes
		for (idx_t i = 0; i < key.len - 1; i++) {
			if (key.data[i] == '\0') {
				throw NotImplementedException("Indexes cannot contain BLOBs that contain null-terminated bytes.");
			}
		}
	}

	key.data[key.len - 1] = '\0';
}

template <>
void Key::CreateKey(ArenaAllocator &allocator, const LogicalType &type, Key &key, const char *value) {
	Key::CreateKey(allocator, type, key, string_t(value, strlen(value)));
}

bool Key::operator>(const Key &k) const {
	for (idx_t i = 0; i < MinValue<idx_t>(len, k.len); i++) {
		if (data[i] > k.data[i]) {
			return true;
		} else if (data[i] < k.data[i]) {
			return false;
		}
	}
	return len > k.len;
}

bool Key::operator<(const Key &k) const {
	for (idx_t i = 0; i < MinValue<idx_t>(len, k.len); i++) {
		if (data[i] < k.data[i]) {
			return true;
		} else if (data[i] > k.data[i]) {
			return false;
		}
	}
	return len < k.len;
}

bool Key::operator>=(const Key &k) const {
	for (idx_t i = 0; i < MinValue<idx_t>(len, k.len); i++) {
		if (data[i] > k.data[i]) {
			return true;
		} else if (data[i] < k.data[i]) {
			return false;
		}
	}
	return len >= k.len;
}

bool Key::operator==(const Key &k) const {
	if (len != k.len) {
		return false;
	}
	for (idx_t i = 0; i < len; i++) {
		if (data[i] != k.data[i]) {
			return false;
		}
	}
	return true;
}

bool Key::ByteMatches(Key &other, idx_t &depth) {
	return data[depth] == other[depth];
}

bool Key::Empty() {
	return len == 0;
}

void Key::ConcatenateKey(ArenaAllocator &allocator, Key &other_key) {

	auto compound_data = allocator.Allocate(len + other_key.len);
	memcpy(compound_data, data, len);
	memcpy(compound_data + len, other_key.data, other_key.len);
	len += other_key.len;
	data = compound_data;
}
} // namespace duckdb
