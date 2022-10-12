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
Key Key::CreateKey(ArenaAllocator &allocator, string_t value) {
	idx_t len = value.GetSize() + 1;
	auto data = allocator.Allocate(len);
	memcpy(data, value.GetDataUnsafe(), len - 1);
	data[len - 1] = '\0';
	return Key(data, len);
}

template <>
Key Key::CreateKey(ArenaAllocator &allocator, const char *value) {
	return Key::CreateKey(allocator, string_t(value, strlen(value)));
}

template <>
void Key::CreateKey(ArenaAllocator &allocator, Key &key, string_t value) {
	key.len = value.GetSize() + 1;
	key.data = allocator.Allocate(key.len);
	memcpy(key.data, value.GetDataUnsafe(), key.len - 1);
	key.data[key.len - 1] = '\0';
}

template <>
void Key::CreateKey(ArenaAllocator &allocator, Key &key, const char *value) {
	Key::CreateKey(allocator, key, string_t(value, strlen(value)));
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
