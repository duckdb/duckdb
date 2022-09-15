#include "duckdb/execution/index/art/faster_art_key.hpp"

#include "duckdb/execution/index/art/art.hpp"

namespace duckdb {

FKey::FKey(data_ptr_t data, idx_t len) : len(len), data(data) {
}

FKey::FKey(ArenaAllocator &allocator, idx_t len) : len(len) {
	data = allocator.Allocate(len);
}

template <>
unique_ptr<FKey> FKey::CreateKey(ArenaAllocator &allocator, string_t value) {
	idx_t len = value.GetSize() + 1;
	auto data = allocator.Allocate(len);
	memcpy(data, value.GetDataUnsafe(), len - 1);
	data[len - 1] = '\0';
	return make_unique<FKey>(data, len);
}

template <>
unique_ptr<FKey> FKey::CreateKey(ArenaAllocator &allocator, const char *value) {
	return FKey::CreateKey(allocator, string_t(value, strlen(value)));
}

bool FKey::operator>(const FKey &k) const {
	for (idx_t i = 0; i < MinValue<idx_t>(len, k.len); i++) {
		if (data[i] > k.data[i]) {
			return true;
		} else if (data[i] < k.data[i]) {
			return false;
		}
	}
	return len > k.len;
}

bool FKey::operator<(const FKey &k) const {
	for (idx_t i = 0; i < MinValue<idx_t>(len, k.len); i++) {
		if (data[i] < k.data[i]) {
			return true;
		} else if (data[i] > k.data[i]) {
			return false;
		}
	}
	return len < k.len;
}

bool FKey::operator>=(const FKey &k) const {
	for (idx_t i = 0; i < MinValue<idx_t>(len, k.len); i++) {
		if (data[i] > k.data[i]) {
			return true;
		} else if (data[i] < k.data[i]) {
			return false;
		}
	}
	return len >= k.len;
}

bool FKey::operator==(const FKey &k) const {
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

bool FKey::ByteMatches(FKey &other, idx_t &depth) {
	return data[depth] == other[depth];
}
} // namespace duckdb
