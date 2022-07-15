#include "duckdb/execution/index/art/art_key.hpp"

#include "duckdb/execution/index/art/art.hpp"

namespace duckdb {

Key::Key(unique_ptr<data_t[]> data, idx_t len) : len(len), data(move(data)) {
}

template <>
unique_ptr<Key> Key::CreateKey(string_t value) {
	idx_t len = value.GetSize() + 1;
	auto data = unique_ptr<data_t[]>(new data_t[len]);
	memcpy(data.get(), value.GetDataUnsafe(), len - 1);
	data[len - 1] = '\0';
	return make_unique<Key>(move(data), len);
}

template <>
unique_ptr<Key> Key::CreateKey(const char *value) {
	return Key::CreateKey(string_t(value, strlen(value)));
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
} // namespace duckdb
