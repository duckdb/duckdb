#include "duckdb/execution/index/art/art_key.hpp"
#include "duckdb/execution/index/art/art.hpp"

using namespace duckdb;

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

static uint8_t FlipSign(uint8_t key_byte) {
	return key_byte ^ 128;
}

Key::Key(unique_ptr<data_t[]> data, index_t len) : len(len), data(move(data)) {
}

template <> unique_ptr<data_t[]> Key::CreateData(int8_t value, bool is_little_endian) {
	auto data = unique_ptr<data_t[]>(new data_t[sizeof(value)]);
	reinterpret_cast<uint8_t *>(data.get())[0] = value;
	data[0] = FlipSign(data[0]);
	return data;
}

template <> unique_ptr<data_t[]> Key::CreateData(int16_t value, bool is_little_endian) {
	auto data = unique_ptr<data_t[]>(new data_t[sizeof(value)]);
	reinterpret_cast<uint16_t *>(data.get())[0] = is_little_endian ? BSWAP16(value) : value;
	data[0] = FlipSign(data[0]);
	return data;
}

template <> unique_ptr<data_t[]> Key::CreateData(int32_t value, bool is_little_endian) {
	auto data = unique_ptr<data_t[]>(new data_t[sizeof(value)]);
	reinterpret_cast<uint32_t *>(data.get())[0] = is_little_endian ? BSWAP32(value) : value;
	data[0] = FlipSign(data[0]);
	return data;
}

template <> unique_ptr<data_t[]> Key::CreateData(int64_t value, bool is_little_endian) {
	auto data = unique_ptr<data_t[]>(new data_t[sizeof(value)]);
	reinterpret_cast<uint64_t *>(data.get())[0] = is_little_endian ? BSWAP64(value) : value;
	data[0] = FlipSign(data[0]);
	return data;
}

template <> unique_ptr<Key> Key::CreateKey(string value, bool is_little_endian) {
	index_t len = value.size() + 1;
	auto data = unique_ptr<data_t[]>(new data_t[len]);
	memcpy(data.get(), value.c_str(), len);
	return make_unique<Key>(move(data), len);
}

bool Key::operator>(const Key &k) const {
	for (index_t i = 0; i < std::min(len, k.len); i++) {
		if (data[i] > k.data[i]) {
			return true;
		} else if (data[i] < k.data[i]) {
			return false;
		}
	}
	return len > k.len;
}

bool Key::operator>=(const Key &k) const {
	for (index_t i = 0; i < std::min(len, k.len); i++) {
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
	for (index_t i = 0; i < len; i++) {
		if (data[i] != k.data[i]) {
			return false;
		}
	}
	return true;
}

inline uint8_t &Key::operator[](std::size_t i) {
	assert(i <= len);
	return data[i];
}

inline const uint8_t &Key::operator[](std::size_t i) const {
	assert(i <= len);
	return data[i];
}
