#include <cfloat>
#include <limits.h>
#include <cstring> // strlen() on Solaris

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

uint32_t Key::EncodeFloat(float x) {
	unsigned long buff;

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
	buff = reinterpret_cast<uint32_t *>(&x)[0];
	if ((buff & (1u << 31)) == 0) { //! +0 and positive numbers
		buff |= (1u << 31);
	} else {          //! negative numbers
		buff = ~buff; //! complement 1
	}

	return buff;
}

uint64_t Key::EncodeDouble(double x) {
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
	buff = reinterpret_cast<uint64_t *>(&x)[0];
	if (buff < (1ull << 63)) { //! +0 and positive numbers
		buff += (1ull << 63);
	} else {          //! negative numbers
		buff = ~buff; //! complement 1
	}
	return buff;
}

Key::Key(unique_ptr<data_t[]> data, idx_t len) : len(len), data(move(data)) {
}

template <> unique_ptr<data_t[]> Key::CreateData(bool value, bool is_little_endian) {
	auto data = unique_ptr<data_t[]>(new data_t[sizeof(value)]);
	data[0] = value ? 1 : 0;
	return data;
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

template <> unique_ptr<data_t[]> Key::CreateData(float value, bool is_little_endian) {
	uint32_t converted_value = EncodeFloat(value);
	auto data = unique_ptr<data_t[]>(new data_t[sizeof(converted_value)]);
	reinterpret_cast<uint32_t *>(data.get())[0] = is_little_endian ? BSWAP32(converted_value) : converted_value;
	return data;
}
template <> unique_ptr<data_t[]> Key::CreateData(double value, bool is_little_endian) {
	uint64_t converted_value = EncodeDouble(value);
	auto data = unique_ptr<data_t[]>(new data_t[sizeof(converted_value)]);
	reinterpret_cast<uint64_t *>(data.get())[0] = is_little_endian ? BSWAP64(converted_value) : converted_value;
	return data;
}

template <> unique_ptr<Key> Key::CreateKey(string_t value, bool is_little_endian) {
	idx_t len = value.GetSize() + 1;
	auto data = unique_ptr<data_t[]>(new data_t[len]);
	memcpy(data.get(), value.GetData(), len);
	return make_unique<Key>(move(data), len);
}

template <> unique_ptr<Key> Key::CreateKey(const char *value, bool is_little_endian) {
	return Key::CreateKey(string_t(value, strlen(value)), is_little_endian);
}

bool Key::operator>(const Key &k) const {
	for (idx_t i = 0; i < std::min(len, k.len); i++) {
		if (data[i] > k.data[i]) {
			return true;
		} else if (data[i] < k.data[i]) {
			return false;
		}
	}
	return len > k.len;
}

bool Key::operator<(const Key &k) const {
	for (idx_t i = 0; i < std::min(len, k.len); i++) {
		if (data[i] < k.data[i]) {
			return true;
		} else if (data[i] > k.data[i]) {
			return false;
		}
	}
	return len < k.len;
}

bool Key::operator>=(const Key &k) const {
	for (idx_t i = 0; i < std::min(len, k.len); i++) {
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
