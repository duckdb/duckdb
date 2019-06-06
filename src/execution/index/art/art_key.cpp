#include "execution/index/art/art_key.hpp"
#include "execution/index/art/art.hpp"

using namespace duckdb;

Key::Key(ART &art, TypeId type, uintptr_t k) :
	len(art.maxPrefix) {
	data = unique_ptr<uint8_t[]>(new uint8_t[art.maxPrefix]);
	ConvertToBinaryComparable(art.is_little_endian, type, k);
}

Key::Key() {
}

//! these are optimized and assume a particular byte order
#define BSWAP8(x) ((uint8_t)((((uint8_t)(x)&0xf0) >> 4) | (((uint8_t)(x)&0x0f) << 4)))

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

void Key::ConvertToBinaryComparable(bool is_little_endian, TypeId type, uintptr_t tid) {
	switch (type) {
	case TypeId::TINYINT:
		len = 1;
		if (is_little_endian) {
			data[0] = BSWAP8(tid);
		} else {
			data[0] = tid;
		}
		data[0] = FlipSign(data[0]);
		break;
	case TypeId::SMALLINT:
		len = 2;
		if (is_little_endian) {
			reinterpret_cast<uint16_t *>(data.get())[0] = BSWAP16(tid);
		} else {
			reinterpret_cast<uint16_t *>(data.get())[0] = tid;
		}
		data[0] = FlipSign(data[0]);
		break;
	case TypeId::INTEGER:
		len = 4;
		if (is_little_endian) {
			reinterpret_cast<uint32_t *>(data.get())[0] = BSWAP32(tid);
		} else {
			reinterpret_cast<uint32_t *>(data.get())[0] = tid;
		}
		data[0] = FlipSign(data[0]);
		break;
	case TypeId::BIGINT:
		len = 8;
		if (is_little_endian) {
			reinterpret_cast<uint64_t *>(data.get())[0] = BSWAP64(tid);
		} else {
			reinterpret_cast<uint64_t *>(data.get())[0] = tid;
		}
		data[0] = FlipSign(data[0]);
		break;
	default:
		throw NotImplementedException("Unimplemented type for ART index");
	}
}

bool Key::operator>(const Key &k) const {
	for (int i = 0; i < len; i++) {
		if (data[i] > k.data[i]) {
			return true;
		} else if (data[i] < k.data[i]) {
			return false;
		}
	}
	return false;
}

inline uint8_t &Key::operator[](std::size_t i) {
	assert(i <= len);
	return data[i];
}

inline const uint8_t &Key::operator[](std::size_t i) const {
	assert(i <= len);
	return data[i];
}