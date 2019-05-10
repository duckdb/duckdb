#include "execution/index/art/art_key.hpp"

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

inline KeyLen Key::getKeyLen() const {
	return len;
}

inline Key::~Key() {
	if (len > stackLen) {
		delete[] data;
		data = nullptr;
	}
}

inline Key::Key(Key &&key) {
	len = key.len;
	if (len > stackLen) {
		data = key.data;
		key.data = nullptr;
	} else {
		memcpy(stackKey, key.stackKey, key.len);
		data = stackKey;
	}
}

inline void Key::set(const char bytes[], const std::size_t length) {
	if (len > stackLen) {
		delete[] data;
	}
	if (length <= stackLen) {
		memcpy(stackKey, bytes, length);
		data = stackKey;
	} else {
		data = new uint8_t[length];
		memcpy(data, bytes, length);
	}
	len = length;
}

inline void Key::operator=(const char key[]) {
	if (len > stackLen) {
		delete[] data;
	}
	len = strlen(key);
	if (len <= stackLen) {
		memcpy(stackKey, key, len);
		data = stackKey;
	} else {
		data = new uint8_t[len];
		memcpy(data, key, len);
	}
}

inline void Key::setKeyLen(KeyLen newLen) {
	if (len == newLen)
		return;
	if (len > stackLen) {
		delete[] data;
	}
	len = newLen;
	if (len > stackLen) {
		data = new uint8_t[len];
	} else {
		data = stackKey;
	}
}

void Key::convert_to_binary_comparable(bool isLittleEndian, TypeId type, uintptr_t tid) {
	data = stackKey;
	switch (type) {
	case TypeId::BOOLEAN:
		len = 1;
		if (isLittleEndian) {
			reinterpret_cast<uint8_t *>(stackKey)[0] = BSWAP8(tid);
		} else {
			reinterpret_cast<uint8_t *>(stackKey)[0] = tid;
		}
	case TypeId::TINYINT:
		len = 1;
		if (isLittleEndian) {
			reinterpret_cast<uint8_t *>(stackKey)[0] = BSWAP8(tid);
		} else {
			reinterpret_cast<uint8_t *>(stackKey)[0] = tid;
		}
		stackKey[0] = flipSign(stackKey[0]);
		break;
	case TypeId::SMALLINT:
		len = 2;
		if (isLittleEndian) {
			reinterpret_cast<uint16_t *>(stackKey)[0] = BSWAP16(tid);
		} else {
			reinterpret_cast<uint16_t *>(stackKey)[0] = tid;
		}
		stackKey[0] = flipSign(stackKey[0]);
		break;
	case TypeId::INTEGER:
		len = 4;
		if (isLittleEndian) {
			reinterpret_cast<uint32_t *>(stackKey)[0] = BSWAP32(tid);
		} else {
			reinterpret_cast<uint32_t *>(stackKey)[0] = tid;
		}
		stackKey[0] = flipSign(stackKey[0]);
		break;
	case TypeId::BIGINT:
		            len = 8;
		if (isLittleEndian) {
			reinterpret_cast<uint64_t *>(stackKey)[0] = BSWAP64(tid);
		} else {
			reinterpret_cast<uint64_t *>(stackKey)[0] = tid;
		}
		stackKey[0] = flipSign(stackKey[0]);
		break;
	default:
		throw NotImplementedException("Unimplemented type for ART index");
	}
}
