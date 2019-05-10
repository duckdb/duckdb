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

KeyLen Key::getKeyLen() const {
	return len;
}

Key::Key(Key &&key) {
	len = key.len;
	data = move(key.data);

}

void Key::set(const char bytes[], const uint64_t length) {
		data = unique_ptr<uint8_t[]>(new uint8_t[length]);
		memcpy(data.get(), bytes, length);
	len = length;
}

void Key::operator=(const char key[]) {
	len = strlen(key);

		data = unique_ptr<uint8_t[]>(new uint8_t[len]);
		memcpy(data.get(), key, len);
}

void Key::setKeyLen(KeyLen newLen) {
	if (len == newLen)
		return;
	len = newLen;
		data = unique_ptr<uint8_t[]>(new uint8_t[len]);
}

void Key::convert_to_binary_comparable(bool isLittleEndian, TypeId type, uintptr_t tid) {
//	data = move(stackKey);
	switch (type) {
	case TypeId::BOOLEAN:
		len = 1;
		if (isLittleEndian) {
			data[0] = BSWAP8(tid);
		} else {
			data[0] = tid;
		}
		break;
	case TypeId::TINYINT:
		len = 1;
		if (isLittleEndian) {
			data[0] = BSWAP8(tid);
		} else {
			data[0] = tid;
		}
		data[0] = flipSign(data[0]);
		break;
	case TypeId::SMALLINT:
		len = 2;
		if (isLittleEndian) {
			reinterpret_cast<uint16_t *>(data.get())[0] = BSWAP16(tid);
		} else {
			reinterpret_cast<uint16_t *>(data.get())[0] = tid;
		}
		data[0] = flipSign(data[0]);
		break;
	case TypeId::INTEGER:
		len = 4;
		if (isLittleEndian) {
			reinterpret_cast<uint32_t *>(data.get())[0] = BSWAP32(tid);
		} else {
			reinterpret_cast<uint32_t *>(data.get())[0] = tid;
		}
		data[0] = flipSign(data[0]);
		break;
	case TypeId::BIGINT:
		            len = 8;
		if (isLittleEndian) {
			reinterpret_cast<uint64_t *>(data.get())[0] = BSWAP64(tid);
		} else {
			reinterpret_cast<uint64_t *>(data.get())[0] = tid;
		}
		data[0] = flipSign(data[0]);
		break;
	default:
		throw NotImplementedException("Unimplemented type for ART index");
	}
}
