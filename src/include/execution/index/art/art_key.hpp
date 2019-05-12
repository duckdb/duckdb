//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/index/art/art_key.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <stdint.h>
#include <cstring>
#include <memory>
#include <assert.h>
#include "common/common.hpp"
#include <common/exception.hpp>

using KeyLen = uint8_t;
using namespace duckdb;
class Key {
public:
	uint8_t len;

    unique_ptr<uint8_t[]> data;

	Key(bool isLittleEndian, TypeId type, uintptr_t k, uint8_t maxKeyLength) {
		len = maxKeyLength;
		data = unique_ptr<uint8_t[]>(new uint8_t[maxKeyLength]);
		convert_to_binary_comparable(isLittleEndian, type, k);
	}

	uint8_t flipSign(uint8_t keyByte) {
		return keyByte ^ 128;
	}

	void convert_to_binary_comparable(bool isLittleEndian, TypeId type, uintptr_t tid);

	Key() {
	}

	Key(const Key &key) = delete;

	Key(Key &&key);

	void set(const char bytes[], const uint64_t length);

	void operator=(const char key[]);

	bool operator==(const Key &k) const {
		if (k.getKeyLen() != getKeyLen()) {
			return false;
		}
		return std::memcmp(&k[0], data.get(), getKeyLen()) == 0;
	}

	bool operator!=(const Key &k) const {
		if (k.getKeyLen() != getKeyLen()) {
			return true;
		}
		return !(std::memcmp(&k[0], data.get(), getKeyLen()) == 0);
	}

	uint8_t &operator[](std::size_t i);

	const uint8_t &operator[](std::size_t i) const;


	bool operator>(const Key &k) const {
		for (int i = 0; i < len; i ++){
			if (data[i] > k.data[i])
				return true;
			else if (data[i] < k.data[i])
				return false;
		}
		return false;
	}
	KeyLen getKeyLen() const;

	void setKeyLen(KeyLen len);
};

inline uint8_t &Key::operator[](std::size_t i) {
	assert(i <= len);
	return data[i];
}

inline const uint8_t &Key::operator[](std::size_t i) const {
	assert(i <= len);
	return data[i];
}
