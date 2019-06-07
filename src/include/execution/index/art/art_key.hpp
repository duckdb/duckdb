//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/index/art/art_key.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"

namespace duckdb {

class ART;

class Key {
public:
	Key(ART &art, TypeId type, uintptr_t k);
	Key();
	Key(const Key &key) = delete;

	uint8_t len;
	unique_ptr<uint8_t[]> data;
public:
	uint8_t &operator[](std::size_t i);
	const uint8_t &operator[](std::size_t i) const;
	bool operator>(const Key &k) const;
	bool operator==(const Key &k) const;
private:
	void ConvertToBinaryComparable(bool is_little_endian, TypeId type, uintptr_t tid);
};

}
