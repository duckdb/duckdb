//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/index/art/art_key.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"
#include "common/exception.hpp"

namespace duckdb {

class ART;

class Key {
public:
	Key(unique_ptr<data_t[]> data, index_t len);

	index_t len;
	unique_ptr<data_t[]> data;
public:
	template<class T>
	static unique_ptr<Key> CreateKey(ART &art, T element) {
		auto data = Key::CreateData<T>(art, element);
		return make_unique<Key>(move(data), sizeof(element));
	}
public:
	data_t &operator[](std::size_t i);
	const data_t &operator[](std::size_t i) const;
	bool operator>(const Key &k) const;
	bool operator>=(const Key &k) const;
	bool operator==(const Key &k) const;
private:
	template<class T>
	static unique_ptr<data_t[]> CreateData(ART &art, T value) {
		throw NotImplementedException("Cannot create data from this type");
	}
};

template<> unique_ptr<data_t[]> Key::CreateData(ART &art, int8_t value);
template<> unique_ptr<data_t[]> Key::CreateData(ART &art, int16_t value);
template<> unique_ptr<data_t[]> Key::CreateData(ART &art, int32_t value);
template<> unique_ptr<data_t[]> Key::CreateData(ART &art, int64_t value);

}
