//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/index/art/art_key.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

class Key {
public:
	Key(unique_ptr<data_t[]> data, index_t len);

	index_t len;
	unique_ptr<data_t[]> data;

public:
	template <class T> static unique_ptr<Key> CreateKey(T element, bool is_little_endian) {
		auto data = Key::CreateData<T>(element, is_little_endian);
		return make_unique<Key>(move(data), sizeof(element));
	}

public:
	data_t &operator[](std::size_t i);
	const data_t &operator[](std::size_t i) const;
	bool operator>(const Key &k) const;
	bool operator>=(const Key &k) const;
	bool operator==(const Key &k) const;

	string ToString(bool is_little_endian, TypeId type);

private:
	template <class T> static unique_ptr<data_t[]> CreateData(T value, bool is_little_endian) {
		throw NotImplementedException("Cannot create data from this type");
	}
};

template <> unique_ptr<data_t[]> Key::CreateData(int8_t value, bool is_little_endian);
template <> unique_ptr<data_t[]> Key::CreateData(int16_t value, bool is_little_endian);
template <> unique_ptr<data_t[]> Key::CreateData(int32_t value, bool is_little_endian);
template <> unique_ptr<data_t[]> Key::CreateData(int64_t value, bool is_little_endian);

template <> unique_ptr<Key> Key::CreateKey(string element, bool is_little_endian);

} // namespace duckdb
