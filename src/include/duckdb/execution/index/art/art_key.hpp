//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/art_key.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/radix.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

class Key {
public:
	Key(unique_ptr<data_t[]> data, idx_t len);

	idx_t len;
	unique_ptr<data_t[]> data;

public:
	template <class T>
	static inline unique_ptr<Key> CreateKey(T element, bool is_little_endian) {
		auto data = Key::CreateData<T>(element, is_little_endian);
		return make_unique<Key>(move(data), sizeof(element));
	}

	template <class T>
	static inline unique_ptr<Key> CreateKey(const Value &element, bool is_little_endian) {
		return CreateKey(element.GetValueUnsafe<T>(), is_little_endian);
	}

public:
	data_t &operator[](std::size_t i) {
		return data[i];
	}
	const data_t &operator[](std::size_t i) const {
		return data[i];
	}
	bool operator>(const Key &k) const;
	bool operator<(const Key &k) const;
	bool operator>=(const Key &k) const;
	bool operator==(const Key &k) const;

private:
	template <class T>
	static inline unique_ptr<data_t[]> CreateData(T value, bool is_little_endian) {
		auto data = unique_ptr<data_t[]>(new data_t[sizeof(value)]);
		Radix::EncodeData<T>(data.get(), value, is_little_endian);
		return data;
	}
};

template <>
unique_ptr<Key> Key::CreateKey(string_t value, bool is_little_endian);
template <>
unique_ptr<Key> Key::CreateKey(const char *value, bool is_little_endian);

} // namespace duckdb
