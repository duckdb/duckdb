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
#include "duckdb/storage/arena_allocator.hpp"

namespace duckdb {

class Key {
public:
	Key();
	Key(data_ptr_t data, idx_t len);
	Key(ArenaAllocator &allocator, idx_t len);

	idx_t len;
	data_ptr_t data;
	row_t row_id;

public:
	template <class T>
	static inline Key CreateKey(ArenaAllocator &allocator, T element) {
		auto data = Key::CreateData<T>(allocator, element);
		return Key(data, sizeof(element));
	}

	template <class T>
	static inline Key CreateKey(ArenaAllocator &allocator, const Value &element) {
		return CreateKey(allocator, element.GetValueUnsafe<T>());
	}

	template <class T>
	static inline void CreateKey(ArenaAllocator &allocator, Key &key, T element) {
		key.data = Key::CreateData<T>(allocator, element);
		key.len = sizeof(element);
	}

	template <class T>
	static inline void CreateKey(ArenaAllocator &allocator, Key &key, const Value element) {
		key.data = Key::CreateData<T>(allocator, element.GetValueUnsafe<T>());
		key.len = sizeof(element);
	}

public:
	data_t &operator[](size_t i) {
		return data[i];
	}
	const data_t &operator[](size_t i) const {
		return data[i];
	}
	bool operator>(const Key &k) const;
	bool operator<(const Key &k) const;
	bool operator>=(const Key &k) const;
	bool operator==(const Key &k) const;

	bool ByteMatches(Key &other, idx_t &depth);
	bool Empty();
	void ConcatenateKey(ArenaAllocator &allocator, Key &concat_key);

private:
	template <class T>
	static inline data_ptr_t CreateData(ArenaAllocator &allocator, T value) {
		auto data = allocator.Allocate(sizeof(value));
		Radix::EncodeData<T>(data, value);
		return data;
	}
};

template <>
Key Key::CreateKey(ArenaAllocator &allocator, string_t value);
template <>
Key Key::CreateKey(ArenaAllocator &allocator, const char *value);
template <>
void Key::CreateKey(ArenaAllocator &allocator, Key &key, string_t value);
template <>
void Key::CreateKey(ArenaAllocator &allocator, Key &key, const char *value);

} // namespace duckdb
