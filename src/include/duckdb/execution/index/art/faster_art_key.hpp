//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/faster_art_key.hpp
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

class FKey {
public:
	FKey();
	FKey(data_ptr_t data, idx_t len);
	FKey(ArenaAllocator &allocator, idx_t len);

	idx_t len;
	data_ptr_t data;

public:
	template <class T>
	static inline FKey CreateKey(ArenaAllocator &allocator, T element) {
		auto data = FKey::CreateData<T>(allocator, element);
		return FKey(data, sizeof(element));
	}

	template <class T>
	static inline FKey CreateKey(ArenaAllocator &allocator, const Value &element) {
		return CreateKey(allocator, element.GetValueUnsafe<T>());
	}

public:
	data_t &operator[](size_t i) {
		return data[i];
	}
	const data_t &operator[](size_t i) const {
		return data[i];
	}
	bool operator>(const FKey &k) const;
	bool operator<(const FKey &k) const;
	bool operator>=(const FKey &k) const;
	bool operator==(const FKey &k) const;

	bool ByteMatches(FKey &other, idx_t &depth);

private:
	template <class T>
	static inline data_ptr_t CreateData(ArenaAllocator &allocator, T value) {
		auto data = allocator.Allocate(sizeof(value));
		Radix::EncodeData<T>(data, value);
		return data;
	}
};

template <>
FKey FKey::CreateKey(ArenaAllocator &allocator, string_t value);
template <>
FKey FKey::CreateKey(ArenaAllocator &allocator, const char *value);

} // namespace duckdb
