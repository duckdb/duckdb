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

class ARTKey {
public:
	ARTKey();
	ARTKey(const data_ptr_t &data, const uint32_t &len);
	ARTKey(ArenaAllocator &allocator, const uint32_t &len);

	uint32_t len;
	data_ptr_t data;

public:
	template <class T>
	static inline ARTKey CreateARTKey(ArenaAllocator &allocator, const LogicalType &type, T element) {
		auto data = ARTKey::CreateData<T>(allocator, element);
		return ARTKey(data, sizeof(element));
	}

	template <class T>
	static inline ARTKey CreateARTKey(ArenaAllocator &allocator, const LogicalType &type, const Value &element) {
		return CreateARTKey(allocator, type, element.GetValueUnsafe<T>());
	}

	template <class T>
	static inline void CreateARTKey(ArenaAllocator &allocator, const LogicalType &type, ARTKey &key, T element) {
		key.data = ARTKey::CreateData<T>(allocator, element);
		key.len = sizeof(element);
	}

	template <class T>
	static inline void CreateARTKey(ArenaAllocator &allocator, const LogicalType &type, ARTKey &key,
	                                const Value element) {
		key.data = ARTKey::CreateData<T>(allocator, element.GetValueUnsafe<T>());
		key.len = sizeof(element);
	}

public:
	data_t &operator[](size_t i) {
		return data[i];
	}
	const data_t &operator[](size_t i) const {
		return data[i];
	}
	bool operator>(const ARTKey &k) const;
	bool operator>=(const ARTKey &k) const;
	bool operator==(const ARTKey &k) const;

	inline bool ByteMatches(const ARTKey &other, const uint32_t &depth) const {
		return data[depth] == other[depth];
	}
	inline bool Empty() const {
		return len == 0;
	}
	void ConcatenateARTKey(ArenaAllocator &allocator, ARTKey &concat_key);

private:
	template <class T>
	static inline data_ptr_t CreateData(ArenaAllocator &allocator, T value) {
		auto data = allocator.Allocate(sizeof(value));
		Radix::EncodeData<T>(data, value);
		return data;
	}
};

template <>
ARTKey ARTKey::CreateARTKey(ArenaAllocator &allocator, const LogicalType &type, string_t value);
template <>
ARTKey ARTKey::CreateARTKey(ArenaAllocator &allocator, const LogicalType &type, const char *value);
template <>
void ARTKey::CreateARTKey(ArenaAllocator &allocator, const LogicalType &type, ARTKey &key, string_t value);
} // namespace duckdb
