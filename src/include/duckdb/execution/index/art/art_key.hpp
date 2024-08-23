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
	ARTKey(data_ptr_t data, idx_t len);
	ARTKey(ArenaAllocator &allocator, idx_t len);

	idx_t len;
	data_ptr_t data;

public:
	template <class T>
	static inline ARTKey CreateARTKey(ArenaAllocator &allocator, T value) {
		auto data = ARTKey::CreateData<T>(allocator, value);
		return ARTKey(data, sizeof(value));
	}

	template <class T>
	static inline ARTKey CreateARTKey(ArenaAllocator &allocator, Value &value) {
		return CreateARTKey(allocator, value.GetValueUnsafe<T>());
	}

	template <class T>
	static inline void CreateARTKey(ArenaAllocator &allocator, ARTKey &key, T value) {
		key.data = ARTKey::CreateData<T>(allocator, value);
		key.len = sizeof(value);
	}

	template <class T>
	static inline void CreateARTKey(ArenaAllocator &allocator, ARTKey &key, Value value) {
		key.data = ARTKey::CreateData<T>(allocator, value.GetValueUnsafe<T>());
		key.len = sizeof(value);
	}

	static ARTKey CreateKey(ArenaAllocator &allocator, PhysicalType type, Value &value);

public:
	data_t &operator[](idx_t i) {
		return data[i];
	}
	const data_t &operator[](idx_t i) const {
		return data[i];
	}
	bool operator>(const ARTKey &key) const;
	bool operator>=(const ARTKey &key) const;
	bool operator==(const ARTKey &key) const;

	inline bool ByteMatches(const ARTKey &other, idx_t depth) const {
		return data[depth] == other[depth];
	}
	inline bool Empty() const {
		return len == 0;
	}

	void Concat(ArenaAllocator &allocator, const ARTKey &other);
	row_t GetRowId() const;
	idx_t GetMismatchPos(const ARTKey &other, const idx_t start) const;

private:
	template <class T>
	static inline data_ptr_t CreateData(ArenaAllocator &allocator, T value) {
		auto data = allocator.Allocate(sizeof(value));
		Radix::EncodeData<T>(data, value);
		return data;
	}
};

template <>
ARTKey ARTKey::CreateARTKey(ArenaAllocator &allocator, string_t value);
template <>
ARTKey ARTKey::CreateARTKey(ArenaAllocator &allocator, const char *value);
template <>
void ARTKey::CreateARTKey(ArenaAllocator &allocator, ARTKey &key, string_t value);

class ARTKeySection {
public:
	ARTKeySection(idx_t start, idx_t end, idx_t depth, data_t byte);
	ARTKeySection(idx_t start, idx_t end, const unsafe_vector<ARTKey> &keys, const ARTKeySection &section);

	idx_t start;
	idx_t end;
	idx_t depth;
	data_t key_byte;

public:
	void GetChildSections(unsafe_vector<ARTKeySection> &sections, const unsafe_vector<ARTKey> &keys);
};
} // namespace duckdb
