//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/string_heap.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/storage/arena_allocator.hpp"

namespace duckdb {
//! A string heap is the owner of a set of strings, strings can be inserted into
//! it On every insert, a pointer to the inserted string is returned The
//! returned pointer will remain valid until the StringHeap is destroyed
class StringHeap {
public:
	DUCKDB_API StringHeap(Allocator &allocator = Allocator::DefaultAllocator());

	DUCKDB_API void Destroy();
	DUCKDB_API void Move(StringHeap &other);

	//! Add a string to the string heap, returns a pointer to the string
	DUCKDB_API string_t AddString(const char *data, idx_t len);
	//! Add a string to the string heap, returns a pointer to the string
	DUCKDB_API string_t AddString(const char *data);
	//! Add a string to the string heap, returns a pointer to the string
	DUCKDB_API string_t AddString(const string &data);
	//! Add a string to the string heap, returns a pointer to the string
	DUCKDB_API string_t AddString(const string_t &data);
	//! Add a blob to the string heap; blobs can be non-valid UTF8
	DUCKDB_API string_t AddBlob(const string_t &data);
	//! Add a blob to the string heap; blobs can be non-valid UTF8
	DUCKDB_API string_t AddBlob(const char *data, idx_t len);
	//! Allocates space for an empty string of size "len" on the heap
	DUCKDB_API string_t EmptyString(idx_t len);

	//! Size of strings
	DUCKDB_API idx_t SizeInBytes() const;

private:
	ArenaAllocator allocator;
};

} // namespace duckdb
