//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/varint.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/winapi.hpp"
#include "duckdb/common/string.hpp"
#include <stdint.h>

namespace duckdb {

struct varint_t {
	string_t data;

	varint_t() : data() {
	}

	varint_t(const varint_t &rhs) = default;
	varint_t(varint_t &&other) = default;
	varint_t &operator=(const varint_t &rhs) = default;
	varint_t &operator=(varint_t &&rhs) = default;

	//! Reallocate the Varint 2x-ing its size
	void Reallocate(ArenaAllocator &allocator, idx_t min_size);

	void AddInPlace(ArenaAllocator &allocator, const varint_t &rhs);
	//! In case we have unnecessary extra 0's or 1's in our varint we trim them
	void Trim(ArenaAllocator &allocator);
	idx_t GetStartDataPos() const;

	void Print() const;
};

} // namespace duckdb
