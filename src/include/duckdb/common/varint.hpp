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

struct VarintIntermediate {
	VarintIntermediate() : is_negative(false), size(0), data(nullptr) {};
	explicit VarintIntermediate(const varint_t &value);
	void Print() const;
	//! Information on the header
	bool is_negative;
	uint32_t size;
	//! The actual data
	data_ptr_t data;
	//! If the absolute number is  bigger than the absolute rhs
	//! 1 = true, 0 = equal, -1 = false
	int8_t IsAbsoluteBigger(const VarintIntermediate &rhs) const;
	//! Get the absolute value of a byte
	uint8_t GetAbsoluteByte(int64_t index) const;
	//! If most significant bit of the first byte is set.
	bool IsMSBSet() const;
	//! Initializes our varint to 0 and 1 byte
	void Initialize(ArenaAllocator &allocator);
	//! If necessary, we reallocate our intermediate to the next power of 2.
	void Reallocate(ArenaAllocator &allocator, idx_t min_size);
	idx_t GetStartDataPos() const;
	//! In case we have unnecessary extra 0's or 1's in our varint we trim them
	void Trim();
	//! Add a VarintIntermediate to another VarintIntermediate, equivalent of a +=
	void AddInPlace(ArenaAllocator &allocator, const VarintIntermediate &rhs);
	//! Exports to a varint, either arena allocated
	varint_t ToVarint(ArenaAllocator &allocator);
	//! Or Vector allocated
	string_t ToVarint(Vector &result);
};

struct varint_t {
	string_t data;

	varint_t() : data() {
	}

	varint_t(const varint_t &rhs) = default;
	varint_t(varint_t &&other) = default;
	varint_t &operator=(const varint_t &rhs) = default;
	varint_t &operator=(varint_t &&rhs) = default;

	void Print() const;
};

} // namespace duckdb
