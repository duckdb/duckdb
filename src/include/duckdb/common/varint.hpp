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
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/storage/arena_allocator.hpp"

namespace duckdb {

struct varint_t {
	string_t data;

	varint_t() : data() {
	}

	explicit varint_t(const string_t &data) : data(data) {
	}

	varint_t(const varint_t &rhs) = default;
	varint_t(varint_t &&other) = default;
	varint_t &operator=(const varint_t &rhs) = default;
	varint_t &operator=(varint_t &&rhs) = default;

	void Print() const;
};

enum AbsoluteNumberComparison : uint8_t {
	// If number is equal
	EQUAL = 0,
	// If compared number is greater
	GREATER = 1,
	// If compared number is smaller
	SMALLER = 2,
};

struct VarintIntermediate {
	VarintIntermediate() : is_negative(false), size(0), data(nullptr) {};
	explicit VarintIntermediate(const varint_t &value);
	VarintIntermediate(uint8_t *value, idx_t size);
	void Print() const;
	//! Information on the header
	bool is_negative;
	uint32_t size;
	//! The actual data
	data_ptr_t data;
	//! If the absolute number is  bigger than the absolute rhs
	//! 1 = true, 0 = equal, -1 = false
	AbsoluteNumberComparison IsAbsoluteBigger(const VarintIntermediate &rhs) const;
	//! Get the absolute value of a byte
	uint8_t GetAbsoluteByte(int64_t index) const;
	//! If the most significant bit of the first byte is set.
	bool IsMSBSet() const;
	//! Initializes our varint to 0 and 1 byte
	void Initialize(ArenaAllocator &allocator);
	//! If necessary, we reallocate our intermediate to the next power of 2.
	void Reallocate(ArenaAllocator &allocator, idx_t min_size);
	static uint32_t GetStartDataPos(data_ptr_t data, idx_t size, bool is_negative);
	uint32_t GetStartDataPos() const;
	//! In case we have unnecessary extra 0's or 1's in our varint we trim them
	static idx_t Trim(data_ptr_t data, uint32_t &size, bool is_negative);
	void Trim();
	//! Add a VarintIntermediate to another VarintIntermediate, equivalent of a +=
	void AddInPlace(ArenaAllocator &allocator, const VarintIntermediate &rhs);
	//! Adds two VarintIntermediates and returns a string_t result, equivalent of a +
	static string_t Add(Vector &result, const VarintIntermediate &lhs, const VarintIntermediate &rhs);
	//! Negates a value, e.g., -x
	string_t Negate(Vector &result_vector) const;
	void NegateInPlace();
	//! Exports to a varint, either arena allocated
	varint_t ToVarint(ArenaAllocator &allocator);
	//! Check if an over/underflow has occurred
	static bool OverOrUnderflow(data_ptr_t data, idx_t size, bool is_negative);
	bool OverOrUnderflow() const;
};

} // namespace duckdb
