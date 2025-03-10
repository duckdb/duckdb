//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/sorting/sort_key.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/typedefs.hpp"

namespace duckdb {

enum class SortKeyType : uint8_t {
	INVALID = 0,
	//! Without payload
	NO_PAYLOAD_FIXED_8 = 1,
	NO_PAYLOAD_FIXED_16 = 2,
	NO_PAYLOAD_FIXED_24 = 3,
	NO_PAYLOAD_FIXED_32 = 4,
	NO_PAYLOAD_VARIABLE_32 = 5,
	//! With payload (requires row pointer in key)
	PAYLOAD_FIXED_16 = 6,
	PAYLOAD_FIXED_24 = 7,
	PAYLOAD_FIXED_32 = 8,
	PAYLOAD_VARIABLE_32 = 9,
};

template <SortKeyType>
struct SortKey {
	static constexpr idx_t HEAP_SIZE_OFFSET = DConstants::INVALID_INDEX;
	static constexpr idx_t ROW_POINTER_OFFSET = DConstants::INVALID_INDEX;
};

template <>
struct SortKey<SortKeyType::NO_PAYLOAD_FIXED_8> {

	//! Normalized key
	uint64_t part1 = 0;
};

template <>
struct SortKey<SortKeyType::NO_PAYLOAD_FIXED_16> {

	//! Normalized key
	uint64_t part1 = 0;
	uint64_t part2 = 0;
};

template <>
struct SortKey<SortKeyType::NO_PAYLOAD_FIXED_24> {

	//! Normalized key
	uint64_t part1 = 0;
	uint64_t part2 = 0;
	uint64_t part3 = 0;
};

template <>
struct SortKey<SortKeyType::NO_PAYLOAD_FIXED_32> {

	//! Normalized key
	uint64_t part1 = 0;
	uint64_t part2 = 0;
	uint64_t part3 = 0;
	uint64_t part4 = 0;
};

template <>
struct SortKey<SortKeyType::NO_PAYLOAD_VARIABLE_32> {
	static constexpr idx_t HEAP_SIZE_OFFSET = 16;

	//! Normalized key
	uint64_t part1 = 0;
	uint64_t part2 = 0;

	//! Remainder lives here
	uint64_t size = 0;
	data_ptr_t data = nullptr;
};

template <>
struct SortKey<SortKeyType::PAYLOAD_FIXED_16> {

	//! Normalized key
	uint64_t part1 = 0;

	//! Pointer to corresponding payload row
	data_ptr_t row_ptr = nullptr;
};

template <>
struct SortKey<SortKeyType::PAYLOAD_FIXED_24> {

	//! Normalized key
	uint64_t part1 = 0;
	uint64_t part2 = 0;

	//! Pointer to corresponding payload row
	data_ptr_t row_ptr = nullptr;
};

template <>
struct SortKey<SortKeyType::PAYLOAD_FIXED_32> {

	//! Normalized key
	uint64_t part1 = 0;
	uint64_t part2 = 0;
	uint64_t part3 = 0;

	//! Pointer to corresponding payload row
	data_ptr_t row_ptr = nullptr;
};

template <>
struct SortKey<SortKeyType::PAYLOAD_VARIABLE_32> {
	static constexpr idx_t HEAP_SIZE_OFFSET = 8;

	//! Normalized key
	uint64_t part1 = 0;

	//! Size/pointer to remainder
	uint64_t size = 0;
	data_ptr_t data = nullptr;

	//! Pointer to corresponding payload row
	data_ptr_t row_ptr = nullptr;
};

static_assert(sizeof(SortKey<SortKeyType::NO_PAYLOAD_FIXED_8>) == 8, "NO_PAYLOAD_FIXED_8 must be 8 wide");
static_assert(sizeof(SortKey<SortKeyType::NO_PAYLOAD_FIXED_16>) == 16, "NO_PAYLOAD_FIXED_16 must be 16 wide");
static_assert(sizeof(SortKey<SortKeyType::NO_PAYLOAD_FIXED_24>) == 24, "NO_PAYLOAD_FIXED_24 must be 24 wide");
static_assert(sizeof(SortKey<SortKeyType::NO_PAYLOAD_FIXED_32>) == 32, "NO_PAYLOAD_FIXED_32 must be 32 wide");
static_assert(sizeof(SortKey<SortKeyType::NO_PAYLOAD_VARIABLE_32>) == 32, "NO_PAYLOAD_VARIABLE_32 must be 32 wide");
static_assert(sizeof(SortKey<SortKeyType::PAYLOAD_FIXED_16>) == 16, "PAYLOAD_FIXED_16 must be 16 wide");
static_assert(sizeof(SortKey<SortKeyType::PAYLOAD_FIXED_24>) == 24, "PAYLOAD_FIXED_24 must be 24 wide");
static_assert(sizeof(SortKey<SortKeyType::PAYLOAD_FIXED_32>) == 32, "PAYLOAD_FIXED_32 must be 32 wide");
static_assert(sizeof(SortKey<SortKeyType::PAYLOAD_VARIABLE_32>) == 32, "PAYLOAD_VARIABLE_32 must be 32 wide");

} // namespace duckdb
