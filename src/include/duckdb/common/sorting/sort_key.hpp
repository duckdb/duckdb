//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/sorting/sort_key.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/fast_mem.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types/string_type.hpp"

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

template <class SORT_KEY>
struct FixedSortKey {
private:
	FixedSortKey() = default;
	friend SORT_KEY;

public:
	void Construct(const string_t &str, data_ptr_t &) {
		D_ASSERT(str.GetSize() <= SORT_KEY::INLINE_LENGTH);
		auto &sort_key = static_cast<SORT_KEY &>(*this);
		if (SORT_KEY::INLINE_LENGTH <= string_t::INLINE_LENGTH) {
			memcpy(&sort_key.part1, str.GetPrefix(), SORT_KEY::INLINE_LENGTH);
		} else if (str.IsInlined()) {
			memcpy(&sort_key.part1, str.GetPrefix(), string_t::INLINE_LENGTH);
		} else {
			FastMemcpy(&sort_key.part1, str.GetPointer(), str.GetSize());
		}
	}

	static int32_t Compare(const SORT_KEY &lhs, const SORT_KEY &rhs) {
		return memcmp(&lhs.part1, &rhs.part1, SORT_KEY::INLINE_LENGTH);
	}

	friend bool operator<(const SORT_KEY &lhs, const SORT_KEY &rhs) {
		return Compare(lhs, rhs) < 0;
	}
};

template <class SORT_KEY>
struct VariableSortKey {
private:
	VariableSortKey() = default;
	friend SORT_KEY;

public:
	void Construct(const string_t &str, data_ptr_t &heap_ptr) {
		auto &sort_key = static_cast<SORT_KEY &>(*this);
		sort_key.size = str.GetSize();
		const auto str_ptr = str.GetData();
		memcpy(&sort_key.part1, str_ptr, sort_key.size);
		if (sort_key.size > SORT_KEY::INLINE_LENGTH) {
			sort_key.data = heap_ptr;
			memcpy(sort_key.data, str_ptr, sort_key.size);
			heap_ptr += sort_key.size;
		}
	}

	static int32_t Compare(const SORT_KEY &lhs, const SORT_KEY &rhs) {
		auto result = memcmp(&lhs.part1, &rhs.part1, SORT_KEY::INLINE_LENGTH);
		if (result == 0) {
			result = memcmp(lhs.data, rhs.data, MinValue(lhs.size, rhs.size));
		}
		return result;
	}

	friend bool operator<(const SORT_KEY &lhs, const SORT_KEY &rhs) {
		return Compare(lhs, rhs) < 0;
	}
};

template <SortKeyType>
struct SortKey;

template <>
struct SortKey<SortKeyType::NO_PAYLOAD_FIXED_8> : FixedSortKey<SortKey<SortKeyType::NO_PAYLOAD_FIXED_8>> {
	static constexpr idx_t INLINE_LENGTH = 8;
	uint64_t part1 = 0;
};

template <>
struct SortKey<SortKeyType::NO_PAYLOAD_FIXED_16> : FixedSortKey<SortKey<SortKeyType::NO_PAYLOAD_FIXED_16>> {
	static constexpr idx_t INLINE_LENGTH = 16;
	uint64_t part1 = 0;
	uint64_t part2 = 0;
};

template <>
struct SortKey<SortKeyType::NO_PAYLOAD_FIXED_24> : FixedSortKey<SortKey<SortKeyType::NO_PAYLOAD_FIXED_24>> {
	static constexpr idx_t INLINE_LENGTH = 24;
	uint64_t part1 = 0;
	uint64_t part2 = 0;
	uint64_t part3 = 0;
};

template <>
struct SortKey<SortKeyType::NO_PAYLOAD_FIXED_32> : FixedSortKey<SortKey<SortKeyType::NO_PAYLOAD_FIXED_32>> {
	static constexpr idx_t INLINE_LENGTH = 32;
	uint64_t part1 = 0;
	uint64_t part2 = 0;
	uint64_t part3 = 0;
	uint64_t part4 = 0;
};

template <>
struct SortKey<SortKeyType::NO_PAYLOAD_VARIABLE_32> : VariableSortKey<SortKey<SortKeyType::NO_PAYLOAD_VARIABLE_32>> {
	static constexpr idx_t INLINE_LENGTH = 16;
	static constexpr idx_t HEAP_SIZE_OFFSET = 16;
	uint64_t part1 = 0;
	uint64_t part2 = 0;
	uint64_t size = 0;
	data_ptr_t data = nullptr;
};

template <>
struct SortKey<SortKeyType::PAYLOAD_FIXED_16> : FixedSortKey<SortKey<SortKeyType::PAYLOAD_FIXED_16>> {
	static constexpr idx_t INLINE_LENGTH = 8;
	uint64_t part1 = 0;
	data_ptr_t payload_ptr = nullptr;
};

template <>
struct SortKey<SortKeyType::PAYLOAD_FIXED_24> : FixedSortKey<SortKey<SortKeyType::PAYLOAD_FIXED_24>> {
	static constexpr idx_t INLINE_LENGTH = 16;
	uint64_t part1 = 0;
	uint64_t part2 = 0;
	data_ptr_t payload_ptr = nullptr;
};

template <>
struct SortKey<SortKeyType::PAYLOAD_FIXED_32> : FixedSortKey<SortKey<SortKeyType::PAYLOAD_FIXED_32>> {
	static constexpr idx_t INLINE_LENGTH = 24;
	uint64_t part1 = 0;
	uint64_t part2 = 0;
	uint64_t part3 = 0;
	data_ptr_t payload_ptr = nullptr;
};

template <>
struct SortKey<SortKeyType::PAYLOAD_VARIABLE_32> : VariableSortKey<SortKey<SortKeyType::PAYLOAD_VARIABLE_32>> {
	static constexpr idx_t INLINE_LENGTH = 8;
	static constexpr idx_t HEAP_SIZE_OFFSET = 8;
	uint64_t part1 = 0;
	uint64_t size = 0;
	data_ptr_t data = nullptr;
	data_ptr_t payload_ptr = nullptr;
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

struct SortKeyUtils {
	static idx_t GetInlineLength(const SortKeyType sort_key_type) {
		switch (sort_key_type) {
		case SortKeyType::NO_PAYLOAD_FIXED_8:
			return SortKey<SortKeyType::NO_PAYLOAD_FIXED_8>::INLINE_LENGTH;
		case SortKeyType::NO_PAYLOAD_FIXED_16:
			return SortKey<SortKeyType::NO_PAYLOAD_FIXED_16>::INLINE_LENGTH;
		case SortKeyType::NO_PAYLOAD_FIXED_24:
			return SortKey<SortKeyType::NO_PAYLOAD_FIXED_24>::INLINE_LENGTH;
		case SortKeyType::NO_PAYLOAD_FIXED_32:
			return SortKey<SortKeyType::NO_PAYLOAD_FIXED_32>::INLINE_LENGTH;
		case SortKeyType::NO_PAYLOAD_VARIABLE_32:
			return SortKey<SortKeyType::NO_PAYLOAD_VARIABLE_32>::INLINE_LENGTH;
		case SortKeyType::PAYLOAD_FIXED_16:
			return SortKey<SortKeyType::PAYLOAD_FIXED_16>::INLINE_LENGTH;
		case SortKeyType::PAYLOAD_FIXED_24:
			return SortKey<SortKeyType::PAYLOAD_FIXED_24>::INLINE_LENGTH;
		case SortKeyType::PAYLOAD_FIXED_32:
			return SortKey<SortKeyType::PAYLOAD_FIXED_32>::INLINE_LENGTH;
		case SortKeyType::PAYLOAD_VARIABLE_32:
			return SortKey<SortKeyType::PAYLOAD_VARIABLE_32>::INLINE_LENGTH;
		default:
			throw NotImplementedException("SortKeyUtils::GetInlineLength for %s", EnumUtil::ToString(sort_key_type));
		}
	}

	static bool IsConstantSize(const SortKeyType sort_key_type) {
		switch (sort_key_type) {
		case SortKeyType::NO_PAYLOAD_VARIABLE_32:
		case SortKeyType::PAYLOAD_VARIABLE_32:
			return false;
		default:
			return true;
		}
	}

	static bool HasPayload(const SortKeyType sort_key_type) {
		switch (sort_key_type) {
		case SortKeyType::PAYLOAD_FIXED_16:
		case SortKeyType::PAYLOAD_FIXED_24:
		case SortKeyType::PAYLOAD_FIXED_32:
		case SortKeyType::PAYLOAD_VARIABLE_32:
			return true;
		default:
			return false;
		}
	}
};

} // namespace duckdb
