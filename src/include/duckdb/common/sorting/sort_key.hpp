//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/sorting/sort_key.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/bswap.hpp"
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
	NO_PAYLOAD_FIXED_32 = 3,
	NO_PAYLOAD_VARIABLE_32 = 4,
	//! With payload (requires row pointer in key)
	PAYLOAD_FIXED_16 = 5,
	PAYLOAD_FIXED_32 = 6,
	PAYLOAD_VARIABLE_32 = 7,
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
			memcpy(&sort_key.part0, str.GetPrefix(), SORT_KEY::INLINE_LENGTH);
			return;
		}
		for (idx_t i = 0; i < SORT_KEY::PARTS; i++) {
			(&sort_key.part0)[i] = 0;
		}
		if (str.IsInlined()) {
			memcpy(&sort_key.part0, str.GetPrefix(), string_t::INLINE_LENGTH);
		} else {
			FastMemcpy(&sort_key.part0, str.GetPointer(), str.GetSize());
		}
	}

	void Construct(const int64_t &val, data_ptr_t &) {
		auto &sort_key = static_cast<SORT_KEY &>(*this);
		sort_key.part0 = static_cast<uint64_t>(BSwap(val)); // NOLINT: unsafe cast on purpose
	}

	static bool LessThan(const uint64_t *const &lhs, const uint64_t *const &rhs) {
		if (SORT_KEY::PARTS == 1) {
			return BSwap(lhs[0]) < BSwap(rhs[0]);
		}
		if (SORT_KEY::PARTS == 2) {
			const auto lhs_part0 = BSwap(lhs[0]);
			const auto rhs_part0 = BSwap(rhs[0]);
			return lhs_part0 == rhs_part0 ? BSwap(lhs[1]) < BSwap(rhs[1]) : lhs_part0 < rhs_part0;
		}
		// TODO: Check if the above approach is better than memcmp for 3/4 parts
		return memcmp(lhs, rhs, SORT_KEY::INLINE_LENGTH) < 0;
	}

	friend bool operator<(const SORT_KEY &lhs, const SORT_KEY &rhs) {
		return LessThan(&lhs.part0, &rhs.part0);
	}
};

template <class SORT_KEY>
struct VariableSortKey {
private:
	VariableSortKey() = default;
	friend SORT_KEY;

public:
	void Construct(const string_t &val, data_ptr_t &heap_ptr) {
		auto &sort_key = static_cast<SORT_KEY &>(*this);
		for (idx_t i = 0; i < SORT_KEY::PARTS; i++) {
			(&sort_key.part0)[i] = 0;
		}
		sort_key.size = val.GetSize();
		const auto str_ptr = val.GetData();
		memcpy(&sort_key.part0, str_ptr, sort_key.size);
		if (sort_key.size > SORT_KEY::INLINE_LENGTH) {
			sort_key.data = heap_ptr;
			memcpy(sort_key.data, str_ptr, sort_key.size);
			heap_ptr += sort_key.size;
		}
	}

	void Construct(const int64_t &val, data_ptr_t &) {
		throw InternalException("VariableSortKey::Construct() called with an int64_t");
	}

	friend bool operator<(const SORT_KEY &lhs, const SORT_KEY &rhs) {
		auto comp_res = memcmp(&lhs.part0, &rhs.part0, SORT_KEY::INLINE_LENGTH);
		// If inlined is not equal, we can return already
		if (comp_res != 0) {
			return comp_res;
		}

		// Inlined is equal. If either string is inlined, it is considered "less than"
		if (lhs.size <= SORT_KEY::INLINE_LENGTH || rhs.size <= SORT_KEY::INLINE_LENGTH) {
			return lhs.size < rhs.size;
		}

		// Both strings are non-inlined
		comp_res = memcmp(lhs.data, rhs.data, MinValue(lhs.size, rhs.size));
		return comp_res < 0 || (comp_res == 0 && lhs.size < rhs.size);
	}
};

template <class SORT_KEY>
struct SortKeyNoPayload {
private:
	SortKeyNoPayload() = default;
	friend SORT_KEY;

public:
	data_ptr_t GetPayloadPointer() const {
		throw InternalException("SortKeyNoPayload::GetPayloadPointer() called on a SortKeyNoPayload");
	}
};

template <class SORT_KEY>
struct SortKeyPayload {
private:
	SortKeyPayload() = default;
	friend SORT_KEY;

public:
	data_ptr_t GetPayloadPointer() const {
		auto &sort_key = static_cast<const SORT_KEY &>(*this);
		return sort_key.payload_ptr;
	}
};

template <SortKeyType>
struct SortKey;

template <>
struct SortKey<SortKeyType::NO_PAYLOAD_FIXED_8> : FixedSortKey<SortKey<SortKeyType::NO_PAYLOAD_FIXED_8>>,
                                                  SortKeyNoPayload<SortKey<SortKeyType::NO_PAYLOAD_FIXED_8>> {
	static constexpr idx_t PARTS = 1;
	static constexpr idx_t INLINE_LENGTH = 8;
	uint64_t part0 = 0;
};

template <>
struct SortKey<SortKeyType::NO_PAYLOAD_FIXED_16> : FixedSortKey<SortKey<SortKeyType::NO_PAYLOAD_FIXED_16>>,
                                                   SortKeyNoPayload<SortKey<SortKeyType::NO_PAYLOAD_FIXED_16>> {
	static constexpr idx_t PARTS = 2;
	static constexpr idx_t INLINE_LENGTH = 16;
	uint64_t part0 = 0;
	uint64_t part1 = 0;
};

template <>
struct SortKey<SortKeyType::NO_PAYLOAD_FIXED_32> : FixedSortKey<SortKey<SortKeyType::NO_PAYLOAD_FIXED_32>>,
                                                   SortKeyNoPayload<SortKey<SortKeyType::NO_PAYLOAD_FIXED_32>> {
	static constexpr idx_t PARTS = 4;
	static constexpr idx_t INLINE_LENGTH = 32;
	uint64_t part0 = 0;
	uint64_t part1 = 0;
	uint64_t part2 = 0;
	uint64_t part3 = 0;
};

template <>
struct SortKey<SortKeyType::NO_PAYLOAD_VARIABLE_32> : VariableSortKey<SortKey<SortKeyType::NO_PAYLOAD_VARIABLE_32>>,
                                                      SortKeyNoPayload<SortKey<SortKeyType::NO_PAYLOAD_VARIABLE_32>> {
	static constexpr idx_t PARTS = 2;
	static constexpr idx_t INLINE_LENGTH = 16;
	static constexpr idx_t HEAP_SIZE_OFFSET = 16;
	uint64_t part0 = 0;
	uint64_t part1 = 0;
	uint64_t size = 0;
	data_ptr_t data = nullptr;
};

template <>
struct SortKey<SortKeyType::PAYLOAD_FIXED_16> : FixedSortKey<SortKey<SortKeyType::PAYLOAD_FIXED_16>>,
                                                SortKeyPayload<SortKey<SortKeyType::PAYLOAD_FIXED_16>> {
	static constexpr idx_t PARTS = 1;
	static constexpr idx_t INLINE_LENGTH = 8;
	uint64_t part0 = 0;
	data_ptr_t payload_ptr = nullptr;
};

template <>
struct SortKey<SortKeyType::PAYLOAD_FIXED_32> : FixedSortKey<SortKey<SortKeyType::PAYLOAD_FIXED_32>>,
                                                SortKeyPayload<SortKey<SortKeyType::PAYLOAD_FIXED_32>> {
	static constexpr idx_t PARTS = 3;
	static constexpr idx_t INLINE_LENGTH = 24;
	uint64_t part0 = 0;
	uint64_t part1 = 0;
	uint64_t part2 = 0;
	data_ptr_t payload_ptr = nullptr;
};

template <>
struct SortKey<SortKeyType::PAYLOAD_VARIABLE_32> : VariableSortKey<SortKey<SortKeyType::PAYLOAD_VARIABLE_32>>,
                                                   SortKeyPayload<SortKey<SortKeyType::PAYLOAD_VARIABLE_32>> {
	static constexpr idx_t PARTS = 1;
	static constexpr idx_t INLINE_LENGTH = 8;
	static constexpr idx_t HEAP_SIZE_OFFSET = 8;
	uint64_t part0 = 0;
	uint64_t size = 0;
	data_ptr_t data = nullptr;
	data_ptr_t payload_ptr = nullptr;
};

static_assert(sizeof(SortKey<SortKeyType::NO_PAYLOAD_FIXED_8>) == 8, "NO_PAYLOAD_FIXED_8 must be 8 wide");
static_assert(sizeof(SortKey<SortKeyType::NO_PAYLOAD_FIXED_16>) == 16, "NO_PAYLOAD_FIXED_16 must be 16 wide");
static_assert(sizeof(SortKey<SortKeyType::NO_PAYLOAD_FIXED_32>) == 32, "NO_PAYLOAD_FIXED_32 must be 32 wide");
static_assert(sizeof(SortKey<SortKeyType::NO_PAYLOAD_VARIABLE_32>) == 32, "NO_PAYLOAD_VARIABLE_32 must be 32 wide");
static_assert(sizeof(SortKey<SortKeyType::PAYLOAD_FIXED_16>) == 16, "PAYLOAD_FIXED_16 must be 16 wide");
static_assert(sizeof(SortKey<SortKeyType::PAYLOAD_FIXED_32>) == 32, "PAYLOAD_FIXED_32 must be 32 wide");
static_assert(sizeof(SortKey<SortKeyType::PAYLOAD_VARIABLE_32>) == 32, "PAYLOAD_VARIABLE_32 must be 32 wide");

struct SortKeyUtils {
	static idx_t GetInlineLength(const SortKeyType sort_key_type) {
		switch (sort_key_type) {
		case SortKeyType::NO_PAYLOAD_FIXED_8:
			return SortKey<SortKeyType::NO_PAYLOAD_FIXED_8>::INLINE_LENGTH;
		case SortKeyType::NO_PAYLOAD_FIXED_16:
			return SortKey<SortKeyType::NO_PAYLOAD_FIXED_16>::INLINE_LENGTH;
		case SortKeyType::NO_PAYLOAD_FIXED_32:
			return SortKey<SortKeyType::NO_PAYLOAD_FIXED_32>::INLINE_LENGTH;
		case SortKeyType::NO_PAYLOAD_VARIABLE_32:
			return SortKey<SortKeyType::NO_PAYLOAD_VARIABLE_32>::INLINE_LENGTH;
		case SortKeyType::PAYLOAD_FIXED_16:
			return SortKey<SortKeyType::PAYLOAD_FIXED_16>::INLINE_LENGTH;
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
		case SortKeyType::PAYLOAD_FIXED_32:
		case SortKeyType::PAYLOAD_VARIABLE_32:
			return true;
		default:
			return false;
		}
	}
};

} // namespace duckdb
