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
	NO_PAYLOAD_FIXED_24 = 3,
	NO_PAYLOAD_FIXED_32 = 4,
	NO_PAYLOAD_VARIABLE_32 = 5,
	//! With payload (requires row pointer in key)
	PAYLOAD_FIXED_16 = 6,
	PAYLOAD_FIXED_24 = 7,
	PAYLOAD_FIXED_32 = 8,
	PAYLOAD_VARIABLE_32 = 9,
};

//! Forces a pointer size of 8 bytes (even on 32-bit)
struct sort_key_ptr_t { // NOLINT: match stl case
	union {
		data_ptr_t ptr;
		uint64_t pad;
	} u;
};

template <SortKeyType>
struct SortKey;

template <class SORT_KEY>
struct SortKeyNoPayload {
protected:
	SortKeyNoPayload() = default; // NOLINT
	friend SORT_KEY;

public:
	static constexpr bool HAS_PAYLOAD = false;

	data_ptr_t GetPayload() const {
		throw InternalException("GetPayload() called on a SortKeyNoPayload");
	}

	void SetPayload(const data_ptr_t &payload) {
		throw InternalException("SetPayload() called on a SortKeyNoPayload");
	}
};

template <class SORT_KEY>
struct SortKeyPayload {
protected:
	SortKeyPayload() = default; // NOLINT
	friend SORT_KEY;

public:
	static constexpr bool HAS_PAYLOAD = true;

	data_ptr_t GetPayload() const {
		auto &sort_key = static_cast<const SORT_KEY &>(*this);
		return sort_key.payload.u.ptr;
	}

	void SetPayload(const data_ptr_t &payload) {
		auto &sort_key = static_cast<SORT_KEY &>(*this);
		sort_key.payload.u.ptr = payload;
	}
};

template <idx_t REMAINING>
static bool SortKeyLessThan(const uint64_t *const &lhs, const uint64_t *const &rhs) {
	return (*lhs < *rhs) || ((*lhs == *rhs) && SortKeyLessThan<REMAINING - 1>(lhs + 1, rhs + 1));
}

template <>
inline bool SortKeyLessThan<1>(const uint64_t *const &lhs, const uint64_t *const &rhs) {
	return *lhs < *rhs;
}

template <class SORT_KEY, bool HAS_PAYLOAD>
struct FixedSortKey : std::conditional<HAS_PAYLOAD, SortKeyPayload<SORT_KEY>, SortKeyNoPayload<SORT_KEY>>::type {
protected:
	FixedSortKey() = default; // NOLINT
	friend SORT_KEY;

public:
	static constexpr bool CONSTANT_SIZE = true;

	void ByteSwap() {
		auto &sort_key = static_cast<SORT_KEY &>(*this);
		for (idx_t i = 0; i < SORT_KEY::PARTS; i++) {
			(&sort_key.part0)[i] = BSwapIfLE((&sort_key.part0)[i]);
		}
	}

	void Construct(const string_t &str, data_ptr_t &) {
		D_ASSERT(str.GetSize() <= SORT_KEY::INLINE_LENGTH);
		auto &sort_key = static_cast<SORT_KEY &>(*this);
		for (idx_t i = 0; i < SORT_KEY::PARTS; i++) {
			(&sort_key.part0)[i] = 0;
		}

		if (SORT_KEY::INLINE_LENGTH <= string_t::INLINE_LENGTH) {
			memcpy(&sort_key.part0, str.GetPrefix(), SORT_KEY::INLINE_LENGTH);
		} else if (str.IsInlined()) {
			memcpy(&sort_key.part0, str.GetPrefix(), string_t::INLINE_LENGTH);
		} else {
			FastMemcpy(&sort_key.part0, str.GetPointer(), str.GetSize());
		}

		// IMPORTANT NOTE: We don't actually store the data in byte-comparable order!
		// This allows us to do int64_t comparisons, yielding better performance.
		// This means we have to ByteSwap once more when decoding the keys later.
		ByteSwap();
	}

	void Construct(const int64_t &val, data_ptr_t &) {
		auto &sort_key = static_cast<SORT_KEY &>(*this);
		sort_key.part0 = static_cast<uint64_t>(val); // NOLINT: unsafe cast on purpose
	}

	void Deconstruct(string_t &val) {
		auto &sort_key = static_cast<SORT_KEY &>(*this);
		ByteSwap();
		val = string_t(const_char_ptr_cast(&sort_key.part0), UnsafeNumericCast<uint32_t>(SORT_KEY::INLINE_LENGTH));
	}

	void Deconstruct(int64_t &val) {
		auto &sort_key = static_cast<SORT_KEY &>(*this);
		val = static_cast<int64_t>(sort_key.part0); // NOLINT: unsafe cast on purpose
	}

	data_ptr_t GetData() const {
		throw InternalException("GetData() called on a FixedSortKey");
	}

	idx_t GetSize() const {
		throw InternalException("GetSize() called on a FixedSortKey");
	}

	idx_t GetHeapSize() const {
		throw InternalException("GetHeapSize() called on a FixedSortKey");
	}

	friend bool operator<(const SORT_KEY &lhs, const SORT_KEY &rhs) {
		return SortKeyLessThan<SORT_KEY::PARTS>(&lhs.part0, &rhs.part0);
	}
};

template <class SORT_KEY, bool HAS_PAYLOAD>
struct VariableSortKey : std::conditional<HAS_PAYLOAD, SortKeyPayload<SORT_KEY>, SortKeyNoPayload<SORT_KEY>>::type {
protected:
	VariableSortKey() = default; // NOLINT
	friend SORT_KEY;

public:
	static constexpr bool CONSTANT_SIZE = false;

	void ByteSwap() {
		auto &sort_key = static_cast<SORT_KEY &>(*this);
		for (idx_t i = 0; i < SORT_KEY::PARTS; i++) {
			(&sort_key.part0)[i] = BSwapIfLE((&sort_key.part0)[i]);
		}
	}

	void Construct(const string_t &val, data_ptr_t &heap_ptr) {
		auto &sort_key = static_cast<SORT_KEY &>(*this);
		// Initialize to 0's (including size by doing + 1)
		for (idx_t i = 0; i < SORT_KEY::PARTS + 1; i++) {
			(&sort_key.part0)[i] = 0;
		}

		// Deal with inlined part first
		memcpy(&sort_key.part0, val.GetData(), MinValue(val.GetSize(), SORT_KEY::INLINE_LENGTH));
		// Same as FixedSortKey, we do not store the data in byte-comparable order
		ByteSwap();

		// Deal with non-inlined part (if necessary)
		if (val.GetSize() > SORT_KEY::INLINE_LENGTH) {
			sort_key.size = val.GetSize();
			sort_key.data.u.ptr = heap_ptr;
			memcpy(sort_key.data.u.ptr, val.GetData(), sort_key.size);
			heap_ptr += sort_key.size;
		}
	}

	void Construct(const int64_t &, data_ptr_t &) {
		throw InternalException("VariableSortKey::Construct() called with an int64_t");
	}

	void Deconstruct(string_t &val) {
		auto &sort_key = static_cast<SORT_KEY &>(*this);
		if (sort_key.size > SORT_KEY::INLINE_LENGTH) {
			val = string_t(const_char_ptr_cast(sort_key.data.u.ptr), UnsafeNumericCast<uint32_t>(sort_key.size));
		} else {
			ByteSwap();
			val = string_t(const_char_ptr_cast(&sort_key.part0), UnsafeNumericCast<uint32_t>(SORT_KEY::INLINE_LENGTH));
		}
	}

	void Deconstruct(int64_t &) {
		throw InternalException("VariableSortKey::Deconstruct() called with an int64_t");
	}

	data_ptr_t GetData() const {
		auto &sort_key = static_cast<const SORT_KEY &>(*this);
		return sort_key.data.u.ptr;
	}

	void SetData(const data_ptr_t &data) {
		auto &sort_key = static_cast<SORT_KEY &>(*this);
		sort_key.data.u.ptr = data;
	}

	idx_t GetSize() const {
		auto &sort_key = static_cast<const SORT_KEY &>(*this);
		return MaxValue(sort_key.size, SORT_KEY::INLINE_LENGTH);
	}

	idx_t GetHeapSize() const {
		auto &sort_key = static_cast<const SORT_KEY &>(*this);
		return sort_key.size;
	}

	static int32_t TernaryCompare(const uint64_t &lhs, const uint64_t &rhs) {
		return (lhs > rhs) - (lhs < rhs);
	}

	static int32_t LessThan(const uint64_t *const &lhs, const uint64_t *const &rhs) {
		switch (SORT_KEY::PARTS) {
		case 1:
			return TernaryCompare(lhs[0], rhs[0]);
		case 2:
			return 2 * TernaryCompare(lhs[0], rhs[0]) + TernaryCompare(lhs[1], rhs[1]);
		default:
			throw NotImplementedException("VariableSortKey::LessThan for %llu parts", SORT_KEY::PARTS);
		}
	}

	friend bool operator<(const SORT_KEY &lhs, const SORT_KEY &rhs) {
		auto comp_res = LessThan(&lhs.part0, &rhs.part0);
		// If inlined is not equal, we can return already
		if (comp_res != 0) {
			return comp_res < 0;
		}

		// Inlined is equal. If either string is inlined, it is considered "less than"
		if (lhs.size <= SORT_KEY::INLINE_LENGTH || rhs.size <= SORT_KEY::INLINE_LENGTH) {
			return lhs.size < rhs.size;
		}

		// Both strings are non-inlined
		comp_res = memcmp(lhs.data.u.ptr, rhs.data.u.ptr, MinValue(lhs.size, rhs.size));
		return comp_res < 0 || (comp_res == 0 && lhs.size < rhs.size);
	}
};

template <>
struct SortKey<SortKeyType::NO_PAYLOAD_FIXED_8> : FixedSortKey<SortKey<SortKeyType::NO_PAYLOAD_FIXED_8>, false> {
	static constexpr idx_t PARTS = 1;
	static constexpr idx_t INLINE_LENGTH = 8;
	uint64_t part0;
};

template <>
struct SortKey<SortKeyType::NO_PAYLOAD_FIXED_16> : FixedSortKey<SortKey<SortKeyType::NO_PAYLOAD_FIXED_16>, false> {
	static constexpr idx_t PARTS = 2;
	static constexpr idx_t INLINE_LENGTH = 16;
	uint64_t part0;
	uint64_t part1;
};

template <>
struct SortKey<SortKeyType::NO_PAYLOAD_FIXED_24> : FixedSortKey<SortKey<SortKeyType::NO_PAYLOAD_FIXED_24>, false> {
	static constexpr idx_t PARTS = 3;
	static constexpr idx_t INLINE_LENGTH = 24;
	uint64_t part0;
	uint64_t part1;
	uint64_t part2;
};

template <>
struct SortKey<SortKeyType::NO_PAYLOAD_FIXED_32> : FixedSortKey<SortKey<SortKeyType::NO_PAYLOAD_FIXED_32>, false> {
	static constexpr idx_t PARTS = 4;
	static constexpr idx_t INLINE_LENGTH = 32;
	uint64_t part0;
	uint64_t part1;
	uint64_t part2;
	uint64_t part3;
};

template <>
struct SortKey<SortKeyType::NO_PAYLOAD_VARIABLE_32>
    : VariableSortKey<SortKey<SortKeyType::NO_PAYLOAD_VARIABLE_32>, false> {
	static constexpr idx_t PARTS = 2;
	static constexpr idx_t INLINE_LENGTH = 16;
	static constexpr idx_t HEAP_SIZE_OFFSET = 16;
	uint64_t part0;
	uint64_t part1;
	uint64_t size;
	sort_key_ptr_t data;
};

template <>
struct SortKey<SortKeyType::PAYLOAD_FIXED_16> : FixedSortKey<SortKey<SortKeyType::PAYLOAD_FIXED_16>, true> {
	static constexpr idx_t PARTS = 1;
	static constexpr idx_t INLINE_LENGTH = 8;
	uint64_t part0;
	sort_key_ptr_t payload;
};

template <>
struct SortKey<SortKeyType::PAYLOAD_FIXED_24> : FixedSortKey<SortKey<SortKeyType::PAYLOAD_FIXED_24>, true> {
	static constexpr idx_t PARTS = 2;
	static constexpr idx_t INLINE_LENGTH = 16;
	uint64_t part0;
	uint64_t part1;
	sort_key_ptr_t payload;
};

template <>
struct SortKey<SortKeyType::PAYLOAD_FIXED_32> : FixedSortKey<SortKey<SortKeyType::PAYLOAD_FIXED_32>, true> {
	static constexpr idx_t PARTS = 3;
	static constexpr idx_t INLINE_LENGTH = 24;
	uint64_t part0;
	uint64_t part1;
	uint64_t part2;
	sort_key_ptr_t payload;
};

template <>
struct SortKey<SortKeyType::PAYLOAD_VARIABLE_32> : VariableSortKey<SortKey<SortKeyType::PAYLOAD_VARIABLE_32>, true> {
	static constexpr idx_t PARTS = 1;
	static constexpr idx_t INLINE_LENGTH = 8;
	static constexpr idx_t HEAP_SIZE_OFFSET = 8;
	uint64_t part0;
	uint64_t size;
	sort_key_ptr_t data;
	sort_key_ptr_t payload;
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
		case SortKeyType::NO_PAYLOAD_FIXED_8:
			return SortKey<SortKeyType::NO_PAYLOAD_FIXED_8>::CONSTANT_SIZE;
		case SortKeyType::NO_PAYLOAD_FIXED_16:
			return SortKey<SortKeyType::NO_PAYLOAD_FIXED_16>::CONSTANT_SIZE;
		case SortKeyType::NO_PAYLOAD_FIXED_24:
			return SortKey<SortKeyType::NO_PAYLOAD_FIXED_24>::CONSTANT_SIZE;
		case SortKeyType::NO_PAYLOAD_FIXED_32:
			return SortKey<SortKeyType::NO_PAYLOAD_FIXED_32>::CONSTANT_SIZE;
		case SortKeyType::NO_PAYLOAD_VARIABLE_32:
			return SortKey<SortKeyType::NO_PAYLOAD_VARIABLE_32>::CONSTANT_SIZE;
		case SortKeyType::PAYLOAD_FIXED_16:
			return SortKey<SortKeyType::PAYLOAD_FIXED_16>::CONSTANT_SIZE;
		case SortKeyType::PAYLOAD_FIXED_24:
			return SortKey<SortKeyType::PAYLOAD_FIXED_24>::CONSTANT_SIZE;
		case SortKeyType::PAYLOAD_FIXED_32:
			return SortKey<SortKeyType::PAYLOAD_FIXED_32>::CONSTANT_SIZE;
		case SortKeyType::PAYLOAD_VARIABLE_32:
			return SortKey<SortKeyType::PAYLOAD_VARIABLE_32>::CONSTANT_SIZE;
		default:
			throw NotImplementedException("SortKeyUtils::IsConstantSize for %s", EnumUtil::ToString(sort_key_type));
		}
	}

	static bool HasPayload(const SortKeyType sort_key_type) {
		switch (sort_key_type) {
		case SortKeyType::NO_PAYLOAD_FIXED_8:
			return SortKey<SortKeyType::NO_PAYLOAD_FIXED_8>::HAS_PAYLOAD;
		case SortKeyType::NO_PAYLOAD_FIXED_16:
			return SortKey<SortKeyType::NO_PAYLOAD_FIXED_16>::HAS_PAYLOAD;
		case SortKeyType::NO_PAYLOAD_FIXED_24:
			return SortKey<SortKeyType::NO_PAYLOAD_FIXED_24>::HAS_PAYLOAD;
		case SortKeyType::NO_PAYLOAD_FIXED_32:
			return SortKey<SortKeyType::NO_PAYLOAD_FIXED_32>::HAS_PAYLOAD;
		case SortKeyType::NO_PAYLOAD_VARIABLE_32:
			return SortKey<SortKeyType::NO_PAYLOAD_VARIABLE_32>::HAS_PAYLOAD;
		case SortKeyType::PAYLOAD_FIXED_16:
			return SortKey<SortKeyType::PAYLOAD_FIXED_16>::HAS_PAYLOAD;
		case SortKeyType::PAYLOAD_FIXED_24:
			return SortKey<SortKeyType::PAYLOAD_FIXED_24>::HAS_PAYLOAD;
		case SortKeyType::PAYLOAD_FIXED_32:
			return SortKey<SortKeyType::PAYLOAD_FIXED_32>::HAS_PAYLOAD;
		case SortKeyType::PAYLOAD_VARIABLE_32:
			return SortKey<SortKeyType::PAYLOAD_VARIABLE_32>::HAS_PAYLOAD;
		default:
			throw NotImplementedException("SortKeyUtils::HasPayload for %s", EnumUtil::ToString(sort_key_type));
		}
	}
};

} // namespace duckdb
