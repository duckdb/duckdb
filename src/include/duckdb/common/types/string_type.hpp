//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/string_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/assert.hpp"
#include "duckdb/common/bswap.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/types/hash.hpp"

#include <cstring>
#include <algorithm>

namespace duckdb {

enum class StringComparisonMode { GENERIC, INLINED, PREFIX };

struct string_t {
	friend struct StringComparisonOperators;

public:
	static constexpr idx_t PREFIX_BYTES = 4 * sizeof(char);
	static constexpr idx_t INLINE_BYTES = 12 * sizeof(char);
	static constexpr idx_t HEADER_SIZE = sizeof(uint32_t) + PREFIX_BYTES;
	static constexpr idx_t MAX_STRING_SIZE = NumericLimits<uint32_t>::Maximum();
#ifndef DUCKDB_DEBUG_NO_INLINE
	static constexpr idx_t PREFIX_LENGTH = PREFIX_BYTES;
	static constexpr idx_t INLINE_LENGTH = INLINE_BYTES;
#else
	static constexpr idx_t PREFIX_LENGTH = 0;
	static constexpr idx_t INLINE_LENGTH = 0;
#endif

	string_t() = default;
	explicit string_t(uint32_t len) {
		value.inlined.length = len;
		memset(value.inlined.inlined, 0, INLINE_BYTES);
	}
	string_t(const char *data, uint32_t len) {
		value.inlined.length = len;
		D_ASSERT(data || GetSize() == 0);
		if (IsInlined()) {
			// zero initialize the prefix first
			// this makes sure that strings with length smaller than 4 still have an equal prefix
			memset(value.inlined.inlined, 0, INLINE_BYTES);
			if (GetSize() == 0) {
				return;
			}
			// small string: inlined
			memcpy(value.inlined.inlined, data, GetSize());
		} else {
			// large string: store pointer
#ifndef DUCKDB_DEBUG_NO_INLINE
			memcpy(value.pointer.prefix, data, PREFIX_LENGTH);
#else
			memset(value.pointer.prefix, 0, PREFIX_BYTES);
#endif
			value.pointer.ptr = (char *)data; // NOLINT
		}
	}

	string_t(const char *data) // NOLINT: Allow implicit conversion from `const char*`
	    : string_t(data, UnsafeNumericCast<uint32_t>(strlen(data))) {
	}
	string_t(const string &value) // NOLINT: Allow implicit conversion from `const char*`
	    : string_t(value.c_str(), UnsafeNumericCast<uint32_t>(value.size())) {
	}

	bool IsInlined() const {
		return GetSize() <= INLINE_LENGTH;
	}

	const char *GetData() const {
		return IsInlined() ? const_char_ptr_cast(value.inlined.inlined) : value.pointer.ptr;
	}
	const char *GetDataUnsafe() const {
		return GetData();
	}

	char *GetDataWriteable() const {
		return IsInlined() ? (char *)value.inlined.inlined : value.pointer.ptr; // NOLINT
	}

	const char *GetPrefix() const {
		return value.inlined.inlined;
	}

	char *GetPrefixWriteable() {
		return value.inlined.inlined;
	}

	uint32_t GetPrefixIntegerComparable() const {
#ifdef DUCKDB_DEBUG_NO_INLINE
		return 0;
#else
		return BSwapIfLE(Load<uint32_t>(const_data_ptr_cast(GetPrefix())));
#endif
	}

	idx_t GetSize() const {
		return value.inlined.length;
	}

	void SetSizeAndFinalize(uint32_t size, idx_t allocated_size) {
		value.inlined.length = size;
		if (allocated_size > INLINE_LENGTH && IsInlined()) {
			//! Data was written to the 'value.pointer.ptr', has to be copied to the inlined bytes
			D_ASSERT(value.pointer.ptr);
			auto ptr = value.pointer.ptr;
			memcpy(GetDataWriteable(), ptr, size);
		}
		Finalize();
	}

	bool Empty() const {
		return value.inlined.length == 0;
	}

	string GetString() const {
		return string(GetData(), GetSize());
	}

	explicit operator string() const {
		return GetString();
	}

	char *GetPointer() const {
		D_ASSERT(!IsInlined());
		return value.pointer.ptr;
	}

	void SetPointer(char *new_ptr) {
		D_ASSERT(!IsInlined());
		value.pointer.ptr = new_ptr;
	}

	void Finalize() {
		// set trailing NULL byte
		if (GetSize() <= INLINE_LENGTH) {
			// fill prefix with zeros if the length is smaller than the prefix length
			memset(value.inlined.inlined + GetSize(), 0, INLINE_BYTES - GetSize());
		} else {
			// copy the data into the prefix
#ifndef DUCKDB_DEBUG_NO_INLINE
			auto dataptr = GetData();
			memcpy(value.pointer.prefix, dataptr, PREFIX_LENGTH);
#else
			memset(value.pointer.prefix, 0, PREFIX_BYTES);
#endif
		}
	}

	void Verify() const;
	//! Verify also in release mode
	void ForceVerify() const;
	void VerifyUTF8() const;
	void VerifyCharacters() const;

	struct StringComparisonOperators {
		static inline bool Equals(const string_t &a, const string_t &b) {
#ifdef DUCKDB_DEBUG_NO_INLINE
			if (a.GetSize() != b.GetSize()) {
				return false;
			}
			return (memcmp(a.GetData(), b.GetData(), a.GetSize()) == 0);
#endif
			uint64_t a_bulk_comp = Load<uint64_t>(const_data_ptr_cast(&a));
			uint64_t b_bulk_comp = Load<uint64_t>(const_data_ptr_cast(&b));
			if (a_bulk_comp != b_bulk_comp) {
				// Either length or prefix are different -> not equal
				return false;
			}
			// they have the same length and same prefix!
			a_bulk_comp = Load<uint64_t>(const_data_ptr_cast(&a) + 8u);
			b_bulk_comp = Load<uint64_t>(const_data_ptr_cast(&b) + 8u);
			if (a_bulk_comp == b_bulk_comp) {
				// either they are both inlined (so compare equal) or point to the same string (so compare equal)
				return true;
			}
			if (!a.IsInlined()) {
				// 'long' strings of the same length -> compare pointed value
				if (memcmp(a.value.pointer.ptr, b.value.pointer.ptr, a.GetSize()) == 0) {
					return true;
				}
			}
			// either they are short string of same length but different content
			//     or they point to string with different content
			//     either way, they can't represent the same underlying string
			return false;
		}
		// compare up to shared length. if still the same, compare lengths
		template <StringComparisonMode MODE = StringComparisonMode::INLINED>
		static bool GreaterThan(const string_t &left, const string_t &right) {
#ifndef DUCKDB_DEBUG_NO_INLINE
			const auto a_prefix = left.GetPrefixIntegerComparable();
			const auto b_prefix = right.GetPrefixIntegerComparable();
			if (a_prefix != b_prefix) {
				return a_prefix > b_prefix;
			}
			if constexpr (MODE == StringComparisonMode::INLINED) {
				if (left.IsInlined() && right.IsInlined()) {
					return InlinedSuffixGreaterThan(left, right);
				}
			} else if constexpr (MODE == StringComparisonMode::PREFIX) {
				// Inline strings are zero-padded; out-of-line strings contain more than twelve bytes.
				const auto a_suffix = BSwapIfLE(Load<uint64_t>(const_data_ptr_cast(left.GetData()) + PREFIX_BYTES));
				const auto b_suffix = BSwapIfLE(Load<uint64_t>(const_data_ptr_cast(right.GetData()) + PREFIX_BYTES));
				if (a_suffix != b_suffix) {
					return a_suffix > b_suffix;
				}
				if (left.IsInlined() || right.IsInlined()) {
					return left.GetSize() > right.GetSize();
				}
				const auto min_length = MinValue(left.GetSize(), right.GetSize());
				const auto cmp =
				    memcmp(left.GetData() + INLINE_BYTES, right.GetData() + INLINE_BYTES, min_length - INLINE_BYTES);
				return cmp > 0 || (cmp == 0 && left.GetSize() > right.GetSize());
			}
#endif
			const auto left_length = left.GetSize();
			const auto right_length = right.GetSize();
			const auto min_length = MinValue(left_length, right_length);
			auto memcmp_res = memcmp(left.GetData(), right.GetData(), min_length);
			return memcmp_res > 0 || (memcmp_res == 0 && left_length > right_length);
		}
		//! Both strings must be inlined and have equal prefixes: the zero-padded inlined bytes are compared as one
		//! big-endian integer, ties are broken by length (e.g. "a" < "a\0")
		static inline bool InlinedSuffixGreaterThan(const string_t &left, const string_t &right) {
			const auto a_suffix = BSwapIfLE(Load<uint64_t>(const_data_ptr_cast(&left) + 8u));
			const auto b_suffix = BSwapIfLE(Load<uint64_t>(const_data_ptr_cast(&right) + 8u));
			return (a_suffix > b_suffix) | ((a_suffix == b_suffix) & (left.GetSize() > right.GetSize()));
		}
		//! At least one side must be inlined: equal strings then have equal (inlined) lengths, so all 16 bytes match
		static inline bool InlinedEquals(const string_t &a, const string_t &b) {
			const auto a_ptr = const_data_ptr_cast(&a);
			const auto b_ptr = const_data_ptr_cast(&b);
			return ((Load<uint64_t>(a_ptr) ^ Load<uint64_t>(b_ptr)) |
			        (Load<uint64_t>(a_ptr + 8u) ^ Load<uint64_t>(b_ptr + 8u))) == 0;
		}
	};

	bool operator==(const string_t &r) const {
		return StringComparisonOperators::Equals(*this, r);
	}

	bool operator!=(const string_t &r) const {
		return !(*this == r);
	}

	bool operator>(const string_t &r) const {
		return StringComparisonOperators::GreaterThan(*this, r);
	}
	bool operator<(const string_t &r) const {
		return r > *this;
	}
	bool operator<=(const string_t &r) const {
		return !(r < *this);
	}
	bool operator>=(const string_t &r) const {
		return !(*this < r);
	}

private:
	union {
		struct {
			uint32_t length;
			char prefix[4];
			char *ptr;
		} pointer;
		struct {
			uint32_t length;
			char inlined[12];
		} inlined;
	} value;
};

} // namespace duckdb

namespace std {
template <>
struct hash<duckdb::string_t> {
	size_t operator()(const duckdb::string_t &val) const {
		return Hash(val);
	}
};
} // namespace std
