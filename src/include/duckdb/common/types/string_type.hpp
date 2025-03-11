//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/string_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/assert.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/types/hash.hpp"

#include <cstring>
#include <algorithm>

namespace duckdb {

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

	idx_t GetSize() const {
		return value.inlined.length;
	}

	void SetSizeAndFinalize(uint32_t size) {
		value.inlined.length = size;
		Finalize();
		VerifyCharacters();
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
	void VerifyUTF8() const;
	void VerifyCharacters() const;
	void VerifyNull() const;

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
		static bool GreaterThan(const string_t &left, const string_t &right) {
			const uint32_t left_length = UnsafeNumericCast<uint32_t>(left.GetSize());
			const uint32_t right_length = UnsafeNumericCast<uint32_t>(right.GetSize());
			const uint32_t min_length = std::min<uint32_t>(left_length, right_length);

#ifndef DUCKDB_DEBUG_NO_INLINE
			uint32_t a_prefix = Load<uint32_t>(const_data_ptr_cast(left.GetPrefix()));
			uint32_t b_prefix = Load<uint32_t>(const_data_ptr_cast(right.GetPrefix()));

			// Utility to move 0xa1b2c3d4 into 0xd4c3b2a1, basically inverting the order byte-a-byte
			auto byte_swap = [](uint32_t v) -> uint32_t {
				uint32_t t1 = (v >> 16u) | (v << 16u);
				uint32_t t2 = t1 & 0x00ff00ff;
				uint32_t t3 = t1 & 0xff00ff00;
				return (t2 << 8u) | (t3 >> 8u);
			};

			// Check on prefix -----
			// We dont' need to mask since:
			//	if the prefix is greater(after bswap), it will stay greater regardless of the extra bytes
			// 	if the prefix is smaller(after bswap), it will stay smaller regardless of the extra bytes
			//	if the prefix is equal, the extra bytes are guaranteed to be /0 for the shorter one

			if (a_prefix != b_prefix) {
				return byte_swap(a_prefix) > byte_swap(b_prefix);
			}
#endif
			auto memcmp_res = memcmp(left.GetData(), right.GetData(), min_length);
			return memcmp_res > 0 || (memcmp_res == 0 && left_length > right_length);
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
