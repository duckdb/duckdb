//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/string.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <cstring>

#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/assert.hpp"

namespace duckdb {

class String {
	/*
	    String either can own its data (with automatic cleanup) or hold a non-owning reference.
	    * Move only semantics, no copying allowed.
	    * Small Strings (≤7 bytes) are stored inline to avoid heap allocation.
	    * 31-bit length limit with overflow protection.

	    To create an owning String, use the constructors.
	    To create a non-owning String, use the Reference methods.
	 */

public:
	// Constructors (create an owning String)
	String() : len(0), buf {0} {
	}

	String(const std::string &str) { // NOLINT allowimplicit conversion
		AssignOwning(str.data(), SafeStrLen(str));
	}

	String(const char *str, const uint32_t size) {
		AssignOwning(str, size);
	}

	String(const char *str) // NOLINT allowimplicit conversion
	    : String(str, str ? SafeStrLen(str) : 0) {
	}

public:
	// Copying is not allowed, use move semantics instead or explicitly create a new String instance.
	String(const String &other) = delete;
	String &operator=(const String &other) = delete;

	// Move Constructor
	String(String &&other) noexcept {
		TransferOwnership(other);
	}

	// Move Assignment
	String &operator=(String &&other) noexcept {
		if (this != &other) {
			Destroy();
			TransferOwnership(other);
		}
		return *this;
	}

	// Destructor
	~String() {
		Destroy();
	}

public:
	// Operators
	bool operator==(const String &other) const {
		if (this == &other) {
			return true; // points to the same instance
		}

		if (size() != other.size()) {
			return false;
		}

		return memcmp(data(), other.data(), size()) == 0;
	}

	bool operator==(const std::string &other) const {
		if (SafeStrLen(other) != size()) {
			return false;
		}
		return memcmp(data(), other.data(), size()) == 0;
	}

	bool operator==(const char *other) const {
		if (!other || SafeStrLen(other) != size()) {
			return false;
		}

		if (this->data() == other) {
			return true; // points to the same instance
		}

		return memcmp(data(), other, size()) == 0;
	}

	bool operator>(const String &other) const {
		const auto this_size = size();
		const auto other_size = other.size();
		const auto min_size = MinValue<idx_t>(this_size, other_size);

		auto memcmp_res = memcmp(data(), other.data(), min_size);
		return memcmp_res > 0 || (memcmp_res == 0 && this_size > other_size);
	}

	bool operator!=(const String &other) const {
		return !(*this == other);
	}
	bool operator<(const String &other) const {
		return other > *this;
	}
	bool operator>=(const String &other) const {
		return !(*this < other);
	}
	bool operator<=(const String &other) const {
		return !(*this > other);
	}

	char operator[](const idx_t pos) const {
		D_ASSERT(pos < size());

		if (IsInlined()) {
			return buf[pos];
		}
		return ptr[pos];
	}

public:
	// STL-like interface
	// NOLINTBEGIN - mimic std::string interface
	uint32_t size() const {
		return len & ~NON_OWNING_BIT;
	}
	bool empty() const {
		return len == 0;
	}
	const char *data() const {
		return IsInlined() ? buf : ptr;
	}
	const char *begin() const {
		return data();
	}
	const char *end() const {
		return data() + size();
	}
	const char *c_str() const {
		return data();
	}
	// NOLINTEND

	// Helper methods
	bool IsOwning() const {
		return (len & NON_OWNING_BIT) == 0;
	}

	bool IsInlined() const {
		return len <= INLINE_MAX;
	}

	static bool CanBeInlined(uint32_t size) {
		return size <= INLINE_MAX;
	}

	// Creates a new String instance with its own copy of the data
	static String Copy(const char *data, uint32_t size) {
		if (data == nullptr) {
			return String(); // Return an empty String
		}

		String result;
		result.AssignOwning(data, size);
		return result;
	}

	static String Copy(const String &other) {
		return Copy(other.data(), other.size());
	}
	static String Copy(const char *data) {
		return Copy(data, data ? SafeStrLen(data) : 0);
	}
	static String Copy(const std::string &str) {
		return Copy(str.data(), SafeStrLen(str));
	}
	String Copy() const {
		return String::Copy(data(), size());
	}

	// Creates a new String instance that references the data without owning it
	// If the size is small enough, it will inline the data; which WILL be owning
	static String Reference(const char *data, uint32_t size) {
		if (data == nullptr) {
			return String(); // Return an empty String
		}

		String result;
		// If we reference, and we can inline it, we make owning anyway
		if (size <= INLINE_MAX) {
			result.AssignOwning(data, size);
		} else {
			result.ptr = const_cast<char *>(data); // NOLINT allow const cast
			result.len = size | NON_OWNING_BIT;    // Set the non-owning bit
		}
		return result;
	}

	static String Reference(const String &other) {
		return Reference(other.data(), other.size());
	}
	static String Reference(const char *data) {
		return Reference(data, data ? SafeStrLen(data) : 0);
	}
	static String Reference(const std::string &str) {
		return Reference(str.data(), SafeStrLen(str));
	}
	String Reference() const {
		return String::Reference(data(), size());
	}

	std::string ToStdString() const {
		if (IsInlined()) {
			return std::string(buf, size());
		}
		return std::string(ptr, size());
	}

	static uint32_t SafeStrLen(const char *data) {
		if (!data) {
			return 0;
		}

		const auto len = strlen(data);
		D_ASSERT(len < NumericLimits<uint32_t>::Maximum());
		return static_cast<uint32_t>(len);
	}

	static uint32_t SafeStrLen(const std::string &data) {
		const auto len = data.size();
		D_ASSERT(len < NumericLimits<uint32_t>::Maximum());
		return static_cast<uint32_t>(len);
	}

public:
	static char CharacterToLower(char c) {
		if (c >= 'A' && c <= 'Z') {
			return UnsafeNumericCast<char>(c + ('a' - 'A'));
		}
		return c;
	}

	String Lower() const {
		const auto str_data = data();
		const auto str_size = size();

		std::string lowercase_str;
		lowercase_str.reserve(str_size);
		for (idx_t i = 0; i < str_size; ++i) {
			lowercase_str.push_back(CharacterToLower(str_data[i]));
		}
		return String(lowercase_str);
	}

private:
	static constexpr auto INLINE_CAP = sizeof(char *);
	static constexpr auto INLINE_MAX = INLINE_CAP - 1;
	static constexpr auto NON_OWNING_BIT = 1UL << (sizeof(uint32_t) * 8 - 1);
	static constexpr auto LENGTH_MAX = (1UL << (sizeof(uint32_t) * 8 - 1)) - 1;

	void AssignOwning(const char *new_data, uint32_t new_size) {
		len = new_data ? new_size : 0;

		if (len == 0) {
			buf[len] = '\0'; // Null-terminate the inline buffer
			return;
		}

		if (len <= INLINE_MAX) {
			memcpy(buf, new_data, len);
			buf[len] = '\0'; // Null-terminate the inline buffer
			return;
		}

		auto new_ptr = new char[len + 1]; // +1 for null-termination
		memcpy(new_ptr, new_data, len);
		new_ptr[len] = '\0';

		ptr = new_ptr;
	}

	void Destroy() {
		if (IsOwning() && !IsInlined()) {
			delete[] ptr;
		}
	}

	// Releases the ownership of the String, without deleting the data, e.g., when transferring ownership
	void ReleaseOwning() {
		// Set the non-owning bit
		if (IsOwning() && !IsInlined()) {
			len |= NON_OWNING_BIT;
		}
	}

	void TransferOwnership(String &other) {
		len = other.len;
		if (IsInlined()) {
			AssignOwning(other.data(), other.size());
		} else {
			ptr = other.ptr;
			len = other.len;
		}
		other.ReleaseOwning();
	}

private:
	uint32_t len; // The first bit indicates ownership (0 = owning, 1 = non-owning)
	union {       // If length is less than or equal to INLINE_MAX, then it is inlined here
		const char *ptr;
		char buf[INLINE_CAP];
	};
};

inline bool operator==(const std::string &lhs, const String &rhs) {
	return rhs == lhs;
}

inline bool operator==(const char *lhs, const String &rhs) {
	return rhs == lhs;
}

} // namespace duckdb
