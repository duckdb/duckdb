//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/string.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/typedefs.hpp"

namespace duckdb {

class String {
public:
	// Constructors (create an owning String)
	String() : len(0), buf {0} {
	}

	String(const std::string &str) { // NOLINT allowimplicit conversion
		AssignOwning(str.data(), str.size());
	}

	String(const char *str, const size_t size) {
		if (str == nullptr) {
			AssignOwning(str, 0);
		}
		AssignOwning(str, size);
	}

	String(const char *str) // NOLINT allowimplicit conversion
	    : String(str, str ? strlen(str) : 0) {
	}

public:
	// Copying is not allowed, use move semantics instead or explicitly create a new String instance.
	String(const String &other) = delete;
	String &operator=(const String &other) = delete;

	// Move Constructor
	String(String &&other) noexcept {
		AssignOwning(other.data(), other.size());
		other.Release();
	}

	// Move Assignment
	String &operator=(String &&other) noexcept {
		if (this != &other) {
			Destroy();
			AssignOwning(other.data(), other.size());
			other.Release();
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
		const idx_t this_size = size();
		const idx_t other_size = other.size();

		if (this_size != other_size) {
			return false;
		}

		const auto min_size = MinValue<idx_t>(this_size, other_size);
		const char *this_data = data();
		const char *other_data = other.data();

		auto memcmp_res = memcmp(this_data, other_data, min_size);
		return memcmp_res == 0;
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

		if (IsInline()) {
			return buf[pos];
		}
		return ptr[pos];
	}

public:
	// STL-like interface
	// NOLINTBEGIN - mimic std::string interface
	size_t size() const {
		return len & ~NON_OWN_BIT;
	}
	bool empty() const {
		return len == 0;
	}
	const char *data() const {
		return IsInline() ? buf : ptr;
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
		return (len & NON_OWN_BIT) == 0;
	}
	bool IsInline() const {
		return len <= INLINE_MAX;
	}
	static bool CanBeInlined(size_t size) {
		return size <= INLINE_MAX;
	}

	// Creates a new String instance with its own copy of the data
	static String Copy(const char *data, size_t size) {
		String result;
		result.AssignOwning(data, size);
		return result;
	}

	static String Copy(const String &other) {
		return Copy(other.data(), other.size());
	}
	static String Copy(const char *data) {
		return Copy(data, data ? strlen(data) : 0);
	}
	static String Copy(const std::string &str) {
		return Copy(str.data(), str.size());
	}
	String Copy() const {
		return Copy(data(), size());
	}

	// Creates a new String instance that references the data without owning it
	// If the size is small enough, it will inline the data; which WILL be owning
	static String Reference(const char *data, size_t size) {
		String result;
		// If we reference, and we can inline it, we make owning anyway
		if (size <= INLINE_MAX) {
			result.AssignOwning(data, size);
		} else {
			result.ptr = const_cast<char *>(data); // NOLINT allow const cast
			result.len = size | NON_OWN_BIT;       // Set the non-owning bit
		}
		return result;
	}

	static String Reference(const String &other) {
		return Reference(other.data(), other.size());
	}
	static String Reference(const char *data) {
		return Reference(data, data ? strlen(data) : 0);
	}
	static String Reference(const std::string &str) {
		return Reference(str.data(), str.size());
	}
	String Reference() const {
		return Reference(data(), size());
	}

	string ToStdString() const {
		if (IsInline()) {
			return string(buf, size());
		}
		return string(ptr, size());
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
	static constexpr auto NON_OWN_BIT = 1UL << (sizeof(size_t) * 8 - 1);
	static constexpr auto LENGTH_MAX = (1UL << (sizeof(size_t) * 8 - 1)) - 1;

	void AssignOwning(const char *new_data, size_t new_size) {
		len = new_size;

		if (len == 0) {
			buf[len] = '\0'; // Null-terminate the inline buffer
			return;
		}

		D_ASSERT(new_data != nullptr);

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
		if (IsOwning() && !IsInline()) {
			delete[] ptr;
		}
	}

	void Release() {
		// Set the non-owning bit
		if (IsOwning() && !IsInline()) {
			len |= NON_OWN_BIT;
		}
	}

private:
	size_t len;
	union {
		const char *ptr;
		char buf[INLINE_CAP];
	};
};

} // namespace duckdb
