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

namespace detail {
class StringBase {
protected:
	StringBase() : size(0), ptr(nullptr) {
		inlined[0] = '\0'; // Initialize inlined storage
	}

	idx_t GetSize() const;
	void SetSize(idx_t size);
	bool IsOwning() const;
	void SetOwning(bool owning);
	bool IsInlined() const {
		return size <= INLINE_MAX_SIZE;
	}

	char *GetStorage() {
		if (IsInlined()) {
			return static_cast<char *>(inlined);
		} else {
			return ptr;
		}
	}

private:
	static constexpr auto REFERENCE_FLAG = (1ULL << (sizeof(idx_t) * 8 - 1));
	static constexpr auto INLINE_MAX_SIZE = sizeof(char *) - 1; // Reserve space for null terminator
	idx_t size; // size is the size of the string, excluding the null terminator.
	union {
		char *ptr;
		char inlined[INLINE_MAX_SIZE];
	};
};

inline idx_t StringBase::GetSize() const {
	return (size & ~REFERENCE_FLAG);
}

inline bool StringBase::IsOwning() const {
	return (size & REFERENCE_FLAG) == 0;
}

inline void StringBase::SetSize(idx_t size_p) {
	D_ASSERT(size_p < NumericLimits<idx_t>::Maximum());
	const auto reference_flag = size & REFERENCE_FLAG;
	size = size_p;
	size |= reference_flag; // Preserve the reference flag
}

inline void StringBase::SetOwning(bool owning) {
	if (owning) {
		size &= ~REFERENCE_FLAG; // Clear the reference flag
	} else {
		size |= REFERENCE_FLAG; // Set the reference flag
	}
}

} // namespace detail

class Str : public detail::StringBase {

	Str(const std::string &str) // NOLINT: allow implicit conversion
	    : detail::StringBase() {

		SetSize(str.size());
		SetOwning(true);
		auto data = GetData();
	}
};

static void Foo() {
	Str x;
}

class Str {
public:
	Str(const std::string &str) { // NOLINT: allow implicit conversion
		AssignOwned(str.data(), str.size());
	}

	Str(const char *str) {
		AssignOwned(str, strlen(str));
	}

	Str() {
		len = 0;
		is_owning = true;
		inlined[0] = '\0'; // Initialize inlined storage
	}

	//
	// // Copy constructor always makes an owning copy of the data.
	// Str(const Str &other) {
	// 	AssignOwned(other.data(), other.size());
	// }
	//
	// // Copy assignment
	// Str &operator=(const Str &other) {
	// 	if (this != &other) {
	// 		if (!IsInlined() && !is_owning) {
	// 			delete[] ptr;
	// 		}
	// 		AssignOwned(other.data(), other.size());
	// 	}
	// 	return *this;
	// }

	// Not implicitly copyable. Use Copy() method instead.
	Str(const Str &other) = delete;
	Str &operator=(const Str &other) = delete;

	static Str Copy(const char *data, size_t length) {
		Str result;
		result.AssignOwned(data, length);
		return result;
	}

	static Str Copy(const char *data) {
		return Copy(data, strlen(data));
	}
	static Str Copy(const string &str) {
		return Copy(str.data(), str.size());
	}
	Str Copy() const {
		return Copy(data(), size());
	};

	static Str Reference(const char *data, size_t length) {
		Str result;
		result.len = length;
		if (result.IsInlined()) {
			result.is_owning = true;
			if (!result.empty()) {
				D_ASSERT(data != nullptr);
				memcpy(result.inlined, data, length);
			}
			result.inlined[length] = '\0'; // Null-terminate the inlined storage
		} else {
			result.is_owning = false;
			result.ptr = const_cast<char *>(data); // Non-owning reference
		}
	}
	static Str Reference(const char *data) {
		return Reference(data, strlen(data));
	}
	static Str Reference(const string &str) {
		return Reference(str.data(), str.size());
	}
	Str Reference() const {
		return Reference(data(), size());
	}

	static void foo() {
		auto x = Str::Copy("foo");

		auto y = x.Reference();
	}

	// Move constructor
	Str(Str &&other) noexcept {
		if (other.IsInlined()) {
			is_owning = true;
			len = other.len;
			memcpy(inlined, other.inlined, other.len + 1);
			return;
		}

		if (!other.is_owning) {
			is_owning = false;
			len = other.len;
			ptr = other.ptr;
			return;
		}

		// Other is owning!, steal its data!
		is_owning = true;
		len = other.len;
		ptr = other.ptr;
		other.is_owning = false; // Prevent double deletion
		other.len = 0;           // Prevent double deletion
		return;
	}

	// Move assignment
	Str &operator=(Str &&other) noexcept {
		if (this != &other) {
			if (is_owning && !IsInlined()) {
				delete[] ptr;
			}

			if (other.IsInlined()) {
				is_owning = true;
				len = other.len;
				memcpy(inlined, other.inlined, other.len + 1);
			} else if (!other.is_owning) {
				is_owning = false;
				len = other.len;
				ptr = other.ptr;
			} else {
				is_owning = true;
				len = other.len;
				ptr = other.ptr;
				other.is_owning = false; // Prevent double deletion
				other.len = 0;           // Prevent double deletion
			}
		}
		return *this;
	}

	// Destructor
	~Str() {
		if (is_owning && !IsInlined()) {
			delete[] ptr;
		}
	}

	char *data() {
		return len > INLINE_MAX_SIZE ? ptr : inlined;
	}
	const char *data() const {
		return len > INLINE_MAX_SIZE ? ptr : inlined;
	}
	size_t size() const {
		return len;
	}
	bool empty() const {
		return len == 0;
	}

private:
	bool IsInlined() const {
		return len <= INLINE_MAX_SIZE;
	}

	void AssignOwned(const char *data, size_t length) {
		D_ASSERT(length <= NumericLimits<uint32_t>::Maximum());

		len = length;
		is_owning = true;
		if (IsInlined()) {
			memcpy(inlined, data, len);
			inlined[len] = '\0';
		} else {
			ptr = new char[len + 1];
			memcpy(ptr, data, len);
			ptr[len] = '\0';
		}
	}

	static constexpr auto INLINE_MAX_SIZE = sizeof(char *) - 1; // Reserve space for null terminator
	union {
		char *ptr;
		char inlined[INLINE_MAX_SIZE];
	};

	uint32_t len; // len, excluding the null terminator
	bool is_owning;
};

class String {
public:
	// Owning constructors
	String(std::string str) // NOLINT: allow implicit conversion
	    : size(str.size()), is_owned(true) {
		if (size == 0) {
			data = nullptr;
			return;
		}

		char *owned_ptr = new char[size + 1];
		memcpy(owned_ptr, str.data(), size);
		owned_ptr[size] = '\0';
		data = owned_ptr;
	}

	String(const char *ptr, const size_t len) : size(len), is_owned(true) {
		if (size == 0 || !ptr) {
			data = nullptr;
			return;
		}

		char *owned_ptr = new char[size + 1];
		memcpy(owned_ptr, ptr, size);
		owned_ptr[size] = '\0';
		data = owned_ptr;
	}

	String(const char *ptr) // NOLINT: allow implicit conversion
	    : String(ptr, ptr ? strlen(ptr) : 0) {
	}

public:
	String(const String &) = delete;
	String &operator=(const String &) = delete;

	String(String &&other) noexcept : data(other.data), size(other.size), is_owned(other.is_owned) {
		other.data = nullptr;
		other.size = 0;
		other.is_owned = false;
	}

	String &operator=(String &&other) noexcept {
		if (this != &other) {
			if (is_owned && data) {
				delete[] data;
			}
			data = other.data;
			size = other.size;
			is_owned = other.is_owned;

			other.data = nullptr;
			other.size = 0;
			other.is_owned = false;
		}
		return *this;
	}

	~String() {
		if (is_owned && data) {
			delete[] data;
		}
	}

public:
	// Non-owning!
	static String Reference(const char *ptr, const size_t len) {
		String str;
		str.data = ptr;
		str.size = len;
		return str;
	}

	static String Reference(const char *ptr) {
		return Reference(ptr, strlen(ptr));
	}

	String Reference() const {
		return Reference(data, size);
	}

	String Copy() const {
		if (data && size > 0) {
			return String(data, size);
		}
		return String();
	}

	String Clone() {
		return Reference();
	}

public:
	bool operator==(const String &other) const {
		const idx_t this_size = GetSize();
		const idx_t other_size = other.GetSize();

		if (this_size != other_size) {
			return false;
		}

		const char *this_data = GetData();
		const char *other_data = other.GetData();

		if (memcmp(this_data, other_data, this_size) == 0) {
			return true;
		}

		for (idx_t i = 0; i < this_size; i++) {
			if (this_data[i] != other_data[i]) {
				return false;
			}
		}
		return true;
	}

	bool operator!=(const String &other) const {
		return !(*this == other);
	}

	bool operator<=(const String &other) const {
		return *this < other || *this == other;
	}

	bool operator>=(const String &other) const {
		return !(*this < other);
	}

	bool operator<(const String &other) const {
		const char *this_data = GetData();
		const char *other_data = other.GetData();
		const idx_t this_size = GetSize();
		const idx_t other_size = other.GetSize();

		const idx_t length = MinValue<idx_t>(this_size, other_size);

		for (idx_t i = 0; i < length; i++) {
			if (memcmp(this_data, other_data, length) < 0) {
				return true;
			}
			if (memcmp(this_data, other_data, length) > 0) {
				return false;
			}
		}
		return this_size < other_size;
	}

	bool operator>(const String &other) const {
		return !(*this <= other);
	}

public:
	idx_t GetSize() const {
		return size;
	}

	const char *GetData() const {
		return data;
	}

	char operator[](const idx_t pos) const {
		D_ASSERT(pos < size);
		return data[pos];
	}

	bool IsOwned() const {
		return is_owned;
	}

	bool empty() const {
		return size == 0;
	}

	const char *c_str() const {
		return GetData();
	}

	// FIXME: This doesn't work now
	string ToStdString() const {
		return string(data, size);
	}

public:
	static char CharacterToLower(char c) {
		if (c >= 'A' && c <= 'Z') {
			return UnsafeNumericCast<char>(c + ('a' - 'A'));
		}
		return c;
	}

	String Lower() const {
		const auto str_data = GetData();
		const auto str_size = GetSize();

		std::string lowercase_str;
		lowercase_str.reserve(str_size);
		for (idx_t i = 0; i < str_size; ++i) {
			lowercase_str.push_back(CharacterToLower(str_data[i]));
		}
		return String(std::move(lowercase_str));
	}

	struct ConstIterator;

	ConstIterator begin() const;

	ConstIterator end() const;

private:
	String() : data(nullptr), size(0) {
	}

private:
	// mutable string owned_data;
	const char *data;
	idx_t size;

	bool is_owned = false;
};

struct String::ConstIterator {
	const char *ptr;

	explicit ConstIterator(const char *ptr_p) : ptr(ptr_p) {
	}

	const char &operator*() const {
		return *ptr;
	}

	ConstIterator &operator++() {
		ptr++;
		return *this;
	}
};

inline String::ConstIterator String::begin() const {
	return ConstIterator(data);
}

inline String::ConstIterator String::end() const {
	return ConstIterator(data + size);
}
} // namespace duckdb
