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
	// Owning constructors
	String(std::string str) // NOLINT: allow implicit conversion
	    : owned_data(std::move(str)), data(owned_data.c_str()), size(owned_data.size()) {
	}

	String(const char *ptr, const size_t len) : owned_data(ptr, len), data(ptr), size(len) {
	}

	String(const char *ptr) // NOLINT: allow implicit conversion
	    : String(ptr, strlen(ptr)) {
	}

	// Non-owning constructors
	static String CreateView(const char *ptr, const size_t len) {
		String str;
		str.data = ptr;
		str.size = len;
		return str;
	}

	static String CreateView(const char *ptr) {
		return CreateView(ptr, strlen(ptr));
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
			if (this_data[i] < other_data[i]) {
				return true;
			}
			if (this_data[i] > other_data[i]) {
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
		return !owned_data.empty();
	}

	bool empty() const {
		return size == 0;
	}

	const char *c_str() const {
		return GetData();
	}

	const string &ToStdString() const {
		if (!owned_data.empty()) {
			return owned_data;
		}

		owned_data = string(data, size);
		return owned_data;
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
	mutable string owned_data;
	const char *data;
	idx_t size;
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
