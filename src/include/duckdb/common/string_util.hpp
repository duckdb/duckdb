//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/string_util.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include <stdarg.h> // for va_list

namespace duckdb {
/**
 * String Utility Functions
 * Note that these are not the most efficient implementations (i.e., they copy
 * memory) and therefore they should only be used for debug messages and other
 * such things.
 */
class StringUtil {
public:
	//! Returns true if the needle string exists in the haystack
	static bool Contains(const string &haystack, const string &needle);

	//! Returns true if the target string starts with the given prefix
	static bool StartsWith(const string &str, const string &prefix);

	//! Returns true if the target string <b>ends</b> with the given suffix.
	static bool EndsWith(const string &str, const string &suffix);

	//! Repeat a string multiple times
	static string Repeat(const string &str, const idx_t n);

	//! Split the input string based on newline char
	static vector<string> Split(const string &str, char delimiter);

	//! Join multiple strings into one string. Components are concatenated by the given separator
	static string Join(const vector<string> &input, const string &separator);

	//! Join multiple items of container with given size, transformed to string
	//! using function, into one string using the given separator
	template <typename C, typename S, typename Func>
	static string Join(const C &input, S count, const string &separator, Func f) {
		// The result
		std::string result;

		// If the input isn't empty, append the first element. We do this so we
		// don't need to introduce an if into the loop.
		if (count > 0) {
			result += f(input[0]);
		}

		// Append the remaining input components, after the first
		for (size_t i = 1; i < count; i++) {
			result += separator + f(input[i]);
		}

		return result;
	}

	//! Append the prefix to the beginning of each line in str
	static string Prefix(const string &str, const string &prefix);

	//! Return a string that formats the give number of bytes
	static string FormatSize(idx_t bytes);

	//! Convert a string to uppercase
	static string Upper(const string &str);

	//! Convert a string to lowercase
	static string Lower(const string &str);

	//! Format a string using printf semantics
	static string Format(const string fmt_str, ...);
	static string VFormat(const string fmt_str, va_list ap);

	//! Split the input string into a vector of strings based on the split string
	static vector<string> Split(const string &input, const string &split);

	//! Remove the whitespace char in the left end of the string
	static void LTrim(string &str);
	//! Remove the whitespace char in the right end of the string
	static void RTrim(string &str);
	//! Remove the whitespace char in the left and right end of the string
	static void Trim(string &str);

	static string Replace(string source, const string &from, const string &to);
};
} // namespace duckdb
