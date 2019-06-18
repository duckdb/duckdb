//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/string_util.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/constants.hpp"

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
	static string Repeat(const string &str, const index_t n);

	//! Split the input string based on newline char
	static vector<string> Split(const string &str, char delimiter);

	//! Join multiple strings into one string. Components are concatenated by the given separator
	static string Join(const vector<string> &input, const string &separator);

	//! Append the prefix to the beginning of each line in str
	static string Prefix(const string &str, const string &prefix);

	//! Return a string that formats the give number of bytes
	static string FormatSize(index_t bytes);

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
