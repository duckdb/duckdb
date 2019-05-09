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
	/**
	 * Returns true if the needle string exists in the haystack
	 */
	static bool Contains(const string &haystack, const string &needle);

	/**
	 * Returns true if the target string starts with the given prefix
	 */
	static bool StartsWith(const string &str, const string &prefix);

	/**
	 * Returns true if the target string <b>ends</b> with the given suffix.
	 * http://stackoverflow.com/a/2072890
	 */
	static bool EndsWith(const string &str, const string &suffix);

	/**
	 * Repeat a string multiple times
	 */
	static string Repeat(const string &str, const std::uint64_t n);

	/**
	 * Split the input string based on newline char
	 */
	static vector<string> Split(const string &str, char delimiter);

	/**
	 * Join multiple strings into one string. Components are concatenated by the
	 * given separator
	 */
	static string Join(const vector<string> &input, const string &separator);

	/**
	 * Append the prefix to the beginning of each line in str
	 */
	static string Prefix(const string &str, const string &prefix);

	/**
	 * Return a string that formats the give number of bytes into the
	 * appropriate
	 * kilobyte, megabyte, gigabyte representation.
	 * http://ubuntuforums.org/showpost.php?p=10215516&postcount=5
	 */
	static string FormatSize(long bytes);

	/**
	 * Wrap the given string with the control characters
	 * to make the text appear bold in the console
	 */
	static string Bold(const string &str);

	/**
	 * Convert a string to its uppercase form
	 */
	static string Upper(const string &str);

	/**
	 * Convert a string to its uppercase form
	 */
	static string Lower(const string &str);

	/**
	 * Format a string using printf semantics
	 * http://stackoverflow.com/a/8098080
	 */
	static string Format(const string fmt_str, ...);
	static string VFormat(const string fmt_str, va_list ap);

	/**
	 * Split the input string into a vector of strings based on
	 * the split string given us
	 * @param input
	 * @param split
	 * @return
	 */
	static vector<string> Split(const string &input, const string &split);

	/**
	 * Remove the whitespace char in the right end of the string
	 */
	static void RTrim(string &str);

	static string Indent(const int num_indent);

	/**
	 * Return a new string that has stripped all occurrences of the provided
	 * character from the provided string.
	 *
	 * NOTE: This function copies the input string into a new string, which is
	 * wasteful. Don't use this for performance critical code, please!
	 *
	 * @param str The input string
	 * @param c The character we want to remove
	 * @return A new string with no occurrences of the provided character
	 */
	static string Strip(const string &str, char c);

	static string Replace(string source, const string &from, const string &to) {
		if (from.empty())
			return source;
		;
		uint64_t start_pos = 0;
		while ((start_pos = source.find(from, start_pos)) != string::npos) {
			source.replace(start_pos, from.length(), to);
			start_pos += to.length(); // In case 'to' contains 'from', like
			                          // replacing 'x' with 'yx'
		}
		return source;
	}
};
} // namespace duckdb
