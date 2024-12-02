//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/string_util.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/set.hpp"
#include "duckdb/common/vector.hpp"

#include <cstring>

namespace duckdb {

#ifndef DUCKDB_QUOTE_DEFINE
// Preprocessor trick to allow text to be converted to C-string / string
// Expecte use is:
//	#ifdef SOME_DEFINE
//	string str = DUCKDB_QUOTE_DEFINE(SOME_DEFINE)
//	...do something with str
//	#endif SOME_DEFINE
#define DUCKDB_QUOTE_DEFINE_IMPL(x) #x
#define DUCKDB_QUOTE_DEFINE(x)      DUCKDB_QUOTE_DEFINE_IMPL(x)
#endif

/**
 * String Utility Functions
 * Note that these are not the most efficient implementations (i.e., they copy
 * memory) and therefore they should only be used for debug messages and other
 * such things.
 */
class StringUtil {
public:
	static string GenerateRandomName(idx_t length = 16);

	static uint8_t GetHexValue(char c) {
		if (c >= '0' && c <= '9') {
			return UnsafeNumericCast<uint8_t>(c - '0');
		}
		if (c >= 'a' && c <= 'f') {
			return UnsafeNumericCast<uint8_t>(c - 'a' + 10);
		}
		if (c >= 'A' && c <= 'F') {
			return UnsafeNumericCast<uint8_t>(c - 'A' + 10);
		}
		throw InvalidInputException("Invalid input for hex digit: %s", string(1, c));
	}
	static uint8_t GetBinaryValue(char c) {
		if (c >= '0' && c <= '1') {
			return UnsafeNumericCast<uint8_t>(c - '0');
		}
		throw InvalidInputException("Invalid input for binary digit: %s", string(1, c));
	}

	static bool CharacterIsSpace(char c) {
		return c == ' ' || c == '\t' || c == '\n' || c == '\v' || c == '\f' || c == '\r';
	}
	static bool CharacterIsNewline(char c) {
		return c == '\n' || c == '\r';
	}
	static bool CharacterIsDigit(char c) {
		return c >= '0' && c <= '9';
	}
	static bool CharacterIsHex(char c) {
		return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
	}
	static char CharacterToUpper(char c) {
		if (c >= 'a' && c <= 'z') {
			return UnsafeNumericCast<char>(c - ('a' - 'A'));
		}
		return c;
	}
	static char CharacterToLower(char c) {
		if (c >= 'A' && c <= 'Z') {
			return UnsafeNumericCast<char>(c + ('a' - 'A'));
		}
		return c;
	}
	static bool CharacterIsAlpha(char c) {
		return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z');
	}
	static bool CharacterIsOperator(char c) {
		if (c == '_') {
			return false;
		}
		if (c >= '!' && c <= '/') {
			return true;
		}
		if (c >= ':' && c <= '@') {
			return true;
		}
		if (c >= '[' && c <= '`') {
			return true;
		}
		if (c >= '{' && c <= '~') {
			return true;
		}
		return false;
	}

	template <class TO>
	static vector<TO> ConvertStrings(const vector<string> &strings) {
		vector<TO> result;
		for (auto &string : strings) {
			result.emplace_back(string);
		}
		return result;
	}

	static vector<SQLIdentifier> ConvertToSQLIdentifiers(const vector<string> &strings) {
		return ConvertStrings<SQLIdentifier>(strings);
	}

	static vector<SQLString> ConvertToSQLStrings(const vector<string> &strings) {
		return ConvertStrings<SQLString>(strings);
	}

	//! Returns true if the needle string exists in the haystack
	DUCKDB_API static bool Contains(const string &haystack, const string &needle);

	//! Returns the position of needle string within the haystack
	DUCKDB_API static optional_idx Find(const string &haystack, const string &needle);

	//! Returns true if the target string starts with the given prefix
	DUCKDB_API static bool StartsWith(string str, string prefix);

	//! Returns true if the target string <b>ends</b> with the given suffix.
	DUCKDB_API static bool EndsWith(const string &str, const string &suffix);

	//! Repeat a string multiple times
	DUCKDB_API static string Repeat(const string &str, const idx_t n);

	//! Split the input string based on newline char
	DUCKDB_API static vector<string> Split(const string &str, char delimiter);

	//! Split the input string allong a quote. Note that any escaping is NOT supported.
	DUCKDB_API static vector<string> SplitWithQuote(const string &str, char delimiter = ',', char quote = '"');

	//! Join multiple strings into one string. Components are concatenated by the given separator
	DUCKDB_API static string Join(const vector<string> &input, const string &separator);
	DUCKDB_API static string Join(const set<string> &input, const string &separator);

	//! Encode special URL characters in a string
	DUCKDB_API static string URLEncode(const string &str, bool encode_slash = true);
	DUCKDB_API static idx_t URLEncodeSize(const char *input, idx_t input_size, bool encode_slash = true);
	DUCKDB_API static void URLEncodeBuffer(const char *input, idx_t input_size, char *output, bool encode_slash = true);
	//! Decode URL escape sequences (e.g. %20) in a string
	DUCKDB_API static string URLDecode(const string &str, bool plus_to_space = false);
	DUCKDB_API static idx_t URLDecodeSize(const char *input, idx_t input_size, bool plus_to_space = false);
	DUCKDB_API static void URLDecodeBuffer(const char *input, idx_t input_size, char *output,
	                                       bool plus_to_space = false);

	template <class T>
	static string ToString(const vector<T> &input, const string &separator) {
		vector<string> input_list;
		for (auto &i : input) {
			input_list.push_back(i.ToString());
		}
		return StringUtil::Join(input_list, separator);
	}

	//! Join multiple items of container with given size, transformed to string
	//! using function, into one string using the given separator
	template <typename C, typename S, typename FUNC>
	static string Join(const C &input, S count, const string &separator, FUNC f) {
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

	//! Return a string that formats the give number of bytes
	DUCKDB_API static string BytesToHumanReadableString(idx_t bytes, idx_t multiplier = 1024);

	//! Convert a string to UPPERCASE
	DUCKDB_API static string Upper(const string &str);

	//! Convert a string to lowercase
	DUCKDB_API static string Lower(const string &str);

	//! Convert a string to Title Case
	DUCKDB_API static string Title(const string &str);

	DUCKDB_API static bool IsLower(const string &str);

	//! Case insensitive hash
	DUCKDB_API static uint64_t CIHash(const string &str);

	//! Case insensitive equals
	DUCKDB_API static bool CIEquals(const string &l1, const string &l2);

	//! Case insensitive compare
	DUCKDB_API static bool CILessThan(const string &l1, const string &l2);

	//! Case insensitive find, returns DConstants::INVALID_INDEX if not found
	DUCKDB_API static idx_t CIFind(vector<string> &vec, const string &str);

	//! Format a string using printf semantics
	template <typename... ARGS>
	static string Format(const string fmt_str, ARGS... params) {
		return Exception::ConstructMessage(fmt_str, params...);
	}

	//! Split the input string into a vector of strings based on the split string
	DUCKDB_API static vector<string> Split(const string &input, const string &split);

	//! Remove the whitespace char in the left end of the string
	DUCKDB_API static void LTrim(string &str);
	//! Remove the whitespace char in the right end of the string
	DUCKDB_API static void RTrim(string &str);
	//! Remove the all chars from chars_to_trim char in the right end of the string
	DUCKDB_API static void RTrim(string &str, const string &chars_to_trim);
	//! Remove the whitespace char in the left and right end of the string
	DUCKDB_API static void Trim(string &str);

	DUCKDB_API static string Replace(string source, const string &from, const string &to);

	//! Get the levenshtein distance from two strings
	//! The not_equal_penalty is the penalty given when two characters in a string are not equal
	//! The regular levenshtein distance has a not equal penalty of 1, which means changing a character is as expensive
	//! as adding or removing one For similarity searches we often want to give extra weight to changing a character For
	//! example: with an equal penalty of 1, "pg_am" is closer to "depdelay" than "depdelay_minutes"
	//! with an equal penalty of 3, "depdelay_minutes" is closer to "depdelay" than to "pg_am"
	DUCKDB_API static idx_t LevenshteinDistance(const string &s1, const string &s2, idx_t not_equal_penalty = 1);

	//! Returns the similarity score between two strings (edit distance metric - lower is more similar)
	DUCKDB_API static idx_t SimilarityScore(const string &s1, const string &s2);
	//! Returns a normalized similarity rating between 0.0 - 1.0 (higher is more similar)
	DUCKDB_API static double SimilarityRating(const string &s1, const string &s2);
	//! Get the top-n strings (sorted by the given score distance) from a set of scores.
	//! The scores should be normalized between 0.0 and 1.0, where 1.0 is the highest score
	//! At least one entry is returned (if there is one).
	//! Strings are only returned if they have a score higher than the threshold.
	DUCKDB_API static vector<string> TopNStrings(vector<pair<string, double>> scores, idx_t n = 5,
	                                             double threshold = 0.5);
	//! DEPRECATED: old TopNStrings method that uses the levenshtein distance metric instead of the normalized 0.0 - 1.0
	//! rating
	DUCKDB_API static vector<string> TopNStrings(const vector<pair<string, idx_t>> &scores, idx_t n = 5,
	                                             idx_t threshold = 5);
	//! Computes the levenshtein distance of each string in strings, and compares it to target, then returns TopNStrings
	//! with the given params.
	DUCKDB_API static vector<string> TopNLevenshtein(const vector<string> &strings, const string &target, idx_t n = 5,
	                                                 idx_t threshold = 5);
	//! Computes the jaro winkler distance of each string in strings, and compares it to target, then returns
	//! TopNStrings with the given params.
	DUCKDB_API static vector<string> TopNJaroWinkler(const vector<string> &strings, const string &target, idx_t n = 5,
	                                                 double threshold = 0.5);
	DUCKDB_API static string CandidatesMessage(const vector<string> &candidates,
	                                           const string &candidate = "Candidate bindings");

	//! Generate an error message in the form of "{message_prefix}: nearest_string, nearest_string2, ...
	//! Equivalent to calling TopNLevenshtein followed by CandidatesMessage
	DUCKDB_API static string CandidatesErrorMessage(const vector<string> &strings, const string &target,
	                                                const string &message_prefix, idx_t n = 5);

	//! Returns true if two null-terminated strings are equal or point to the same address.
	//! Returns false if only one of the strings is nullptr
	static bool Equals(const char *s1, const char *s2) {
		if (s1 == s2) {
			return true;
		}
		if (s1 == nullptr || s2 == nullptr) {
			return false;
		}
		return strcmp(s1, s2) == 0;
	}

	//! JSON method that parses a { string: value } JSON blob
	//! NOTE: this method ONLY parses a JSON {"key": "value"} object, it does not support ANYTHING else
	//! NOTE: this method is not efficient
	//! NOTE: this method is used in Exception construction - as such it does NOT throw on invalid JSON, instead an
	//! empty map is returned
	DUCKDB_API static unordered_map<string, string> ParseJSONMap(const string &json);
	//! JSON method that constructs a { string: value } JSON map
	//! This is the inverse of ParseJSONMap
	//! NOTE: this method is not efficient
	DUCKDB_API static string ToJSONMap(ExceptionType type, const string &message,
	                                   const unordered_map<string, string> &map);

	DUCKDB_API static string GetFileName(const string &file_path);
	DUCKDB_API static string GetFileExtension(const string &file_name);
	DUCKDB_API static string GetFileStem(const string &file_name);
	DUCKDB_API static string GetFilePath(const string &file_path);

	struct EnumStringLiteral {
		uint32_t number;
		const char *string;
	};

	DUCKDB_API static uint32_t StringToEnum(const EnumStringLiteral enum_list[], idx_t enum_count,
	                                        const char *enum_name, const char *str_value);
	DUCKDB_API static const char *EnumToString(const EnumStringLiteral enum_list[], idx_t enum_count,
	                                           const char *enum_name, uint32_t enum_value);
	DUCKDB_API static const uint8_t ASCII_TO_LOWER_MAP[];
	DUCKDB_API static const uint8_t ASCII_TO_UPPER_MAP[];
};

} // namespace duckdb
