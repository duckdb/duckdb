//===----------------------------------------------------------------------===//
//                         DuckDB
//
// utf8proc_wrapper.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <cassert>
#include <cstring>
#include <cstdint>

namespace duckdb {
class GraphemeIterator;

enum class UnicodeType { INVALID, ASCII, UNICODE };
enum class UnicodeInvalidReason { BYTE_MISMATCH, INVALID_UNICODE };

class Utf8Proc {
public:
	//! Distinguishes ASCII, Valid UTF8 and Invalid UTF8 strings
	static UnicodeType Analyze(const char *s, size_t len, UnicodeInvalidReason *invalid_reason = nullptr, size_t *invalid_pos = nullptr);
	//! Performs UTF NFC normalization of string, return value needs to be free'd
	static char* Normalize(const char* s, size_t len);
	//! Returns whether or not the UTF8 string is valid
	static bool IsValid(const char *s, size_t len);
	//! Makes Invalid Unicode valid by replacing invalid parts with a given character
	static void MakeValid(char *s, size_t len, char special_flag = '?');
	//! Returns the position (in bytes) of the next grapheme cluster
	static size_t NextGraphemeCluster(const char *s, size_t len, size_t pos);
	//! Returns the position (in bytes) of the previous grapheme cluster
	static size_t PreviousGraphemeCluster(const char *s, size_t len, size_t pos);

	//! Transform a codepoint to utf8 and writes it to "c", sets "sz" to the size of the codepoint
	static bool CodepointToUtf8(int cp, int &sz, char *c);
	//! Returns the codepoint length in bytes when encoded in UTF8
	static int CodepointLength(int cp);
	//! Transform a UTF8 string to a codepoint; returns the codepoint and writes the length of the codepoint (in UTF8) to sz
	static int32_t UTF8ToCodepoint(const char *c, int &sz);
	//! Returns the render width of a single character in a string
	static size_t RenderWidth(const char *s, size_t len, size_t pos);
	static size_t RenderWidth(const std::string &str);

	static int32_t CodepointToUpper(int32_t codepoint);
	static int32_t CodepointToLower(int32_t codepoint);

	//! Constructs a class that can be iterated over to fetch grapheme clusters in a string
	static GraphemeIterator GraphemeClusters(const char *s, size_t len);

	//! Returns the number of grapheme clusters in a string
	static size_t GraphemeCount(const char *s, size_t len);


};

struct GraphemeCluster {
	size_t start;
	size_t end;
};

class GraphemeIterator {
public:
	GraphemeIterator(const char *s, size_t len);

private:
	const char *s;
	size_t len;

private:
	class GraphemeClusterIterator {
	public:
		GraphemeClusterIterator(const char *s, size_t len);

		const char *s;
		size_t len;
		GraphemeCluster cluster;

	public:
		void Next();
		void SetInvalid();
		bool IsInvalid() const;

		GraphemeClusterIterator &operator++();
		bool operator!=(const GraphemeClusterIterator &other) const;
		GraphemeCluster operator*() const;
	};

public:
	GraphemeClusterIterator begin() { // NOLINT: match stl API
		return GraphemeClusterIterator(s, len);
	}
	GraphemeClusterIterator end() { // NOLINT: match stl API
		return GraphemeClusterIterator(nullptr, 0);
	}
};


}
