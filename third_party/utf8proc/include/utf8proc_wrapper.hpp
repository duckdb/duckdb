#pragma once

#include <string>
#include <cassert>
#include <cstring>

namespace duckdb {

enum class UnicodeType {INVALID, ASCII, UNICODE};


class Utf8Proc {
public:
	static UnicodeType Analyze(const char* s) {
		return Analyze(s, std::strlen(s));
	}

	static UnicodeType Analyze(std::string s) {
		return Analyze(s.c_str(), s.size());
	}

	static UnicodeType Analyze(const char *s, size_t len);


	static std::string Normalize(std::string s);

	static char* Normalize(const char* s);

	//! Returns whether or not the UTF8 string is valid
	static bool IsValid(const char *s, size_t len);
	//! Returns the position (in bytes) of the next grapheme cluster
	static size_t NextGraphemeCluster(const char *s, size_t len, size_t pos);
	//! Returns the position (in bytes) of the previous grapheme cluster
	static size_t PreviousGraphemeCluster(const char *s, size_t len, size_t pos);
};

}
