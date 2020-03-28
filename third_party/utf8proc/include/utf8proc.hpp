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

};
}
