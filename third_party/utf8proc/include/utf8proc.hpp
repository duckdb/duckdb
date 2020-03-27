#pragma once

#include <string>
#include <cassert>

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

	static std::string Upper(std::string s) {
		return std::string(Upper(s.c_str(), s.size()));
	}

	static char* Upper(const char* s) {
		assert(s);
		return Upper(s, std::strlen(s));
	}

	static char* Upper(const char* s, size_t len);

	static std::string Lower(std::string s) {
		return std::string(Lower(s.c_str(), s.size()));
	}

	static char* Lower(const char* s) {
		assert(s);
		return Lower(s, std::strlen(s));
	}

	static char* Lower(const char* s, size_t len);
};
}
