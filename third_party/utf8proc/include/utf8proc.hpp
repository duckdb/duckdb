#pragma once

#include <string>

namespace duckdb {
class Utf8Proc {
public:

	static bool IsValid(std::string s) {
		return IsValid(s.c_str());
	}

	static bool IsValid(const char* s) {
		assert(s);
		return IsValid(s, strlen(s));
	}
	static bool IsValid(const char* s, size_t len);

	static bool IsAscii(std::string s) {
		return IsAscii(s.c_str());
	}

	static bool IsAscii(const char* s) {
		assert(s);
		return IsAscii(s, strlen(s));
	}
	static bool IsAscii(const char* s, size_t len);


	static std::string Normalize(std::string s) {
		return std::string(Normalize(s.c_str()));
	}

	static char* Normalize(const char* s);

	static std::string Upper(std::string s) {
		return std::string(Upper(s.c_str(), s.size()));
	}

	static char* Upper(const char* s) {
		assert(s);
		return Upper(s, strlen(s));
	}

	static char* Upper(const char* s, size_t len);

	static std::string Lower(std::string s) {
		return std::string(Lower(s.c_str(), s.size()));
	}

	static char* Lower(const char* s) {
		assert(s);
		return Lower(s, strlen(s));
	}

	static char* Lower(const char* s, size_t len);
};
}
