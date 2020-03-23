#pragma once

#include <string>

namespace duckdb {
class Utf8Proc {
public:
	static std::string Normalize(std::string s) {
		return std::string(Normalize(s.c_str()));
	}
	static char* Normalize(const char* s);

};
}
