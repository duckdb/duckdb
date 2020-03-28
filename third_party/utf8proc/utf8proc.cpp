#include "utf8proc.hpp"
#include "utf8proc.h"

using namespace duckdb;
using namespace std;

UnicodeType Utf8Proc::Analyze(const char *s, size_t len) {
	UnicodeType type = UnicodeType::ASCII;
	char c;
	for (size_t i = 0; i < len; i++) {
		c = s[i];
		if ((c & 0x80) == 0)
			continue;
		type = UnicodeType::UNICODE;
		if ((s[++i] & 0xC0) != 0x80)
			return UnicodeType::INVALID;
		if ((c & 0xE0) == 0xC0)
			continue;
		if ((s[++i] & 0xC0) != 0x80)
			return UnicodeType::INVALID;
		if ((c & 0xF0) == 0xE0)
			continue;
		if ((s[++i] & 0xC0) != 0x80)
			return UnicodeType::INVALID;
		if ((c & 0xF8) == 0xF0)
			continue;
		return UnicodeType::INVALID;
	}

	return type;
}


std::string Utf8Proc::Normalize(std::string s) {
	auto normalized = Normalize(s.c_str());
	auto res = std::string(normalized);
	free(normalized);
	return res;
}
;

char* Utf8Proc::Normalize(const char *s) {
	assert(s);
	assert(Utf8Proc::Analyze(s) != UnicodeType::INVALID);
	return (char*) utf8proc_NFC((const utf8proc_uint8_t*) s);
}
;

;
