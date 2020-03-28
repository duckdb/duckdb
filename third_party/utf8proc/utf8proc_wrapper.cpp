#include "utf8proc_wrapper.hpp"
#include "utf8proc.hpp"

using namespace duckdb;
using namespace std;

// This function efficiently checks if a string is valid UTF8.
// It was originally written by Sjoerd Mullender.

// Here is the table that makes it work:

// B 		= Number of Bytes in UTF8 encoding
// C_MIN 	= First Unicode code point
// C_MAX 	= Last Unicode code point
// B1 		= First Byte Prefix

// 	B	C_MIN		C_MAX		B1
//	1	U+000000	U+00007F		0xxxxxxx
//	2	U+000080	U+0007FF		110xxxxx
//	3	U+000800	U+00FFFF		1110xxxx
//	4	U+010000	U+10FFFF		11110xxx

UnicodeType Utf8Proc::Analyze(const char *s, size_t len) {
	UnicodeType type = UnicodeType::ASCII;
	char c;
	for (size_t i = 0; i < len; i++) {
		c = s[i];
		// 1 Byte / ASCII
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
