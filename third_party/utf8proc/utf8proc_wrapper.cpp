#include "utf8proc_wrapper.hpp"
#include "utf8proc_wrapper.h"
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

char* Utf8Proc::Normalize(const char *s) {
	assert(s);
	assert(Utf8Proc::Analyze(s) != UnicodeType::INVALID);
	return (char*) utf8proc_NFC((const utf8proc_uint8_t*) s);
}

bool Utf8Proc::IsValid(const char *s, size_t len) {
	return Utf8Proc::Analyze(s, len) != UnicodeType::INVALID;
}

size_t Utf8Proc::NextGraphemeCluster(const char *s, size_t len, size_t cpos) {
	return utf8proc_next_grapheme(s, len, cpos);
}

size_t Utf8Proc::PreviousGraphemeCluster(const char *s, size_t len, size_t cpos) {
	if (!Utf8Proc::IsValid(s, len)) {
		return cpos - 1;
	}
	size_t current_pos = 0;
	while(true) {
		size_t new_pos = NextGraphemeCluster(s, len, current_pos);
		if (new_pos <= current_pos || new_pos >= cpos) {
			return current_pos;
		}
		current_pos = new_pos;
	}
}

size_t utf8proc_next_grapheme_cluster(const char *s, size_t len, size_t pos) {
	return Utf8Proc::NextGraphemeCluster(s, len, pos);
}

size_t utf8proc_prev_grapheme_cluster(const char *s, size_t len, size_t pos) {
	return Utf8Proc::PreviousGraphemeCluster(s, len, pos);
}

size_t utf8proc_render_width(const char *s, size_t len, size_t pos) {
	int sz;
	auto codepoint = utf8proc_codepoint(s + pos, sz);
	auto properties = utf8proc_get_property(codepoint);
	return properties->charwidth;
}

int utf8proc_is_valid(const char *s, size_t len) {
	return Utf8Proc::IsValid(s, len) ? 1 : 0;
}
