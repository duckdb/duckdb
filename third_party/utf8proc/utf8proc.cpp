#include "utf8proc.hpp"
#include "utf8proc.h"

using namespace duckdb;
using namespace std;


bool Utf8Proc::IsValid(const char* s, size_t len) {
	assert(s);

	auto string_position_ptr = (const utf8proc_uint8_t* ) s;
	utf8proc_ssize_t codepoint_len;
	utf8proc_int32_t codepoint;

	while ((codepoint_len = utf8proc_iterate(string_position_ptr, len, &codepoint)) > 0) {
		string_position_ptr += codepoint_len;
		len -= codepoint_len;
		if (!utf8proc_codepoint_valid(codepoint)) {
			return false;
		}
	  }
	return len == 0;
};

char* Utf8Proc::Normalize(const char* s) {
	assert(s);
	assert(Utf8Proc::IsValid(s));
	return (char*) utf8proc_NFC((const utf8proc_uint8_t* )s);
};

char* Utf8Proc::Upper(const char* s, size_t len) {
	assert(s);
	assert(Utf8Proc::IsValid(s));
	return (char*) s;
//	return (char*) utf8proc_tolower((const utf8proc_uint8_t* )s);
};

char* Utf8Proc::Lower(const char* s, size_t len) {
	assert(s);
	assert(Utf8Proc::IsValid(s));
	return (char*) s;
//	return (char*) utf8proc_tolower((const utf8proc_uint8_t* )s);
};
