#include "utf8proc.hpp"
#include "utf8proc.h"

using namespace duckdb;
using namespace std;

char* Utf8Proc::Normalize(const char* s) {
	assert(s);
	return (char*) utf8proc_NFC((const utf8proc_uint8_t* )s);
};
