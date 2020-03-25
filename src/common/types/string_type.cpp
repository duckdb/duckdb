#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

void string_t::Verify() {
	auto dataptr = GetData();
	assert(dataptr);
	// check the data is a valid UTF8 string
	assert(Value::IsUTF8String(*this));
	// check that the data is a valid NFC UTF8 string
	auto normalized = Utf8Proc::Normalize(dataptr);
	assert(strcmp(dataptr, normalized) == 0);
	free(normalized);
	// verify that the string is null-terminated and that the length is correct
	assert(strlen(dataptr) == length);
	// verify that the prefix contains the first four characters of the string
	for (idx_t i = 0; i < std::min((uint32_t)PREFIX_LENGTH, length); i++) {
		assert(prefix[i] == dataptr[i]);
	}
	// verify that for strings with length < PREFIX_LENGTH, the rest of the prefix is zero
	for (idx_t i = length; i < PREFIX_LENGTH; i++) {
		assert(prefix[i] == '\0');
	}
}

} // namespace duckdb
