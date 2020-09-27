#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/algorithm.hpp"
#include "utf8proc_wrapper.hpp"

namespace duckdb {

void string_t::Verify() {
	auto dataptr = GetData();
	(void)dataptr;
	assert(dataptr);

#ifdef DEBUG
	auto utf_type = Utf8Proc::Analyze(dataptr, GetSize());
	assert(utf_type != UnicodeType::INVALID);
#endif

	// verify that the string is null-terminated and that the length is correct
	assert(strlen(dataptr) == GetSize());
	// verify that the prefix contains the first four characters of the string
	for (idx_t i = 0; i < MinValue<uint32_t>(PREFIX_LENGTH, GetSize()); i++) {
		assert(GetPrefix()[i] == dataptr[i]);
	}
	// verify that for strings with length < PREFIX_LENGTH, the rest of the prefix is zero
	for (idx_t i = GetSize(); i < PREFIX_LENGTH; i++) {
		assert(GetPrefix()[i] == '\0');
	}
}

} // namespace duckdb
