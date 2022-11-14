#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/algorithm.hpp"
#include "utf8proc_wrapper.hpp"

namespace duckdb {

void string_t::Verify() const {
	auto dataptr = GetDataUnsafe();
	(void)dataptr;
	D_ASSERT(dataptr);

#ifdef DEBUG
	auto utf_type = Utf8Proc::Analyze(dataptr, GetSize());
	D_ASSERT(utf_type != UnicodeType::INVALID);
#endif

	// verify that the prefix contains the first four characters of the string
	for (idx_t i = 0; i < MinValue<uint32_t>(PREFIX_LENGTH, GetSize()); i++) {
		D_ASSERT(GetPrefix()[i] == dataptr[i]);
	}
	// verify that for strings with length < INLINE_LENGTH, the rest of the string is zero
	for (idx_t i = GetSize(); i < INLINE_LENGTH; i++) {
		D_ASSERT(GetDataUnsafe()[i] == '\0');
	}
}

} // namespace duckdb
