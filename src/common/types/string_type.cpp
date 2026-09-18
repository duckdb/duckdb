#include "duckdb/common/types/string_type.hpp"

#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/types/bit.hpp"
#include "duckdb/common/types/value.hpp"
#include "utf8proc_wrapper.hpp"

namespace duckdb {
constexpr idx_t string_t::MAX_STRING_SIZE;
constexpr idx_t string_t::INLINE_LENGTH;

void string_t::Verify() const {
#ifdef DEBUG
	ForceVerify();
#endif
}

void string_t::ForceVerify() const {
	VerifyUTF8();
	VerifyCharacters();
}

void string_t::VerifyUTF8() const {
	auto dataptr = GetData();
	(void)dataptr;
	D_ASSERT(dataptr);

	auto utf_type = Utf8Proc::Analyze(dataptr, GetSize());
	if (utf_type == UnicodeType::INVALID) {
		throw InternalException("Invalid UTF8 found in string - %s", string(dataptr, GetSize()));
	}
}

void string_t::VerifyCharacters() const {
	auto dataptr = GetData();
	(void)dataptr;
	D_ASSERT(dataptr);

	// verify that the prefix contains the first four characters of the string
	for (idx_t i = 0; i < MinValue<idx_t>(PREFIX_LENGTH, GetSize()); i++) {
		if (GetPrefix()[i] != dataptr[i]) {
			throw InternalException("Internal string inconsistency - string_t prefix did not match actual string "
			                        "prefix.\nThis could mean a missing string_t::Finalize() call.");
		}
	}
	// verify that for strings with length <= INLINE_LENGTH, the rest of the string is zero
	for (idx_t i = GetSize(); i < INLINE_LENGTH; i++) {
		if (GetData()[i] != '\0') {
			throw InternalException("Internal string inconsistency - string_t data did not contain padding "
			                        "zero's\nThis could mean a missing string_t::Finalize() call.");
		}
	}
}

} // namespace duckdb
