#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

void string_t::Verify() {
	auto dataptr = GetData();
	assert(dataptr);
	assert(strlen(dataptr) == length);
	assert(Value::IsUTF8String(*this));
	for(index_t i = 0; i < std::min((uint32_t) PREFIX_LENGTH, length); i++) {
		assert(prefix[i] == dataptr[i]);
	}
}

}
