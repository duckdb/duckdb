#include "duckdb/common/unique_ptr_utils.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

void __unique_ptr_utils::AssertNotNull(void *ptr) {
	if (!ptr) {
		throw InternalException("Attempted to dereference unique_ptr that is NULL!");
	}
}

} // namespace duckdb
