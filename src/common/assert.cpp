#include "duckdb/common/assert.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/likely.hpp"

namespace duckdb {

void DuckDBAssertInternal(bool condition, const char *condition_name, const char *file, int linenr) {
#ifdef DISABLE_ASSERTIONS
	return;
#endif
	if (DUCKDB_LIKELY(condition)) {
		return;
	}
	throw InternalException("Assertion triggered in file \"%s\" on line %d: %s%s", file, linenr, condition_name,
	                        Exception::GetStackTrace());
}

} // namespace duckdb
