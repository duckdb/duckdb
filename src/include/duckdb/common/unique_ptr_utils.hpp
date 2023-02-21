#pragma once

#include "duckdb/common/winapi.hpp"

namespace duckdb {

struct __unique_ptr_utils {
	DUCKDB_API static void AssertNotNull(void *ptr);
};

} // namespace duckdb
