#pragma once

namespace duckdb {

struct __unique_ptr_utils {
	static void AssertNotNull(void *ptr);
};

} // namespace duckdb
