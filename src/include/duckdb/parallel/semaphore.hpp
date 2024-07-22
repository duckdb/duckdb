//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/semaphore.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/typedefs.hpp"

// fwd declare LightweightSemaphore;
namespace duckdb_moodycamel {
	class LightweightSemaphore;
};

namespace duckdb {

class semaphore { // NOLINT: match std naming style
// NOTE:
// To instantiate this class, these headers have to be included:
// #include "concurrentqueue.h"
// #include "lightweightsemaphore.h"

// This header explicitly does not include these files because they are third_party headers
// which should only be referenced by internal duckdb source files.

public:
	semaphore();
public:
	bool wait();
	bool wait(int64_t timeout_usecs);
public:
	void signal(ssize_t count = 1);
private:
	unsafe_unique_ptr<duckdb_moodycamel::LightweightSemaphore> _sem;
};

} // namespace duckdb
