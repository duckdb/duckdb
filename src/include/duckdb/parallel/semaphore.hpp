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
} // namespace duckdb_moodycamel

namespace duckdb {

class semaphore { // NOLINT: match std naming style
	              // NOTE:
	              // To instantiate this class, these headers have to be included:
	              // #include "concurrentqueue.h"
	              // #include "lightweightsemaphore.h"

	// This header explicitly does not include these files because they are third_party headers
	// which should only be referenced by internal duckdb source files.

public:
	typedef std::make_signed<std::size_t>::type ssize_t;
	semaphore();

public:
	bool wait();                      // NOLINT: match std naming style
	bool wait(int64_t timeout_usecs); // NOLINT: match std naming style

public:
	void signal(ssize_t count = 1); // NOLINT: match std naming style

private:
	unsafe_unique_ptr<duckdb_moodycamel::LightweightSemaphore> sem;
};

} // namespace duckdb
