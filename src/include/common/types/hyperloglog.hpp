//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/types/hyperloglog.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/vector.hpp"

namespace duckdb {

//! The HyperLogLog class holds a HyperLogLog counter for approximate cardinality counting
class HyperLogLog {
public:
	HyperLogLog();
	~HyperLogLog();
	// implicit copying of HyperLogLog is not allowed
	HyperLogLog(const HyperLogLog &) = delete;

	//! Adds an element of the specified size to the HyperLogLog counter
	void Add(uint8_t *element, uint64_t size);
	//! Return the count of this HyperLogLog counter
	uint64_t Count();
	//! Merge this HyperLogLog counter with another counter to create a new one
	unique_ptr<HyperLogLog> Merge(HyperLogLog &other);
	//! Merge a set of HyperLogLogs to create one big one
	static unique_ptr<HyperLogLog> Merge(HyperLogLog logs[], uint64_t count);

private:
	HyperLogLog(void *hll);

	void *hll;
};
} // namespace duckdb
