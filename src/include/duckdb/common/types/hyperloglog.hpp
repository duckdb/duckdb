//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/hyperloglog.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/vector.hpp"
#include "hyperloglog.hpp"

namespace duckdb {

//! The HyperLogLog class holds a HyperLogLog counter for approximate cardinality counting
class HyperLogLog {
public:
	HyperLogLog();
	~HyperLogLog();
	// implicit copying of HyperLogLog is not allowed
	HyperLogLog(const HyperLogLog &) = delete;

	//! Adds an element of the specified size to the HyperLogLog counter
	void Add(data_ptr_t element, idx_t size);
	//! Return the count of this HyperLogLog counter
	idx_t Count();
	//! Merge this HyperLogLog counter with another counter to create a new one
	unique_ptr<HyperLogLog> Merge(HyperLogLog &other);
	HyperLogLog *MergePointer(HyperLogLog &other);
	//! Merge a set of HyperLogLogs to create one big one
	static unique_ptr<HyperLogLog> Merge(HyperLogLog logs[], idx_t count);

	static constexpr unsigned int SEED = 0xadc83b19ULL;
	static constexpr uint64_t M = 0xc6a4a7935bd1e995;
	static constexpr int R = 47;

	//! Compute HLL hashes over vdata, and store them in 'hashes'
	//! Then, compute register indices and prefix lengths, and also store them in 'hashes' as a pair of uint32_t
	static void ComputeHashes(VectorData &vdata, PhysicalType type, idx_t count, uint64_t hashes[]);
	//! Add all hashes to this HLL
	void AddHashes(VectorData &vdata, idx_t count, uint64_t hashes[]);
	//! Add a single entry to this HLL
	int AddHash(uint64_t &hash);

private:
	explicit HyperLogLog(void *hll);

	void *hll;
};
} // namespace duckdb