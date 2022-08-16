//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/hyperloglog.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/common/types/vector.hpp"
#include "hyperloglog.hpp"

namespace duckdb {

enum class HLLStorageType { UNCOMPRESSED = 1 };

class FieldWriter;
class FieldReader;

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
	idx_t Count() const;
	//! Merge this HyperLogLog counter with another counter to create a new one
	unique_ptr<HyperLogLog> Merge(HyperLogLog &other);
	HyperLogLog *MergePointer(HyperLogLog &other);
	//! Merge a set of HyperLogLogs to create one big one
	static unique_ptr<HyperLogLog> Merge(HyperLogLog logs[], idx_t count);
	//! Get the size (in bytes) of a HLL
	static idx_t GetSize();
	//! Get pointer to the HLL
	data_ptr_t GetPtr() const;
	//! Get copy of the HLL
	unique_ptr<HyperLogLog> Copy();
	//! (De)Serialize the HLL
	void Serialize(FieldWriter &writer) const;
	static unique_ptr<HyperLogLog> Deserialize(FieldReader &reader);

public:
	//! Compute HLL hashes over vdata, and store them in 'hashes'
	//! Then, compute register indices and prefix lengths, and also store them in 'hashes' as a pair of uint32_t
	static void ProcessEntries(UnifiedVectorFormat &vdata, const LogicalType &type, uint64_t hashes[], uint8_t counts[],
	                           idx_t count);
	//! Add the indices and counts to the logs
	static void AddToLogs(UnifiedVectorFormat &vdata, idx_t count, uint64_t indices[], uint8_t counts[],
	                      HyperLogLog **logs[], const SelectionVector *log_sel);
	//! Add the indices and counts to THIS log
	void AddToLog(UnifiedVectorFormat &vdata, idx_t count, uint64_t indices[], uint8_t counts[]);

private:
	explicit HyperLogLog(void *hll);

	void *hll;
	mutex lock;
};
} // namespace duckdb
