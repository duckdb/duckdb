//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/statistics/distinct_statistics.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/types/hyperloglog.hpp"

namespace duckdb {
class Vector;
class Serializer;
class Deserializer;

class DistinctStatistics {
public:
	DistinctStatistics();
	explicit DistinctStatistics(unique_ptr<HyperLogLog> log, idx_t sample_count, idx_t total_count);

	//! The HLL of the table
	unique_ptr<HyperLogLog> log;
	//! How many values have been sampled into the HLL
	atomic<idx_t> sample_count;
	//! How many values have been inserted (before sampling)
	atomic<idx_t> total_count;

public:
	void Merge(const DistinctStatistics &other);

	unique_ptr<DistinctStatistics> Copy() const;

	void UpdateSample(Vector &new_data, idx_t count, Vector &hashes);
	void Update(Vector &new_data, idx_t count, Vector &hashes);

	string ToString() const;
	idx_t GetCount() const;

	static bool TypeIsSupported(const LogicalType &type);

	void Serialize(Serializer &serializer) const;
	static unique_ptr<DistinctStatistics> Deserialize(Deserializer &deserializer);

private:
	void UpdateInternal(Vector &update, idx_t count, Vector &hashes);

private:
	//! For distinct statistics we sample the input to speed up insertions
	static constexpr double BASE_SAMPLE_RATE = 0.1;
	//! For integers, we sample more: likely to be join keys (and hashing is cheaper than, e.g., strings)
	static constexpr double INTEGRAL_SAMPLE_RATE = 0.3;
};

} // namespace duckdb
