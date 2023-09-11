//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/statistics/distinct_statistics.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/types/hyperloglog.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"

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

	void Update(Vector &update, idx_t count, bool sample = true);
	void Update(UnifiedVectorFormat &update_data, const LogicalType &ptype, idx_t count, bool sample = true);

	string ToString() const;
	idx_t GetCount() const;

	static bool TypeIsSupported(const LogicalType &type);

	void Serialize(Serializer &serializer) const;
	static unique_ptr<DistinctStatistics> Deserialize(Deserializer &deserializer);

private:
	//! For distinct statistics we sample the input to speed up insertions
	static constexpr const double SAMPLE_RATE = 0.1;
};

} // namespace duckdb
