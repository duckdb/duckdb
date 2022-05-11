//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/statistics/distinct_statistics.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/hyperloglog.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"

namespace duckdb {
class Serializer;
class Deserializer;
class Vector;

class DistinctStatistics : public BaseStatistics {
public:
	DistinctStatistics();
	explicit DistinctStatistics(unique_ptr<HyperLogLog> log);

	//! The HLL of the segment
	unique_ptr<HyperLogLog> log;

public:
	void Merge(const BaseStatistics &other) override;

	unique_ptr<BaseStatistics> Copy() const override;

	void Serialize(Serializer &serializer) const override;
	void Serialize(FieldWriter &writer) const override;

	static unique_ptr<DistinctStatistics> Deserialize(Deserializer &source);
	static unique_ptr<DistinctStatistics> Deserialize(FieldReader &reader);

	void Update(Vector &update, idx_t count);
	void Update(VectorData &update_data, const LogicalType &ptype, idx_t count);

	string ToString() const override;

private:
	//! For distinct statistics we sample the input
	//! This speeds up insertions
	static constexpr const double SAMPLE_RATE = 0.1;
};

} // namespace duckdb
