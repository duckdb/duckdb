//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/statistics/column_statistics.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/statistics/distinct_statistics.hpp"

namespace duckdb {
class Serializer;

class ColumnStatistics {
public:
	explicit ColumnStatistics(BaseStatistics stats_p);
	ColumnStatistics(BaseStatistics stats_p, unique_ptr<DistinctStatistics> distinct_stats_p);

public:
	static shared_ptr<ColumnStatistics> CreateEmptyStats(const LogicalType &type);

	void Merge(ColumnStatistics &other);

	void UpdateDistinctStatistics(Vector &v, idx_t count);

	BaseStatistics &Statistics();

	bool HasDistinctStats();
	DistinctStatistics &DistinctStats();
	void SetDistinct(unique_ptr<DistinctStatistics> distinct_stats);

	shared_ptr<ColumnStatistics> Copy() const;

	void Serialize(Serializer &serializer) const;
	static shared_ptr<ColumnStatistics> Deserialize(Deserializer &source);

private:
	BaseStatistics stats;
	//! The approximate count distinct stats of the column
	unique_ptr<DistinctStatistics> distinct_stats;
};

} // namespace duckdb
