//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/statistics/unbounded_numeric_stats.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

//! The statistics of a NUMERIC column: the value range of exact numbers of any width and scale
struct UnboundedNumericStatsData : public ExtraStatsData {
	//! Whether min and max hold values
	bool has_min_max = false;
	//! Whether the value range is unknown (any exact value may be present)
	bool unknown = false;
	//! The largest number of fractional digits of any value
	uint8_t scale = 0;
	//! The smallest and largest value, stored as DECIMAL(width, scale)
	Value min;
	Value max;
};

struct UnboundedNumericStats {
	DUCKDB_API static BaseStatistics CreateUnknown(LogicalType type);
	DUCKDB_API static BaseStatistics CreateEmpty(LogicalType type);

	//! Merge statistics into these: unbounded numeric statistics, the numeric statistics of an exact type, or validity
	DUCKDB_API static void Merge(BaseStatistics &stats, const BaseStatistics &other);
	DUCKDB_API static void Copy(BaseStatistics &stats, const BaseStatistics &other);
	//! Widen the range to include a value of an exact numeric type
	DUCKDB_API static void Update(BaseStatistics &stats, const Value &value);
	DUCKDB_API static void SetUnknown(BaseStatistics &stats);

	DUCKDB_API static void Serialize(const BaseStatistics &stats, Serializer &serializer);
	DUCKDB_API static void Deserialize(Deserializer &deserializer, BaseStatistics &stats);
	DUCKDB_API static child_list_t<Value> ToStruct(const BaseStatistics &stats);

	DUCKDB_API static bool HasMinMax(const BaseStatistics &stats);
	DUCKDB_API static bool IsUnknown(const BaseStatistics &stats);
	DUCKDB_API static uint8_t GetScale(const BaseStatistics &stats);
	DUCKDB_API static const Value &Min(const BaseStatistics &stats);
	DUCKDB_API static const Value &Max(const BaseStatistics &stats);

	//! The representation that a NUMERIC column with these statistics is bound to
	DUCKDB_API static LogicalType ResolveType(const BaseStatistics &stats);
	//! These statistics as the numeric statistics of an exact type - unknown when the range does not fit the type
	DUCKDB_API static BaseStatistics ToTyped(const BaseStatistics &stats, const LogicalType &type);
	//! The statistics of an exact type (numeric statistics, or string statistics for wide decimals) as unbounded
	//! numeric statistics
	DUCKDB_API static BaseStatistics FromTyped(const BaseStatistics &typed_stats);

private:
	static UnboundedNumericStatsData &GetData(BaseStatistics &stats);
	static const UnboundedNumericStatsData &GetData(const BaseStatistics &stats);
	static void MergeRange(UnboundedNumericStatsData &data, const UnboundedNumericStatsData &other);
};

} // namespace duckdb
