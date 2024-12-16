//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/statistics/numeric_stats.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/enums/filter_propagate_result.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/storage/statistics/numeric_stats_union.hpp"
#include "duckdb/common/array_ptr.hpp"

namespace duckdb {
class BaseStatistics;
struct SelectionVector;
class Vector;

struct NumericStatsData {
	//! Whether or not the value has a max value
	bool has_min;
	//! Whether or not the segment has a min value
	bool has_max;
	//! The minimum value of the segment
	NumericValueUnion min;
	//! The maximum value of the segment
	NumericValueUnion max;
};

struct NumericStats {
	//! Unknown statistics - i.e. "has_min" is false, "has_max" is false
	DUCKDB_API static BaseStatistics CreateUnknown(LogicalType type);
	//! Empty statistics - i.e. "min = MaxValue<type>, max = MinValue<type>"
	DUCKDB_API static BaseStatistics CreateEmpty(LogicalType type);

	//! Returns true if the stats has a constant value
	DUCKDB_API static bool IsConstant(const BaseStatistics &stats);
	//! Returns true if the stats has both a min and max value defined
	DUCKDB_API static bool HasMinMax(const BaseStatistics &stats);
	//! Returns true if the stats has a min value defined
	DUCKDB_API static bool HasMin(const BaseStatistics &stats);
	//! Returns true if the stats has a max value defined
	DUCKDB_API static bool HasMax(const BaseStatistics &stats);
	//! Returns the min value - throws an exception if there is no min value
	DUCKDB_API static Value Min(const BaseStatistics &stats);
	//! Returns the max value - throws an exception if there is no max value
	DUCKDB_API static Value Max(const BaseStatistics &stats);
	//! Sets the min value of the statistics
	DUCKDB_API static void SetMin(BaseStatistics &stats, const Value &val);
	//! Sets the max value of the statistics
	DUCKDB_API static void SetMax(BaseStatistics &stats, const Value &val);

	template <class T>
	static void SetMax(BaseStatistics &stats, T val) {
		auto &nstats = GetDataUnsafe(stats);
		nstats.has_max = true;
		nstats.max.GetReferenceUnsafe<T>() = val;
	}

	template <class T>
	static void SetMin(BaseStatistics &stats, T val) {
		auto &nstats = GetDataUnsafe(stats);
		nstats.has_min = true;
		nstats.min.GetReferenceUnsafe<T>() = val;
	}

	//! Check whether or not a given comparison with a constant could possibly be satisfied by rows given the statistics
	DUCKDB_API static FilterPropagateResult CheckZonemap(const BaseStatistics &stats, ExpressionType comparison_type,
	                                                     array_ptr<Value> constants);

	DUCKDB_API static void Merge(BaseStatistics &stats, const BaseStatistics &other_p);

	DUCKDB_API static void Serialize(const BaseStatistics &stats, Serializer &serializer);
	DUCKDB_API static void Deserialize(Deserializer &deserializer, BaseStatistics &stats);

	DUCKDB_API static string ToString(const BaseStatistics &stats);

	template <class T>
	static inline void UpdateValue(T new_value, T &min, T &max) {
		min = LessThan::Operation(new_value, min) ? new_value : min;
		max = GreaterThan::Operation(new_value, max) ? new_value : max;
	}
	template <class T>
	static inline void Update(NumericStatsData &nstats, T new_value) {
		UpdateValue<T>(new_value, nstats.min.GetReferenceUnsafe<T>(), nstats.max.GetReferenceUnsafe<T>());
	}

	static void Verify(const BaseStatistics &stats, Vector &vector, const SelectionVector &sel, idx_t count);

	template <class T>
	static T GetMin(const BaseStatistics &stats) {
		return NumericStats::Min(stats).GetValueUnsafe<T>();
	}
	template <class T>
	static T GetMax(const BaseStatistics &stats) {
		return NumericStats::Max(stats).GetValueUnsafe<T>();
	}
	template <class T>
	static T GetMinUnsafe(const BaseStatistics &stats);
	template <class T>
	static T GetMaxUnsafe(const BaseStatistics &stats);

private:
	static NumericStatsData &GetDataUnsafe(BaseStatistics &stats);
	static const NumericStatsData &GetDataUnsafe(const BaseStatistics &stats);
	static Value MinOrNull(const BaseStatistics &stats);
	static Value MaxOrNull(const BaseStatistics &stats);
	template <class T>
	static void TemplatedVerify(const BaseStatistics &stats, Vector &vector, const SelectionVector &sel, idx_t count);
};

} // namespace duckdb
