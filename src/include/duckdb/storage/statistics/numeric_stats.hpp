//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/statistics/numeric_stats.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/enums/filter_propagate_result.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {
class BaseStatistics;
class FieldWriter;
class FieldReader;
struct SelectionVector;
class Vector;

struct NumericValueUnion {
	union Val {
		int8_t boolean;
		int8_t tinyint;
		int16_t smallint;
		int32_t integer;
		int64_t bigint;
		uint8_t utinyint;
		uint16_t usmallint;
		uint32_t uinteger;
		uint64_t ubigint;
		hugeint_t hugeint;
		float float_;
		double double_;
	} value_;

	template <class T>
	T &GetReferenceUnsafe() {
		throw InternalException("NumericValueUnion::GetReferenceUnsafe called on unsupported type");
	}
};

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

	//! Check whether or not a given comparison with a constant could possibly be satisfied by rows given the statistics
	DUCKDB_API static FilterPropagateResult CheckZonemap(const BaseStatistics &stats, ExpressionType comparison_type,
	                                                     const Value &constant);

	DUCKDB_API static void Merge(BaseStatistics &stats, const BaseStatistics &other_p);

	DUCKDB_API static void Serialize(const BaseStatistics &stats, FieldWriter &writer);
	DUCKDB_API static BaseStatistics Deserialize(FieldReader &reader, LogicalType type);

	DUCKDB_API static string ToString(const BaseStatistics &stats);

	template <class T>
	static inline void UpdateValue(T new_value, T &min, T &max) {
		if (LessThan::Operation(new_value, min)) {
			min = new_value;
		}
		if (GreaterThan::Operation(new_value, max)) {
			max = new_value;
		}
	}

	template <class T>
	static inline void Update(BaseStatistics &stats, T new_value) {
		auto &nstats = NumericStats::GetDataUnsafe(stats);
		UpdateValue<T>(new_value, nstats.min.GetReferenceUnsafe<T>(), nstats.max.GetReferenceUnsafe<T>());
	}

	static void Verify(const BaseStatistics &stats, Vector &vector, const SelectionVector &sel, idx_t count);

	template <class T>
	static T GetMinUnsafe(const BaseStatistics &stats) {
		return NumericStats::Min(stats).template GetValueUnsafe<T>();
	}
	template <class T>
	static T GetMaxUnsafe(const BaseStatistics &stats) {
		return NumericStats::Max(stats).template GetValueUnsafe<T>();
	}

private:
	static NumericStatsData &GetDataUnsafe(BaseStatistics &stats);
	static const NumericStatsData &GetDataUnsafe(const BaseStatistics &stats);
	static Value MinOrNull(const BaseStatistics &stats);
	static Value MaxOrNull(const BaseStatistics &stats);
	template <class T>
	static void TemplatedVerify(const BaseStatistics &stats, Vector &vector, const SelectionVector &sel, idx_t count);
};

template <>
void NumericStats::Update<interval_t>(BaseStatistics &stats, interval_t new_value);
template <>
void NumericStats::Update<list_entry_t>(BaseStatistics &stats, list_entry_t new_value);

template <>
int8_t &NumericValueUnion::GetReferenceUnsafe();
template <>
int16_t &NumericValueUnion::GetReferenceUnsafe();
template <>
int32_t &NumericValueUnion::GetReferenceUnsafe();
template <>
int64_t &NumericValueUnion::GetReferenceUnsafe();
template <>
hugeint_t &NumericValueUnion::GetReferenceUnsafe();
template <>
uint8_t &NumericValueUnion::GetReferenceUnsafe();
template <>
uint16_t &NumericValueUnion::GetReferenceUnsafe();
template <>
uint32_t &NumericValueUnion::GetReferenceUnsafe();
template <>
uint64_t &NumericValueUnion::GetReferenceUnsafe();
template <>
float &NumericValueUnion::GetReferenceUnsafe();
template <>
double &NumericValueUnion::GetReferenceUnsafe();

} // namespace duckdb
