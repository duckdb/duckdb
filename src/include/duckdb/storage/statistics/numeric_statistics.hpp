//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/statistics/numeric_statistics.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/statistics/segment_statistics.hpp"
#include "duckdb/common/serializer.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/windows_undefs.hpp"
#include "duckdb/common/enums/filter_propagate_result.hpp"

namespace duckdb {

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

class NumericStatistics : public BaseStatistics {
public:
	DUCKDB_API explicit NumericStatistics(LogicalType type);
	DUCKDB_API NumericStatistics(LogicalType type, const Value &min, const Value &max);

public:
	DUCKDB_API void Merge(const BaseStatistics &other) override;

	DUCKDB_API bool IsConstant() const override;

	bool HasMin() const {
		return has_min;
	}
	bool HasMax() const {
		return has_max;
	}
	//! Returns the min value - throws an exception if there is no min value
	DUCKDB_API Value Min() const;
	//! Returns the max value - throws an exception if there is no max value
	DUCKDB_API Value Max() const;
	DUCKDB_API void SetMin(const Value &min);
	DUCKDB_API void SetMax(const Value &max);

	DUCKDB_API FilterPropagateResult CheckZonemap(ExpressionType comparison_type, const Value &constant) const;

	unique_ptr<BaseStatistics> Copy() const override;
	void Serialize(FieldWriter &writer) const override;
	static unique_ptr<BaseStatistics> Deserialize(FieldReader &reader, LogicalType type);
	void Verify(Vector &vector, const SelectionVector &sel, idx_t count) const override;

	string ToString() const override;

private:
	//! Whether or not the value has a max value
	bool has_min;
	//! Whether or not the segment has a min value
	bool has_max;
	//! The minimum value of the segment
	NumericValueUnion min;
	//! The maximum value of the segment
	NumericValueUnion max;

private:
	template <class T>
	void TemplatedVerify(Vector &vector, const SelectionVector &sel, idx_t count) const;

public:
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
	static inline void Update(SegmentStatistics &stats, T new_value) {
		auto &nstats = (NumericStatistics &)*stats.statistics;
		UpdateValue<T>(new_value, nstats.min.GetReferenceUnsafe<T>(), nstats.max.GetReferenceUnsafe<T>());
	}
	void SetValueInternal(const Value &input, NumericValueUnion &val, bool &has_val);
	Value MinOrNull() const;
	Value MaxOrNull() const;
};

template <>
void NumericStatistics::Update<interval_t>(SegmentStatistics &stats, interval_t new_value);
template <>
void NumericStatistics::Update<list_entry_t>(SegmentStatistics &stats, list_entry_t new_value);

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
