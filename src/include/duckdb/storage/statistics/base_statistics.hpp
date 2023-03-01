//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/statistics/base_statistics.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"
#include "duckdb/storage/statistics/string_stats.hpp"

namespace duckdb {
struct SelectionVector;

class Serializer;
class Deserializer;
class FieldWriter;
class FieldReader;
class Vector;
struct UnifiedVectorFormat;

enum class StatsInfo : uint8_t {
	CAN_HAVE_NULL_VALUES = 0,
	CANNOT_HAVE_NULL_VALUES = 1,
	CAN_HAVE_VALID_VALUES = 2,
	CANNOT_HAVE_VALID_VALUES = 3,
	CAN_HAVE_NULL_AND_VALID_VALUES = 4
};

class BaseStatistics {
	friend struct NumericStats;
	friend struct StringStats;
	friend struct StructStats;
	friend struct ListStats;

public:
	BaseStatistics(LogicalType type);
	virtual ~BaseStatistics();

public:
	static unique_ptr<BaseStatistics> CreateEmpty(LogicalType type);

	DUCKDB_API bool CanHaveNull() const;
	DUCKDB_API bool CanHaveNoNull() const;

	void SetDistinctCount(idx_t distinct_count);

	virtual bool IsConstant() const;

	const LogicalType &GetType() const {
		return type;
	}

	void Set(StatsInfo info);
	void CombineValidity(BaseStatistics &left, BaseStatistics &right);
	void CopyValidity(BaseStatistics &stats);
	void CopyValidity(BaseStatistics *stats);
	inline void SetHasNull() {
		has_null = true;
	}
	inline void SetHasNoNull() {
		has_no_null = true;
	}

	virtual void Merge(const BaseStatistics &other);

	virtual unique_ptr<BaseStatistics> Copy() const;
	void CopyBase(const BaseStatistics &orig);

	virtual void Serialize(Serializer &serializer) const;
	virtual void Serialize(FieldWriter &writer) const;

	virtual idx_t GetDistinctCount();

	static unique_ptr<BaseStatistics> Deserialize(Deserializer &source, LogicalType type);

	//! Verify that a vector does not violate the statistics
	virtual void Verify(Vector &vector, const SelectionVector &sel, idx_t count) const;
	void Verify(Vector &vector, idx_t count) const;

	virtual string ToString() const;

protected:
	void InitializeBase();

	//! The type of the logical segment
	LogicalType type;
	//! Whether or not the segment can contain NULL values
	bool has_null;
	//! Whether or not the segment can contain values that are not null
	bool has_no_null;
	// estimate that one may have even if distinct_stats==nullptr
	idx_t distinct_count;
	//! Numeric and String stats
	union {
		//! Numeric stats data, for numeric stats
		NumericStatsData numeric_data;
		//! String stats data, for string stats
		StringStatsData string_data;
	} stats_union;
	//! Child stats (for LIST and STRUCT)
	vector<unique_ptr<BaseStatistics>> child_stats;
};

} // namespace duckdb
