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

namespace duckdb {
struct SelectionVector;

class Serializer;
class Deserializer;
class FieldWriter;
class FieldReader;
class Vector;
class ValidityStatistics;
class DistinctStatistics;
struct VectorData;

enum StatisticsType { LOCAL_STATS = 0, GLOBAL_STATS = 1 };

class BaseStatistics {
public:
	BaseStatistics(LogicalType type, StatisticsType stats_type);
	virtual ~BaseStatistics();

	//! The type of the logical segment
	LogicalType type;
	//! The validity stats of the column (if any)
	unique_ptr<BaseStatistics> validity_stats;
	//! The approximate count distinct stats of the column (if any)
	unique_ptr<BaseStatistics> distinct_stats;
	//! Whether these are 'global' stats, i.e., over a whole table, or just over a segment
	//! Some statistics are more expensive to keep, therefore we only keep them globally
	StatisticsType stats_type;

public:
	static unique_ptr<BaseStatistics> CreateEmpty(LogicalType type, StatisticsType stats_type);

	bool CanHaveNull() const;
	bool CanHaveNoNull() const;

	void UpdateDistinctStatistics(Vector &v, idx_t count);

	virtual bool IsConstant() const {
		return false;
	}

	virtual void Merge(const BaseStatistics &other);

	virtual unique_ptr<BaseStatistics> Copy() const;
	void CopyBase(const BaseStatistics &orig);

	virtual void Serialize(Serializer &serializer) const;
	virtual void Serialize(FieldWriter &writer) const;

	static unique_ptr<BaseStatistics> Deserialize(Deserializer &source, LogicalType type);

	//! Verify that a vector does not violate the statistics
	virtual void Verify(Vector &vector, const SelectionVector &sel, idx_t count) const;
	void Verify(Vector &vector, idx_t count) const;

	virtual string ToString() const;

protected:
	void InitializeBase();
};

} // namespace duckdb
