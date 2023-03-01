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
struct UnifiedVectorFormat;

class BaseStatistics {
public:
	BaseStatistics(LogicalType type);
	virtual ~BaseStatistics();

	//! The validity stats of the column (if any)
	unique_ptr<BaseStatistics> validity_stats;
	// estimate that one may have even if distinct_stats==nullptr
	idx_t distinct_count;

public:
	static unique_ptr<BaseStatistics> CreateEmpty(LogicalType type);

	DUCKDB_API bool CanHaveNull() const;
	DUCKDB_API bool CanHaveNoNull() const;

	void UpdateDistinctStatistics(Vector &v, idx_t count);

	virtual bool IsConstant() const {
		return false;
	}

	const LogicalType &GetType() {
		return type;
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
};

} // namespace duckdb
