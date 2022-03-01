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

class BaseStatistics {
public:
	explicit BaseStatistics(LogicalType type);
	virtual ~BaseStatistics();

	//! The type of the logical segment
	LogicalType type;
	//! The validity stats of the column (if any)
	unique_ptr<BaseStatistics> validity_stats;

public:
	bool CanHaveNull() const;
	bool CanHaveNoNull() const;

	virtual bool IsConstant() const {
		return false;
	}

	static unique_ptr<BaseStatistics> CreateEmpty(LogicalType type);

	virtual void Merge(const BaseStatistics &other);

	virtual unique_ptr<BaseStatistics> Copy() const;
	void Serialize(Serializer &serializer) const;
	virtual void Serialize(FieldWriter &writer) const;
	static unique_ptr<BaseStatistics> Deserialize(Deserializer &source, LogicalType type);

	//! Verify that a vector does not violate the statistics
	virtual void Verify(Vector &vector, const SelectionVector &sel, idx_t count) const;
	void Verify(Vector &vector, idx_t count) const;

	virtual string ToString() const;
};

} // namespace duckdb
