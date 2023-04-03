//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_pivot.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/parser/tableref/pivotref.hpp"

namespace duckdb {

class LogicalPivot : public LogicalOperator {
public:
	LogicalPivot(idx_t pivot_idx, unique_ptr<LogicalOperator> plan);

	idx_t pivot_index;
	//! The return types of the pivot operator
	vector<LogicalType> return_types;
	//! The set of values to pivot on
	vector<PivotValueElement> pivot_values;

public:
	vector<ColumnBinding> GetColumnBindings() override;
	void Serialize(FieldWriter &writer) const override;
	static unique_ptr<LogicalOperator> Deserialize(LogicalDeserializationState &state, FieldReader &reader);
	vector<idx_t> GetTableIndex() const override;

protected:
	void ResolveTypes() override;
};
} // namespace duckdb
