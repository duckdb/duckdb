//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_recursive_cte.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/operator/logical_cte.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

class LogicalRecursiveCTE : public LogicalCTE {
	LogicalRecursiveCTE() : LogicalCTE(LogicalOperatorType::LOGICAL_RECURSIVE_CTE) {
	}

public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_RECURSIVE_CTE;

public:
	LogicalRecursiveCTE(string ctename_p, idx_t table_index, idx_t column_count, bool union_all,
	                    vector<unique_ptr<Expression>> key_targets, unique_ptr<LogicalOperator> top,
	                    unique_ptr<LogicalOperator> bottom)
	    : LogicalCTE(std::move(ctename_p), table_index, column_count, std::move(top), std::move(bottom),
	                 LogicalOperatorType::LOGICAL_RECURSIVE_CTE),
	      union_all(union_all), key_targets(std::move(key_targets)) {
	}

	bool union_all;
	// Flag if recurring table is referenced, if not we do not copy ht into ColumnDataCollection
	bool ref_recurring;
	vector<unique_ptr<Expression>> key_targets;

public:
	vector<ColumnBinding> GetColumnBindings() override {
		return GenerateColumnBindings(table_index, column_count);
	}

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);

	vector<idx_t> GetTableIndex() const override;
	string GetName() const override;

protected:
	void ResolveTypes() override {
		types = children[0]->types;
	}
};
} // namespace duckdb
