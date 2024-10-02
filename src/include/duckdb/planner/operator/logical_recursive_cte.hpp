//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_recursive_cte.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

class LogicalRecursiveCTE : public LogicalOperator {
	LogicalRecursiveCTE() : LogicalOperator(LogicalOperatorType::LOGICAL_RECURSIVE_CTE) {
	}

public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_RECURSIVE_CTE;

public:
	LogicalRecursiveCTE(string ctename_p, idx_t table_index, idx_t column_count, bool union_all,
	                    unique_ptr<LogicalOperator> top, unique_ptr<LogicalOperator> bottom)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_RECURSIVE_CTE), union_all(union_all),
	      ctename(std::move(ctename_p)), table_index(table_index), column_count(column_count) {
		children.push_back(std::move(top));
		children.push_back(std::move(bottom));
	}

	bool union_all;
	string ctename;
	idx_t table_index;
	idx_t column_count;
	vector<CorrelatedColumnInfo> correlated_columns;

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
