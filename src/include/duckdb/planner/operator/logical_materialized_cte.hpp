//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_materialized_cte.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class LogicalMaterializedCTE : public LogicalOperator {
	explicit LogicalMaterializedCTE() : LogicalOperator(LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
	}

public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_MATERIALIZED_CTE;

public:
	LogicalMaterializedCTE(string ctename, idx_t table_index, idx_t column_count, unique_ptr<LogicalOperator> cte,
	                       unique_ptr<LogicalOperator> child)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_MATERIALIZED_CTE), table_index(table_index),
	      column_count(column_count), ctename(ctename) {
		children.push_back(std::move(cte));
		children.push_back(std::move(child));
	}

	idx_t table_index;
	idx_t column_count;
	string ctename;

public:
	vector<ColumnBinding> GetColumnBindings() override {
		return children[1]->GetColumnBindings();
	}

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);

	vector<idx_t> GetTableIndex() const override;

protected:
	void ResolveTypes() override {
		types = children[1]->types;
	}
};
} // namespace duckdb
