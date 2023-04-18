//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_cte.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class LogicalCTE : public LogicalOperator {
	LogicalCTE(idx_t table_index) : LogicalOperator(LogicalOperatorType::LOGICAL_CTE), table_index(table_index) {
	}

public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_CTE;

public:
	LogicalCTE(string ctename, idx_t table_index, unique_ptr<LogicalOperator> cte, unique_ptr<LogicalOperator> child)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_CTE), table_index(table_index), ctename(ctename) {
		children.push_back(std::move(cte));
		children.push_back(std::move(child));
	}

	idx_t table_index;
	string ctename;

public:
	vector<ColumnBinding> GetColumnBindings() override {
		return children[1]->GetColumnBindings();
	}
	void Serialize(FieldWriter &writer) const override;
	static unique_ptr<LogicalOperator> Deserialize(LogicalDeserializationState &state, FieldReader &reader);
	vector<idx_t> GetTableIndex() const override;

protected:
	void ResolveTypes() override {
		types = children[1]->types;
	}
};
} // namespace duckdb
