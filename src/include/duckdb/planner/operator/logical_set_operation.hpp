//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_set_operation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class LogicalSetOperation : public LogicalOperator {
	LogicalSetOperation(idx_t table_index, idx_t column_count, LogicalOperatorType type)
	    : LogicalOperator(type), table_index(table_index), column_count(column_count) {
	}

public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_INVALID;

public:
	LogicalSetOperation(idx_t table_index, idx_t column_count, unique_ptr<LogicalOperator> top,
	                    unique_ptr<LogicalOperator> bottom, LogicalOperatorType type)
	    : LogicalOperator(type), table_index(table_index), column_count(column_count) {
		D_ASSERT(type == LogicalOperatorType::LOGICAL_UNION || type == LogicalOperatorType::LOGICAL_EXCEPT ||
		         type == LogicalOperatorType::LOGICAL_INTERSECT);
		children.push_back(std::move(top));
		children.push_back(std::move(bottom));
	}

	idx_t table_index;
	idx_t column_count;

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
