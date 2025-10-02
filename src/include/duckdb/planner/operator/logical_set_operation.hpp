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
	LogicalSetOperation(idx_t table_index, idx_t column_count, LogicalOperatorType type, bool setop_all,
	                    bool allow_out_of_order);

public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_INVALID;

public:
	LogicalSetOperation(idx_t table_index, idx_t column_count, unique_ptr<LogicalOperator> top,
	                    unique_ptr<LogicalOperator> bottom, LogicalOperatorType type, bool setop_all,
	                    bool allow_out_of_order = true);
	LogicalSetOperation(idx_t table_index, idx_t column_count, vector<unique_ptr<LogicalOperator>> children,
	                    LogicalOperatorType type, bool setop_all, bool allow_out_of_order = true);

	idx_t table_index;
	idx_t column_count;
	bool setop_all;
	//! Whether or not UNION statements can be executed out of order
	bool allow_out_of_order;

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
