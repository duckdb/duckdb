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
	                    bool allow_out_of_order)
	    : LogicalOperator(type), table_index(table_index), column_count(column_count), setop_all(setop_all),
	      allow_out_of_order(allow_out_of_order) {
	}

public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_INVALID;

public:
	LogicalSetOperation(idx_t table_index, idx_t column_count, unique_ptr<LogicalOperator> top,
	                    unique_ptr<LogicalOperator> bottom, LogicalOperatorType type, bool setop_all,
	                    bool allow_out_of_order = true)
	    : LogicalOperator(type), table_index(table_index), column_count(column_count), setop_all(setop_all),
	      allow_out_of_order(allow_out_of_order) {
		D_ASSERT(type == LogicalOperatorType::LOGICAL_UNION || type == LogicalOperatorType::LOGICAL_EXCEPT ||
		         type == LogicalOperatorType::LOGICAL_INTERSECT);
		children.push_back(std::move(top));
		children.push_back(std::move(bottom));
	}

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
