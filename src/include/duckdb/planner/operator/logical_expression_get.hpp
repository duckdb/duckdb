//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_expression_get.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! LogicalExpressionGet represents a scan operation over a set of to-be-executed expressions
class LogicalExpressionGet : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_EXPRESSION_GET;

public:
	LogicalExpressionGet(idx_t table_index, vector<LogicalType> types,
	                     vector<vector<unique_ptr<Expression>>> expressions)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_EXPRESSION_GET), table_index(table_index), expr_types(types),
	      expressions(std::move(expressions)) {
	}

	//! The table index in the current bind context
	idx_t table_index;
	//! The types of the expressions
	vector<LogicalType> expr_types;
	//! The set of expressions
	vector<vector<unique_ptr<Expression>>> expressions;

public:
	vector<ColumnBinding> GetColumnBindings() override {
		return GenerateColumnBindings(table_index, expr_types.size());
	}

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);
	idx_t EstimateCardinality(ClientContext &context) override {
		return expressions.size();
	}
	vector<idx_t> GetTableIndex() const override;
	string GetName() const override;

protected:
	void ResolveTypes() override {
		// types are resolved in the constructor
		this->types = expr_types;
	}
};
} // namespace duckdb
