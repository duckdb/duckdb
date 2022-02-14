//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/expression/function_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/vector.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/result_modifier.hpp"

namespace duckdb {
//! Represents a function call
class FunctionExpression : public ParsedExpression {
public:
	FunctionExpression(string schema_name, const string &function_name, vector<unique_ptr<ParsedExpression>> children,
	                   unique_ptr<ParsedExpression> filter = nullptr, unique_ptr<OrderModifier> order_bys = nullptr,
	                   bool distinct = false, bool is_operator = false, bool export_state = false);
	FunctionExpression(const string &function_name, vector<unique_ptr<ParsedExpression>> children,
	                   unique_ptr<ParsedExpression> filter = nullptr, unique_ptr<OrderModifier> order_bys = nullptr,
	                   bool distinct = false, bool is_operator = false, bool export_state = false);

	//! Schema of the function
	string schema;
	//! Function name
	string function_name;
	//! Whether or not the function is an operator, only used for rendering
	bool is_operator;
	//! List of arguments to the function
	vector<unique_ptr<ParsedExpression>> children;
	//! Whether or not the aggregate function is distinct, only used for aggregates
	bool distinct;
	//! Expression representing a filter, only used for aggregates
	unique_ptr<ParsedExpression> filter;
	//! Modifier representing an ORDER BY, only used for aggregates
	unique_ptr<OrderModifier> order_bys;
	//! whether this function should export its state or not
	bool export_state;

public:
	string ToString() const override;

	unique_ptr<ParsedExpression> Copy() const override;

	static bool Equals(const FunctionExpression *a, const FunctionExpression *b);
	hash_t Hash() const override;

	void Serialize(FieldWriter &writer) const override;
	static unique_ptr<ParsedExpression> Deserialize(ExpressionType type, FieldReader &source);
};
} // namespace duckdb
