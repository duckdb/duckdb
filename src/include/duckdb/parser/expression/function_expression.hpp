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

namespace duckdb {
//! Represents a function call
class FunctionExpression : public ParsedExpression {
public:
	FunctionExpression(string schema_name, const string &function_name, vector<unique_ptr<ParsedExpression>> children,
	                   unique_ptr<ParsedExpression> filter = nullptr, bool distinct = false, bool is_operator = false);
	FunctionExpression(const string &function_name, vector<unique_ptr<ParsedExpression>> children,
	                   unique_ptr<ParsedExpression> filter = nullptr, bool distinct = false, bool is_operator = false);

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

public:
	string ToString() const override;

	unique_ptr<ParsedExpression> Copy() const override;

	static bool Equals(const FunctionExpression *a, const FunctionExpression *b);
	hash_t Hash() const override;

	//! Serializes a FunctionExpression to a stand-alone binary blob
	void Serialize(Serializer &serializer) override;
	//! Deserializes a blob back into an FunctionExpression
	static unique_ptr<ParsedExpression> Deserialize(ExpressionType type, Deserializer &source);
};
} // namespace duckdb
