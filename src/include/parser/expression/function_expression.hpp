//===----------------------------------------------------------------------===// 
// 
//                         DuckDB 
// 
// parser/expression/function_expression.hpp
// 
// 
// 
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/expression.hpp"
#include "parser/sql_node_visitor.hpp"

namespace duckdb {
class ScalarFunctionCatalogEntry;

//! Represents a function call
class FunctionExpression : public Expression {
  public:
	FunctionExpression(std::string schema_name, std::string function_name,
	                   std::vector<std::unique_ptr<Expression>> &children);
	FunctionExpression(std::string function_name,
	                   std::vector<std::unique_ptr<Expression>> &children)
	    : FunctionExpression(DEFAULT_SCHEMA, function_name, children) {
	}

	void ResolveType() override;

	std::unique_ptr<Expression> Accept(SQLNodeVisitor *v) override {
		return v->Visit(*this);
	}
	ExpressionClass GetExpressionClass() override {
		return ExpressionClass::FUNCTION;
	}

	std::unique_ptr<Expression> Copy() override;

	//! Serializes an Expression to a stand-alone binary blob
	void Serialize(Serializer &serializer) override;
	//! Deserializes a blob back into an ConstantExpression
	static std::unique_ptr<Expression>
	Deserialize(ExpressionDeserializeInfo *info, Deserializer &source);

	//! Schema of the function
	std::string schema;
	//! Function name
	std::string function_name;

	// FIXME: remove this
	ScalarFunctionCatalogEntry *bound_function;
};
} // namespace duckdb
