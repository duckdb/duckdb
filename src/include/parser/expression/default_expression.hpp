//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/expression/default_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_expression.hpp"

namespace duckdb {
//! Represents the default value of a column
class DefaultExpression : public ParsedExpression {
public:
	DefaultExpression();
public:
	bool IsScalar() override {
		return false;
	}

	string ToString() const override;

	unique_ptr<ParsedExpression> Copy() override;
	
	static unique_ptr<ParsedExpression> Deserialize(ExpressionType type, Deserializer &source);

};
} // namespace duckdb
