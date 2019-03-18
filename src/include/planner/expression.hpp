//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_expression.hpp"

namespace duckdb {
//!  The Expression class represents a bound ParsedExpression with a return type
class Expression : public ParsedExpression {
public:
	Expression(ExpressionType type, ExpressionClass expression_class, TypeId return_type);

	TypeId return_type;
public:
	void Serialize(Serializer &serializer) override;
protected:
	//! Copy base Expression properties from another expression to this one,
	//! used in Copy method
	void CopyProperties(Expression &other) {
		type = other.type;
		alias = other.alias;
		return_type = other.return_type;
	}
};

} // namespace duckdb
