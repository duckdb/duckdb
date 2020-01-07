//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression/bound_default_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/expression.hpp"

namespace duckdb {

class BoundDefaultExpression : public Expression {
public:
	BoundDefaultExpression(TypeId type = TypeId::INVALID, SQLType sql_type = SQLType())
	    : Expression(ExpressionType::VALUE_DEFAULT, ExpressionClass::BOUND_DEFAULT, type), sql_type(sql_type) {
	}

	SQLType sql_type;

public:
	bool IsScalar() const override {
		return false;
	}
	bool IsFoldable() const override {
		return false;
	}

	string ToString() const override {
		return "DEFAULT";
	}

	unique_ptr<Expression> Copy() override {
		return make_unique<BoundDefaultExpression>(return_type, sql_type);
	}
};
} // namespace duckdb
