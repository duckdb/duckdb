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
	explicit BoundDefaultExpression(LogicalType type = LogicalType())
	    : Expression(ExpressionType::VALUE_DEFAULT, ExpressionClass::BOUND_DEFAULT, type) {
	}

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
		return make_uniq<BoundDefaultExpression>(return_type);
	}

	void Serialize(FieldWriter &writer) const override;
	static unique_ptr<Expression> Deserialize(ExpressionDeserializationState &state, FieldReader &reader);
};
} // namespace duckdb
