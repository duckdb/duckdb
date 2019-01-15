//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/expression/bound_subquery_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/expression.hpp"
#include "execution/physical_operator.hpp"
#include "planner/bindcontext.hpp"
#include "planner/logical_operator.hpp"

namespace duckdb {

//! Represents a subquery
class BoundSubqueryExpression : public Expression {
public:
	BoundSubqueryExpression() : Expression(ExpressionType::SELECT_SUBQUERY), subquery_type(SubqueryType::DEFAULT) {
	}

	ExpressionClass GetExpressionClass() override {
		return ExpressionClass::BOUND_SUBQUERY;
	}

	unique_ptr<Expression> Copy() override;

	//! Serializes an Expression to a stand-alone binary blob
	void Serialize(Serializer &serializer) override;

	bool Equals(const Expression *other) const override;

	unique_ptr<QueryNode> subquery;
	unique_ptr<LogicalOperator> op;
	unique_ptr<BindContext> context;
	unique_ptr<PhysicalOperator> plan;
	
	SubqueryType subquery_type;
	bool is_correlated = false;

	string ToString() const override {
		string result = ExpressionTypeToString(type);
		if (op) {
			result += "(" + op->ToString() + ")";
		}
		return result;
	}

	bool HasSubquery() override {
		return true;
	}

	bool IsScalar() override {
		return false;
	}
};
} // namespace duckdb
