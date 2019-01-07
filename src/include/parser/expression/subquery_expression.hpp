//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/expression/subquery_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/sql_node_visitor.hpp"
#include "parser/statement/select_statement.hpp"
#include "parser/tableref.hpp"

// FIXME: should not include this here!
#include "execution/physical_operator.hpp"
#include "planner/bindcontext.hpp"
#include "planner/logical_operator.hpp"

namespace duckdb {

//! Represents a subquery
class SubqueryExpression : public Expression {
public:
	SubqueryExpression() : Expression(ExpressionType::SELECT_SUBQUERY), subquery_type(SubqueryType::DEFAULT) {
	}

	unique_ptr<Expression> Accept(SQLNodeVisitor *v) override {
		return v->Visit(*this);
	}
	ExpressionClass GetExpressionClass() override {
		return ExpressionClass::SUBQUERY;
	}

	unique_ptr<Expression> Copy() override;

	void EnumerateChildren(std::function<unique_ptr<Expression>(unique_ptr<Expression> expression)> callback) override {
	}
	void EnumerateChildren(std::function<void(Expression *expression)> callback) const override {
	}
	//! Serializes an Expression to a stand-alone binary blob
	void Serialize(Serializer &serializer) override;
	//! Deserializes a blob back into an ConstantExpression
	static unique_ptr<Expression> Deserialize(ExpressionType type, TypeId return_type, Deserializer &source);

	bool Equals(const Expression *other) const override;

	// FIXME: move these, not related to parser but to execution!
	unique_ptr<LogicalOperator> op;
	unique_ptr<BindContext> context;
	unique_ptr<PhysicalOperator> plan;
	bool is_correlated = false;
	// FIXME

	unique_ptr<QueryNode> subquery;
	SubqueryType subquery_type;

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
