//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/expression/subquery_expression.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

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
	SubqueryExpression()
	    : Expression(ExpressionType::SELECT_SUBQUERY),
	      subquery_type(SubqueryType::DEFAULT) {}

	virtual void Accept(SQLNodeVisitor *v) override { v->Visit(*this); }
	virtual ExpressionClass GetExpressionClass() override {
		return ExpressionClass::SUBQUERY;
	}

	virtual std::unique_ptr<Expression> Copy() override;

	//! Serializes an Expression to a stand-alone binary blob
	virtual void Serialize(Serializer &serializer) override;
	//! Deserializes a blob back into an ConstantExpression
	static std::unique_ptr<Expression>
	Deserialize(ExpressionDeserializeInformation *info, Deserializer &source);

	// FIXME: move these, not related to parser but to execution!
	std::unique_ptr<LogicalOperator> op;
	std::unique_ptr<BindContext> context;
	std::unique_ptr<PhysicalOperator> plan;
	bool is_correlated = false;
	// FIXME

	std::unique_ptr<SelectStatement> subquery;
	SubqueryType subquery_type;

	virtual std::string ToString() const override {
		std::string result = GetExprName();
		if (op) {
			result += "(" + op->ToString() + ")";
		}
		return result;
	}

	virtual bool HasSubquery() override { return true; }

	virtual bool IsScalar() override { return false; }
};
} // namespace duckdb
