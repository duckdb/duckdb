//===----------------------------------------------------------------------===// 
// 
//                         DuckDB 
// 
// parser/expression/subquery_expression.hpp
// 
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
	SubqueryExpression()
	    : Expression(ExpressionType::SELECT_SUBQUERY),
	      subquery_type(SubqueryType::DEFAULT) {
	}

	std::unique_ptr<Expression> Accept(SQLNodeVisitor *v) override {
		return v->Visit(*this);
	}
	ExpressionClass GetExpressionClass() override {
		return ExpressionClass::SUBQUERY;
	}

	std::unique_ptr<Expression> Copy() override;

	//! Serializes an Expression to a stand-alone binary blob
	void Serialize(Serializer &serializer) override;
	//! Deserializes a blob back into an ConstantExpression
	static std::unique_ptr<Expression>
	Deserialize(ExpressionDeserializeInformation *info, Deserializer &source);

	bool Equals(const Expression *other) override;

	// FIXME: move these, not related to parser but to execution!
	std::unique_ptr<LogicalOperator> op;
	std::unique_ptr<BindContext> context;
	std::unique_ptr<PhysicalOperator> plan;
	bool is_correlated = false;
	// FIXME

	std::unique_ptr<SelectStatement> subquery;
	SubqueryType subquery_type;

	std::string ToString() const override {
		std::string result = ExpressionTypeToString(type);
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
