//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/expression/constant_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/value.hpp"
#include "parser/expression.hpp"
#include "parser/sql_node_visitor.hpp"

namespace duckdb {
//! Represents a constant value in the query
class ConstantExpression : public Expression {
public:
	ConstantExpression() : Expression(ExpressionType::VALUE_CONSTANT, TypeId::INTEGER), value() {
	}
	ConstantExpression(Value val) : Expression(ExpressionType::VALUE_CONSTANT, val.type), value(val) {
	}

	void Accept(SQLNodeVisitor *v) override {
		v->Visit(*this);
	}
	ExpressionClass GetExpressionClass() override {
		return ExpressionClass::CONSTANT;
	}

	unique_ptr<Expression> Copy() override;

	//! Serializes a ConstantExpression to a stand-alone binary blob
	void Serialize(Serializer &serializer) override;
	//! Deserializes a blob back into an ConstantExpression
	static unique_ptr<Expression> Deserialize(ExpressionType type, TypeId return_type, Deserializer &source);

	//! Resolve the type of the constant
	void ResolveType() override;

	uint64_t Hash() const override;
	bool Equals(const Expression *other_) const override;
	string ToString() const override {
		return value.ToString();
	}

	//! The constant value referenced
	Value value;
};
} // namespace duckdb
