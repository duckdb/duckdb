//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/expression/parameter_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/value.hpp"
#include "parser/expression.hpp"

namespace duckdb {
class ParameterExpression : public Expression {
public:
	ParameterExpression()
	    : Expression(ExpressionType::VALUE_PARAMETER, TypeId::INVALID), parameter_nr(0), value(Value()) {
	}

	ExpressionClass GetExpressionClass() override {
		return ExpressionClass::PARAMETER;
	}

	unique_ptr<Expression> Copy() override;

	//! Serializes a ConstantExpression to a stand-alone binary blob
	void Serialize(Serializer &serializer) override;
	//! Deserializes a blob back into an ConstantExpression
	static unique_ptr<Expression> Deserialize(ExpressionType type, TypeId return_type, Deserializer &source);

	//! Resolve the type of the constant
	void ResolveType() override;

	uint64_t Hash() const override;

	virtual bool IsScalar() override {
		return true;
	}

	virtual bool HasParameter() override {
		return true;
	}

	bool Equals(const Expression *other_) const override;
	string ToString() const override {
		return "?";
	}

	size_t parameter_nr;
	Value value;
};
} // namespace duckdb
