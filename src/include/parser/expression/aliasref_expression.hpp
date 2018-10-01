//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/expression/aliasref_expression.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/expression.hpp"

namespace duckdb {

//! Represents a reference to a column from either the FROM clause or from an
//! alias
class AliasRefExpression : public Expression {
  public:
	AliasRefExpression(TypeId type, size_t index)
	    : Expression(ExpressionType::ALIAS_REF, type), index(index),
	      reference(nullptr) {}

	virtual void Accept(SQLNodeVisitor *v) override { v->Visit(*this); }

	virtual void ResolveType() override {
		Expression::ResolveType();
		if (return_type == TypeId::INVALID) {
			throw Exception("Type of AliasRefExpression was not resolved!");
		}
	}

	virtual bool Equals(const Expression *other_) override {
		if (!Expression::Equals(other_)) {
			return false;
		}
		auto other = reinterpret_cast<const AliasRefExpression *>(other_);
		if (!other) {
			return false;
		}
		return reference == other->reference;
	}

	//! Index used for reference resolving
	size_t index = (size_t)-1;
	//! A reference to the Expression this references
	Expression *reference;

	virtual std::string ToString() const override {
		if (index != (size_t)-1) {
			return "#" + std::to_string(index);
		}
		auto str = table_name.empty() ? std::to_string(binding.table_index)
		                              : table_name;
		str += ".";
		str += column_name.empty() ? std::to_string(binding.column_index)
		                           : column_name;
		return str;
	}

	virtual bool IsScalar() override { return false; }
};
} // namespace duckdb
