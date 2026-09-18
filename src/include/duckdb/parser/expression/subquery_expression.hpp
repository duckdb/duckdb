//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/expression/subquery_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/subquery_type.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/statement/select_statement.hpp"

namespace duckdb {

//! Represents a subquery
class SubqueryExpression : public ParsedExpression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::SUBQUERY;

public:
	SubqueryExpression();

public:
	const unique_ptr<SelectStatement> &Subquery() const {
		return subquery;
	}
	unique_ptr<SelectStatement> &SubqueryMutable() {
		return subquery;
	}
	SubqueryType GetSubqueryType() const {
		return subquery_type;
	}
	SubqueryType &GetSubqueryTypeMutable() {
		return subquery_type;
	}
	const unique_ptr<ParsedExpression> &GetChild() const {
		return child;
	}
	unique_ptr<ParsedExpression> &GetChildMutable() {
		return child;
	}
	ExpressionType GetComparisonType() const {
		return comparison_type;
	}
	ExpressionType &GetComparisonTypeMutable() {
		return comparison_type;
	}

	bool HasSubquery() const override {
		return true;
	}
	bool IsScalar() const override {
		return false;
	}

	string ToString() const override;

	bool Equals(const ParsedExpression &other) const override;
	hash_t Hash() const override;

	unique_ptr<ParsedExpression> Copy() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParsedExpression> Deserialize(Deserializer &deserializer);

private:
	//! The actual subquery
	unique_ptr<SelectStatement> subquery;
	//! The subquery type
	SubqueryType subquery_type;
	//! the child expression to compare with (in case of IN, ANY, ALL operators, empty for EXISTS queries and scalar
	//! subquery)
	unique_ptr<ParsedExpression> child;
	//! The comparison type of the child expression with the subquery (in case of ANY, ALL operators), empty otherwise
	ExpressionType comparison_type;
};
} // namespace duckdb
