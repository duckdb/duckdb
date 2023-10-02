//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/base_expression.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/parser/expression_util.hpp"

namespace duckdb {
class Deserializer;
class Serializer;

//!  The ParsedExpression class is a base class that can represent any expression
//!  part of a SQL statement.
/*!
 The ParsedExpression class is a base class that can represent any expression
 part of a SQL statement. This is, for example, a column reference in a SELECT
 clause, but also operators, aggregates or filters. The Expression is emitted by the parser and does not contain any
 information about bindings to the catalog or to the types. ParsedExpressions are transformed into regular Expressions
 in the Binder.
 */
class ParsedExpression : public BaseExpression {
public:
	//! Create an Expression
	ParsedExpression(ExpressionType type, ExpressionClass expression_class) : BaseExpression(type, expression_class) {
	}

	//! The location in the query (if any)
	idx_t query_location = DConstants::INVALID_INDEX;

public:
	bool IsAggregate() const override;
	bool IsWindow() const override;
	bool HasSubquery() const override;
	bool IsScalar() const override;
	bool HasParameter() const override;

	bool Equals(const BaseExpression &other) const override;
	hash_t Hash() const override;

	//! Create a copy of this expression
	virtual unique_ptr<ParsedExpression> Copy() const = 0;

	virtual void Serialize(Serializer &serializer) const;
	static unique_ptr<ParsedExpression> Deserialize(Deserializer &deserializer);

	static bool Equals(const unique_ptr<ParsedExpression> &left, const unique_ptr<ParsedExpression> &right);
	static bool ListEquals(const vector<unique_ptr<ParsedExpression>> &left,
	                       const vector<unique_ptr<ParsedExpression>> &right);

protected:
	//! Copy base Expression properties from another expression to this one,
	//! used in Copy method
	void CopyProperties(const ParsedExpression &other) {
		type = other.type;
		expression_class = other.expression_class;
		alias = other.alias;
	}
};

} // namespace duckdb
