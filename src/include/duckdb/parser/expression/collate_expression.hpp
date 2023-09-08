//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/expression/collate_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {

//! CollateExpression represents a COLLATE statement
class CollateExpression : public ParsedExpression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::COLLATE;

public:
	CollateExpression(string collation, unique_ptr<ParsedExpression> child);

	//! The child of the cast expression
	unique_ptr<ParsedExpression> child;
	//! The collation clause
	string collation;

public:
	string ToString() const override;

	static bool Equal(const CollateExpression &a, const CollateExpression &b);

	unique_ptr<ParsedExpression> Copy() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParsedExpression> Deserialize(Deserializer &deserializer);

private:
	CollateExpression();
};
} // namespace duckdb
