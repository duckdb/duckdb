//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/expression/lambdaref_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {

//! Represents a reference to a lambda parameter
class LambdaRefExpression : public ParsedExpression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::LAMBDA_REF;

public:
	//! Constructs a LambdaRefExpression from a lambda_idx and a column_name. We do not specify a table name,
	//! because we use dummy tables to bind lambda parameters
	LambdaRefExpression(idx_t lambda_idx, string column_name_p);

	//! The index of the lambda parameter in the lambda_bindings vector
	idx_t lambda_idx;
	//! The name of the lambda parameter (in a specific Binding in lambda_bindings)
	string column_name;

public:
	bool IsScalar() const override;
	string GetName() const override;
	string ToString() const override;
	hash_t Hash() const override;
	unique_ptr<ParsedExpression> Copy() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParsedExpression> Deserialize(Deserializer &deserializer);
};
} // namespace duckdb
