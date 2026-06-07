//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/expression/lambdaref_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/common/optional_ptr.hpp"

namespace duckdb {

struct DummyBinding;

//! Represents a reference to a lambda parameter
class LambdaRefExpression : public ParsedExpression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::LAMBDA_REF;

public:
	//! Constructs a LambdaRefExpression from a lambda_idx and a column_name. We do not specify a table name,
	//! because we use dummy tables to bind lambda parameters
	LambdaRefExpression(idx_t lambda_idx, Identifier column_name_p);

public:
	idx_t LambdaIndex() const {
		return lambda_idx;
	}
	idx_t &LambdaIndexMutable() {
		return lambda_idx;
	}
	const Identifier &ColumnName() const {
		return column_name;
	}
	Identifier &ColumnNameMutable() {
		return column_name;
	}

	bool IsScalar() const override;
	Identifier GetName() const override;
	string ToString() const override;
	bool Equals(const ParsedExpression &other) const override;
	hash_t Hash() const override;
	unique_ptr<ParsedExpression> Copy() const override;

	//! Traverses the lambda_bindings to find a matching binding for the column_name
	static unique_ptr<ParsedExpression> FindMatchingBinding(optional_ptr<vector<DummyBinding>> &lambda_bindings,
	                                                        const Identifier &parameter_name);

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParsedExpression> Deserialize(Deserializer &deserializer);

private:
	//! The index of the lambda parameter in the lambda_bindings vector
	idx_t lambda_idx;
	//! The name of the lambda parameter (in a specific Binding in lambda_bindings)
	Identifier column_name;
};
} // namespace duckdb
