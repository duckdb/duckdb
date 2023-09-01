//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/expression/lambdaref_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/planner/column_binding.hpp"

namespace duckdb {

//! Represents a reference to a lambda parameter
class LambdaRefExpression : public ParsedExpression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::LAMBDA_REF;

public:
	//! Specify both the column and table name
	LambdaRefExpression(const idx_t lambda_idx, const string &column_name);

	//! The index of the lambda parameter in the lambda bindings vector
	idx_t lambda_index;
	//! ??
	string column_name;

public:
	bool IsQualified() const;
	const string &GetColumnName() const;
	bool IsScalar() const override {
		return false;
	}

	string GetName() const override;
	string ToString() const override;

	static bool Equal(const LambdaRefExpression &a, const LambdaRefExpression &b);
	hash_t Hash() const override;

	unique_ptr<ParsedExpression> Copy() const override;

	void Serialize(FieldWriter &writer) const override;
	static unique_ptr<ParsedExpression> Deserialize(ExpressionType type, FieldReader &source);
	//	void FormatSerialize(FormatSerializer &serializer) const override;
	//	static unique_ptr<ParsedExpression> FormatDeserialize(FormatDeserializer &deserializer);
};
} // namespace duckdb
