//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/expression/constant_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/value.hpp"
#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {

//! ConstantExpression represents a constant value in the query
class ConstantExpression : public ParsedExpression {
public:
	DUCKDB_API explicit ConstantExpression(Value val);

	//! The constant value referenced
	Value value;

public:
	string ToString() const override;

	static bool Equal(const ConstantExpression *a, const ConstantExpression *b);
	hash_t Hash() const override;

	unique_ptr<ParsedExpression> Copy() const override;

	void Serialize(FieldWriter &writer) const override;
	static unique_ptr<ParsedExpression> Deserialize(ExpressionType type, FieldReader &source);
};

} // namespace duckdb
