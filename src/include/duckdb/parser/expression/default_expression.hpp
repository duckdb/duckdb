//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/expression/default_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"

namespace duckdb {
class Deserializer;
class Serializer;

//! Represents the default value of a column
class DefaultExpression : public ParsedExpression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::DEFAULT;

public:
	DefaultExpression();

public:
	bool IsScalar() const override {
		return false;
	}

	string ToString() const override;

	unique_ptr<ParsedExpression> Copy() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParsedExpression> Deserialize(Deserializer &deserializer);
};
} // namespace duckdb
