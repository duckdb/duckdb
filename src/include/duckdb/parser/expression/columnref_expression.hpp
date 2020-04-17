//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/expression/columnref_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {

//! Represents a reference to a column from either the FROM clause or from an
//! alias
class ColumnRefExpression : public ParsedExpression {
public:
	//! Specify both the column and table name
	ColumnRefExpression(string column_name, string table_name);
	//! Only specify the column name, the table name will be derived later
	ColumnRefExpression(string column_name);

	//! Column name that is referenced
	string column_name;
	//! Table name of the column name that is referenced (optional)
	string table_name;

public:
	bool IsScalar() const override {
		return false;
	}

	string GetName() const override;
	string ToString() const override;

	static bool Equals(const ColumnRefExpression *a, const ColumnRefExpression *b);
	hash_t Hash() const override;

	unique_ptr<ParsedExpression> Copy() const override;

	void Serialize(Serializer &serializer) override;
	static unique_ptr<ParsedExpression> Deserialize(ExpressionType type, Deserializer &source);
};
} // namespace duckdb
