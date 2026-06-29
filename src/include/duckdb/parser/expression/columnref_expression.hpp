//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/expression/columnref_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {
struct BindingAlias;

//! Represents a reference to a column from either the FROM clause or from an
//! alias
class ColumnRefExpression : public ParsedExpression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::COLUMN_REF;

public:
	//! Specify both the column and table name
	ColumnRefExpression(Identifier column_name, Identifier table_name);
	//! Specify both the column and table alias
	ColumnRefExpression(Identifier column_name, const BindingAlias &alias);
	//! Only specify the column name, the table name will be derived later
	explicit ColumnRefExpression(Identifier column_name);
	//! Specify a set of names
	explicit ColumnRefExpression(vector<Identifier> column_names);

public:
	const vector<Identifier> &ColumnNames() const {
		return column_names;
	}
	vector<Identifier> &ColumnNamesMutable() {
		return column_names;
	}

	bool IsQualified() const;
	const Identifier &GetColumnName() const;
	const Identifier &GetTableName() const;
	bool IsScalar() const override {
		return false;
	}

	Identifier GetName() const override;
	string ToString() const override;

	bool Equals(const ParsedExpression &other) const override;
	hash_t Hash() const override;

	unique_ptr<ParsedExpression> Copy() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParsedExpression> Deserialize(Deserializer &deserializer);

private:
	//! The stack of names in order of which they appear (column_names[0].column_names[1].column_names[2]....)
	vector<Identifier> column_names;

private:
	ColumnRefExpression();
};
} // namespace duckdb
