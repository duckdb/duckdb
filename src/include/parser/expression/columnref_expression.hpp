//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/expression/columnref_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/expression.hpp"

namespace duckdb {

//! Represents a reference to a column from either the FROM clause or from an
//! alias
class ColumnRefExpression : public Expression {
public:
	//! Only specify the column name, the table name will be derived later
	ColumnRefExpression(string column_name) : Expression(ExpressionType::COLUMN_REF), column_name(column_name) {
	}

	//! Specify both the column and table name
	ColumnRefExpression(string column_name, string table_name)
	    : Expression(ExpressionType::COLUMN_REF), column_name(column_name), table_name(table_name) {
	}

	const string &GetColumnName() const {
		return column_name;
	}
	const string &GetTableName() const {
		return table_name;
	}

	string GetName() const override {
		return !alias.empty() ? alias : column_name;
	}
	ExpressionClass GetExpressionClass() override {
		return ExpressionClass::COLUMN_REF;
	}

	unique_ptr<Expression> Copy() const override;

	//! Serializes an Expression to a stand-alone binary blob
	void Serialize(Serializer &serializer) override;
	//! Deserializes a blob back into an ConstantExpression
	static unique_ptr<Expression> Deserialize(ExpressionType type, TypeId return_type, Deserializer &source);

	void ResolveType() override;

	uint64_t Hash() const override;
	bool Equals(const Expression *other) const override;

	//! Column name that is referenced
	string column_name;
	//! Table name of the column name that is referenced (optional)
	string table_name;

	string ToString() const override;

	bool IsScalar() override {
		return false;
	}
};
} // namespace duckdb
