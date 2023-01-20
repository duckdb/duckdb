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

enum class ColumnQualification : uint8_t { NAME = 0, TABLE = 1, SCHEMA = 2, CATALOG = 3, QUALIFICATION_ENUM_SIZE };

//! Represents a reference to a column from either the FROM clause or from an
//! alias
class ColumnRefExpression : public ParsedExpression {
public:
	//! Specify both the column and table name
	ColumnRefExpression(string column_name, string table_name);
	//! Only specify the column name, the table name will be derived later
	explicit ColumnRefExpression(string column_name);
	//! Specify a set of names
	explicit ColumnRefExpression(vector<string> column_names);

	//! The stack of names in order of which they appear (column_names[0].column_names[1].column_names[2]....)
	//! The name of the column is always the last entry in the list
	vector<string> column_names;

public:
	idx_t GetIndexOfQualification(ColumnQualification qualifier) const;
	const string &GetQualificationName(ColumnQualification qualifier) const;
	bool IsQualified(ColumnQualification qualifier = ColumnQualification::TABLE) const;
	const string &GetColumnName() const;
	const vector<string> &GetColumnNames() const;
	const string &GetTableName() const;
	bool IsScalar() const override {
		return false;
	}

	string GetName() const override;
	string ToString() const override;

	static bool Equal(const ColumnRefExpression *a, const ColumnRefExpression *b);
	hash_t Hash() const override;

	unique_ptr<ParsedExpression> Copy() const override;

	void Serialize(FieldWriter &writer) const override;
	static unique_ptr<ParsedExpression> Deserialize(ExpressionType type, FieldReader &source);

private:
	void VerifyQualification() const;
};
} // namespace duckdb
