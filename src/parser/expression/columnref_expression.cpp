#include "parser/expression/columnref_expression.hpp"

#include "common/exception.hpp"
#include "common/serializer.hpp"
#include "common/types/hash.hpp"

using namespace duckdb;
using namespace std;

//! Specify both the column and table name
ColumnRefExpression::ColumnRefExpression(string column_name, string table_name)
    : ParsedExpression(ExpressionType::COLUMN_REF, ExpressionClass::COLUMN_REF), column_name(column_name),
      table_name(table_name) {
}

ColumnRefExpression::ColumnRefExpression(string column_name) : ColumnRefExpression(column_name, string()) {
}

string ColumnRefExpression::GetName() const {
	return !alias.empty() ? alias : column_name;
}

string ColumnRefExpression::ToString() const {
	if (table_name.empty()) {
		return column_name;
	} else {
		return table_name + "." + column_name;
	}
}

bool ColumnRefExpression::Equals(const BaseExpression *other_) const {
	if (!BaseExpression::Equals(other_)) {
		return false;
	}
	auto other = (ColumnRefExpression *)other_;
	return column_name == other->column_name && table_name == other->table_name;
}

uint64_t ColumnRefExpression::Hash() const {
	uint64_t result = ParsedExpression::Hash();
	result = CombineHash(result, duckdb::Hash<const char *>(column_name.c_str()));
	return result;
}

unique_ptr<ParsedExpression> ColumnRefExpression::Copy() const {
	auto copy = make_unique<ColumnRefExpression>(column_name, table_name);
	copy->CopyProperties(*this);
	return move(copy);
}

void ColumnRefExpression::Serialize(Serializer &serializer) {
	ParsedExpression::Serialize(serializer);
	serializer.WriteString(table_name);
	serializer.WriteString(column_name);
}

unique_ptr<ParsedExpression> ColumnRefExpression::Deserialize(ExpressionType type, Deserializer &source) {
	auto table_name = source.Read<string>();
	auto column_name = source.Read<string>();
	auto expression = make_unique<ColumnRefExpression>(column_name, table_name);
	return move(expression);
}
