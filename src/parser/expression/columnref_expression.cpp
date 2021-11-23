#include "duckdb/parser/expression/columnref_expression.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/serializer.hpp"
#include "duckdb/common/types/hash.hpp"

namespace duckdb {

ColumnRefExpression::ColumnRefExpression(string column_name, string table_name)
    : ColumnRefExpression(table_name.empty() ? vector<string> {move(column_name)}
                                             : vector<string> {move(table_name), move(column_name)}) {
}

ColumnRefExpression::ColumnRefExpression(string column_name) : ColumnRefExpression(vector<string> {move(column_name)}) {
}

ColumnRefExpression::ColumnRefExpression(vector<string> column_names_p)
    : ParsedExpression(ExpressionType::COLUMN_REF, ExpressionClass::COLUMN_REF), column_names(move(column_names_p)) {
#ifdef DEBUG
	for (auto &col_name : column_names) {
		D_ASSERT(!col_name.empty());
	}
#endif
}

bool ColumnRefExpression::IsQualified() const {
	return column_names.size() > 1;
}

const string &ColumnRefExpression::GetColumnName() const {
	D_ASSERT(column_names.size() <= 3);
	return column_names.back();
}

const string &ColumnRefExpression::GetTableName() const {
	D_ASSERT(column_names.size() >= 2 && column_names.size() <= 3);
	return column_names.size() == 3 ? column_names[1] : column_names[0];
}

string ColumnRefExpression::GetName() const {
	return !alias.empty() ? alias : column_names.back();
}

string ColumnRefExpression::ToString() const {
	string result;
	for (idx_t i = 0; i < column_names.size(); i++) {
		if (i > 0) {
			result += ".";
		}
		result += column_names[i];
	}
	return result;
}

bool ColumnRefExpression::Equals(const ColumnRefExpression *a, const ColumnRefExpression *b) {
	return a->column_names == b->column_names;
}

hash_t ColumnRefExpression::Hash() const {
	hash_t result = ParsedExpression::Hash();
	for (auto &column_name : column_names) {
		result = CombineHash(result, duckdb::Hash<const char *>(column_name.c_str()));
	}
	return result;
}

unique_ptr<ParsedExpression> ColumnRefExpression::Copy() const {
	auto copy = make_unique<ColumnRefExpression>(column_names);
	copy->CopyProperties(*this);
	return move(copy);
}

void ColumnRefExpression::Serialize(Serializer &serializer) {
	ParsedExpression::Serialize(serializer);
	serializer.Write<idx_t>(column_names.size());
	for (auto &column_name : column_names) {
		serializer.WriteString(column_name);
	}
}

unique_ptr<ParsedExpression> ColumnRefExpression::Deserialize(ExpressionType type, Deserializer &source) {
	auto column_count = source.Read<idx_t>();
	vector<string> column_names;
	for (idx_t i = 0; i < column_count; i++) {
		column_names.push_back(source.Read<string>());
	}
	auto expression = make_unique<ColumnRefExpression>(move(column_names));
	return move(expression);
}

} // namespace duckdb
