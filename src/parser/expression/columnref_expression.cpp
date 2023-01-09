#include "duckdb/parser/expression/columnref_expression.hpp"

#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/qualified_name.hpp"

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
	return column_names.size() >= 2 && column_names.size() <= 4;
}

const string &ColumnRefExpression::GetColumnName() const {
	D_ASSERT(column_names.size() <= 4);
	return column_names.back();
}

string &ColumnRefExpression::InnerGetTableNameMutable() {
	D_ASSERT(IsQualified());
	if (column_names.size() == 4) {
		return column_names[2];
	}
	if (column_names.size() == 3) {
		return column_names[1];
	}
	return column_names[0];
}

const string &ColumnRefExpression::GetTableName() const {
	return ((ColumnRefExpression &)*this).InnerGetTableNameMutable();
}

void ColumnRefExpression::SetTableName(string table_name) {
	if (IsQualified()) {
		auto &name = InnerGetTableNameMutable();
		name = move(table_name);
		return;
	}
	column_names.insert(column_names.begin(), move(table_name));
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
		result += KeywordHelper::WriteOptionallyQuoted(column_names[i]);
	}
	return result;
}

bool ColumnRefExpression::Equals(const ColumnRefExpression *a, const ColumnRefExpression *b) {
	if (a->column_names.size() != b->column_names.size()) {
		return false;
	}
	for (idx_t i = 0; i < a->column_names.size(); i++) {
		auto lcase_a = StringUtil::Lower(a->column_names[i]);
		auto lcase_b = StringUtil::Lower(b->column_names[i]);
		if (lcase_a != lcase_b) {
			return false;
		}
	}
	return true;
}

hash_t ColumnRefExpression::Hash() const {
	hash_t result = ParsedExpression::Hash();
	for (auto &column_name : column_names) {
		auto lcase = StringUtil::Lower(column_name);
		result = CombineHash(result, duckdb::Hash<const char *>(lcase.c_str()));
	}
	return result;
}

unique_ptr<ParsedExpression> ColumnRefExpression::Copy() const {
	auto copy = make_unique<ColumnRefExpression>(column_names);
	copy->CopyProperties(*this);
	return move(copy);
}

void ColumnRefExpression::Serialize(FieldWriter &writer) const {
	writer.WriteList<string>(column_names);
}

unique_ptr<ParsedExpression> ColumnRefExpression::Deserialize(ExpressionType type, FieldReader &reader) {
	auto column_names = reader.ReadRequiredList<string>();
	auto expression = make_unique<ColumnRefExpression>(move(column_names));
	return move(expression);
}

} // namespace duckdb
