#include "duckdb/parser/expression/columnref_expression.hpp"

#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/planner/binding_alias.hpp"

namespace duckdb {

ColumnRefExpression::ColumnRefExpression() : ParsedExpression(ExpressionType::COLUMN_REF, ExpressionClass::COLUMN_REF) {
}

ColumnRefExpression::ColumnRefExpression(string column_name, string table_name)
    : ColumnRefExpression(table_name.empty() ? vector<string> {std::move(column_name)}
                                             : vector<string> {std::move(table_name), std::move(column_name)}) {
}

ColumnRefExpression::ColumnRefExpression(string column_name, const BindingAlias &alias)
    : ParsedExpression(ExpressionType::COLUMN_REF, ExpressionClass::COLUMN_REF) {
	if (alias.IsSet()) {
		if (!alias.GetCatalog().empty()) {
			column_names.push_back(alias.GetCatalog());
		}
		if (!alias.GetSchema().empty()) {
			column_names.push_back(alias.GetSchema());
		}
		column_names.push_back(alias.GetAlias());
	}
	column_names.push_back(std::move(column_name));
}

ColumnRefExpression::ColumnRefExpression(string column_name)
    : ColumnRefExpression(vector<string> {std::move(column_name)}) {
}

ColumnRefExpression::ColumnRefExpression(vector<string> column_names_p)
    : ParsedExpression(ExpressionType::COLUMN_REF, ExpressionClass::COLUMN_REF),
      column_names(std::move(column_names_p)) {
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
	D_ASSERT(column_names.size() <= 4);
	return column_names.back();
}

void ColumnRefExpression::ReplaceOrRemoveTableName(const string &replacement) {
	auto table_name_index = GetTableNameIndex();
	D_ASSERT(table_name_index.IsValid());
	D_ASSERT(column_names.size() > table_name_index.GetIndex());
	if (replacement.empty()) {
		// Remove all the qualifiers leading up to the table name
		auto offset = static_cast<long>(table_name_index.GetIndex()) + 1;
		column_names.erase(column_names.begin(), column_names.begin() + offset);
	} else {
		column_names[table_name_index.GetIndex()] = replacement;
	}
}

optional_idx ColumnRefExpression::GetTableNameIndex() const {
	if (column_names.size() < 2) {
		return optional_idx();
	}
	// FIXME: this entire function is problematic in combination with structs
	// column_names.size() could exceed 4
	D_ASSERT(column_names.size() <= 4);
	if (column_names.size() == 4) {
		// catalog.schema.table.column
		// FIXME: this also matches for: schema.table.struct_column.field1
		return 2;
	}
	if (column_names.size() == 3) {
		// schema.table.column
		// FIXME: this also matches for: table.struct_column.field1
		return 1;
	}
	return 0;
}

const string &ColumnRefExpression::GetTableName() const {
	auto table_name_index = GetTableNameIndex();
	D_ASSERT(table_name_index.IsValid());
	return column_names[table_name_index.GetIndex()];
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

bool ColumnRefExpression::Equal(const ColumnRefExpression &a, const ColumnRefExpression &b) {
	if (a.column_names.size() != b.column_names.size()) {
		return false;
	}
	for (idx_t i = 0; i < a.column_names.size(); i++) {
		if (!StringUtil::CIEquals(a.column_names[i], b.column_names[i])) {
			return false;
		}
	}
	return true;
}

hash_t ColumnRefExpression::Hash() const {
	hash_t result = ParsedExpression::Hash();
	for (auto &column_name : column_names) {
		result = CombineHash(result, StringUtil::CIHash(column_name));
	}
	return result;
}

unique_ptr<ParsedExpression> ColumnRefExpression::Copy() const {
	auto copy = make_uniq<ColumnRefExpression>(column_names);
	copy->CopyProperties(*this);
	return std::move(copy);
}

} // namespace duckdb
