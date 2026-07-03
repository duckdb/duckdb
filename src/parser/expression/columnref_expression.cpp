#include "duckdb/parser/expression/columnref_expression.hpp"

#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/binding_alias.hpp"
#include "duckdb/parser/keyword_helper.hpp"

namespace duckdb {

ColumnRefExpression::ColumnRefExpression() : ParsedExpression(ExpressionType::COLUMN_REF, ExpressionClass::COLUMN_REF) {
}

ColumnRefExpression::ColumnRefExpression(Identifier column_name, Identifier table_name)
    : ColumnRefExpression(table_name.empty() ? vector<Identifier> {std::move(column_name)}
                                             : vector<Identifier> {std::move(table_name), std::move(column_name)}) {
}

ColumnRefExpression::ColumnRefExpression(Identifier column_name, const BindingAlias &alias)
    : ParsedExpression(ExpressionType::COLUMN_REF, ExpressionClass::COLUMN_REF) {
	if (alias.IsSet()) {
		if (!alias.GetCatalog().empty()) {
			column_names.emplace_back(alias.GetCatalog());
		}
		if (!alias.GetSchema().empty()) {
			column_names.emplace_back(alias.GetSchema());
		}
		column_names.emplace_back(alias.GetAlias());
	}
	column_names.emplace_back(std::move(column_name));
}

ColumnRefExpression::ColumnRefExpression(Identifier column_name)
    : ColumnRefExpression(vector<Identifier> {std::move(column_name)}) {
}

ColumnRefExpression::ColumnRefExpression(vector<Identifier> column_names_p)
    : ParsedExpression(ExpressionType::COLUMN_REF, ExpressionClass::COLUMN_REF) {
	column_names.reserve(column_names_p.size());
	for (auto &col_name : column_names_p) {
		column_names.emplace_back(std::move(col_name));
	}
#ifdef DEBUG
	for (auto &col_name : column_names) {
		D_ASSERT(!col_name.empty());
	}
#endif
}

bool ColumnRefExpression::IsQualified() const {
	return column_names.size() > 1;
}

const Identifier &ColumnRefExpression::GetColumnName() const {
	D_ASSERT(column_names.size() <= 4);
	return column_names.back();
}

const Identifier &ColumnRefExpression::GetTableName() const {
	D_ASSERT(column_names.size() >= 2 && column_names.size() <= 4);
	if (column_names.size() == 4) {
		return column_names[2];
	}
	if (column_names.size() == 3) {
		return column_names[1];
	}
	return column_names[0];
}

Identifier ColumnRefExpression::GetName() const {
	return !alias.empty() ? alias : column_names.back();
}

string ColumnRefExpression::ToString() const {
	string result;
	for (idx_t i = 0; i < column_names.size(); i++) {
		if (i > 0) {
			result += ".";
		}
		result += SQLIdentifier(column_names[i]);
	}
	return result;
}

} // namespace duckdb
