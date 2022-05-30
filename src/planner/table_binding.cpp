#include "duckdb/planner/table_binding.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/planner/bind_context.hpp"
#include "duckdb/planner/bound_query_node.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"

namespace duckdb {

Binding::Binding(const string &alias, vector<LogicalType> coltypes, vector<string> colnames,
                 vector<TableColumnType> categories, idx_t index)
    : alias(alias), index(index), types(move(coltypes)), names(move(colnames)), categories(move(categories)) {
	D_ASSERT(types.size() == names.size());
	for (idx_t i = 0; i < names.size(); i++) {
		auto &name = names[i];
		D_ASSERT(!name.empty());
		if (name_map.find(name) != name_map.end()) {
			throw BinderException("table \"%s\" has duplicate column name \"%s\"", alias, name);
		}
		auto category = i < this->categories.size() ? this->categories[i] : TableColumnType::STANDARD;
		auto column_info = TableColumnInfo(i, category);
		name_map[name] = column_info;
	}
}

bool Binding::TryGetBindingIndex(const string &column_name, TableColumnInfo &result) {
	auto entry = name_map.find(column_name);
	if (entry == name_map.end()) {
		return false;
	}
	auto column_info = entry->second;
	result = column_info;
	return true;
}

TableColumnInfo Binding::GetBindingInfo(const string &column_name) {
	TableColumnInfo result;
	if (!TryGetBindingIndex(column_name, result)) {
		throw InternalException("Binding index for column \"%s\" not found", column_name);
	}
	return result;
}

bool Binding::HasMatchingBinding(const string &column_name) {
	TableColumnInfo result;
	return TryGetBindingIndex(column_name, result);
}

string Binding::ColumnNotFoundError(const string &column_name) const {
	return StringUtil::Format("Values list \"%s\" does not have a column named \"%s\"", alias, column_name);
}

BindResult Binding::Bind(ColumnRefExpression &colref, idx_t depth) {
	TableColumnInfo column_info;
	bool success = false;
	success = TryGetBindingIndex(colref.GetColumnName(), column_info);
	if (!success) {
		return BindResult(ColumnNotFoundError(colref.GetColumnName()));
	}
	auto column_index = column_info.index;
	ColumnBinding binding;
	binding.table_index = index;
	binding.column_index = column_index;
	LogicalType sql_type = types[column_index];
	if (colref.alias.empty()) {
		colref.alias = names[column_index];
	}
	return BindResult(make_unique<BoundColumnRefExpression>(colref.GetName(), sql_type, binding, depth));
}

TableCatalogEntry *Binding::GetTableEntry() {
	return nullptr;
}

TableBinding::TableBinding(const string &alias, vector<LogicalType> types_p, vector<string> names_p,
                           vector<TableColumnType> categories_p, LogicalGet &get, idx_t index, bool add_row_id)
    : Binding(alias, move(types_p), move(names_p), move(categories_p), index), get(get) {
	if (add_row_id) {
		if (name_map.find("rowid") == name_map.end()) {
			auto column_info = TableColumnInfo(COLUMN_IDENTIFIER_ROW_ID);
			name_map["rowid"] = column_info;
		}
	}
}

static void BakeTableName(ParsedExpression &expr, const string &table_name) {
	if (expr.type == ExpressionType::COLUMN_REF) {
		auto &colref = (ColumnRefExpression &)expr;
		D_ASSERT(!colref.IsQualified());
		auto &col_names = colref.column_names;
		col_names.insert(col_names.begin(), table_name);
	}
	ParsedExpressionIterator::EnumerateChildren(
	    expr, [&](const ParsedExpression &child) { BakeTableName((ParsedExpression &)child, table_name); });
}

unique_ptr<ParsedExpression> TableBinding::ExpandGeneratedColumn(const string &column_name) {
	auto table_catalog_entry = GetTableEntry();
	D_ASSERT(table_catalog_entry); // Should only be called on a TableBinding

	// Get the index of the generated column
	auto column_info = GetBindingInfo(column_name);
	D_ASSERT(column_info.column_type == TableColumnType::GENERATED);
	// Get a copy of the generated column
	auto expression = table_catalog_entry->columns[column_info.index].GeneratedExpression().Copy();
	BakeTableName(*expression, alias);
	return (expression);
}

BindResult TableBinding::Bind(ColumnRefExpression &colref, idx_t depth) {
	auto &column_name = colref.GetColumnName();
	TableColumnInfo column_info;
	bool success = false;
	success = TryGetBindingIndex(column_name, column_info);
	if (!success) {
		return BindResult(ColumnNotFoundError(column_name));
	}
	D_ASSERT(column_info.column_type == TableColumnType::STANDARD);
	auto column_index = column_info.index;
	// fetch the type of the column
	LogicalType col_type;
	if (column_index == COLUMN_IDENTIFIER_ROW_ID) {
		// row id: BIGINT type
		col_type = LogicalType::BIGINT;
	} else {
		// normal column: fetch type from base column
		col_type = types[column_index];
		if (colref.alias.empty()) {
			colref.alias = names[column_index];
		}
	}

	auto &column_ids = get.column_ids;
	// check if the entry already exists in the column list for the table
	ColumnBinding binding;

	binding.column_index = column_ids.size();
	for (idx_t i = 0; i < column_ids.size(); i++) {
		if (column_ids[i] == column_index) {
			binding.column_index = i;
			break;
		}
	}
	if (binding.column_index == column_ids.size()) {
		// column binding not found: add it to the list of bindings
		column_ids.push_back(column_index);
	}
	binding.table_index = index;
	return BindResult(make_unique<BoundColumnRefExpression>(colref.GetName(), col_type, binding, depth));
}

TableCatalogEntry *TableBinding::GetTableEntry() {
	return get.GetTable();
}

string TableBinding::ColumnNotFoundError(const string &column_name) const {
	return StringUtil::Format("Table \"%s\" does not have a column named \"%s\"", alias, column_name);
}

MacroBinding::MacroBinding(vector<LogicalType> types_p, vector<string> names_p, string macro_name_p)
    : Binding(MacroBinding::MACRO_NAME, move(types_p), move(names_p), vector<TableColumnType>(), -1),
      macro_name(move(macro_name_p)) {
}

BindResult MacroBinding::Bind(ColumnRefExpression &colref, idx_t depth) {
	TableColumnInfo column_info;
	if (!TryGetBindingIndex(colref.GetColumnName(), column_info)) {
		throw InternalException("Column %s not found in macro", colref.GetColumnName());
	}
	D_ASSERT(column_info.column_type == TableColumnType::STANDARD);
	auto column_index = column_info.index;
	ColumnBinding binding;
	binding.table_index = index;
	binding.column_index = column_index;

	// we are binding a parameter to create the macro, no arguments are supplied
	return BindResult(make_unique<BoundColumnRefExpression>(colref.GetName(), types[column_index], binding, depth));
}

unique_ptr<ParsedExpression> MacroBinding::ParamToArg(ColumnRefExpression &colref) {
	TableColumnInfo column_info;
	if (!TryGetBindingIndex(colref.GetColumnName(), column_info)) {
		throw InternalException("Column %s not found in macro", colref.GetColumnName());
	}
	D_ASSERT(column_info.column_type == TableColumnType::STANDARD);
	auto arg = arguments[column_info.index]->Copy();
	arg->alias = colref.alias;
	return arg;
}

} // namespace duckdb
