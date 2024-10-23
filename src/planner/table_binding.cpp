#include "duckdb/planner/table_binding.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/planner/bind_context.hpp"
#include "duckdb/planner/bound_query_node.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_lambdaref_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"

#include <algorithm>

namespace duckdb {

Binding::Binding(BindingType binding_type, const string &alias, vector<LogicalType> coltypes, vector<string> colnames,
                 idx_t index)
    : binding_type(binding_type), alias(alias), index(index), types(std::move(coltypes)), names(std::move(colnames)) {
	D_ASSERT(types.size() == names.size());
	for (idx_t i = 0; i < names.size(); i++) {
		auto &name = names[i];
		D_ASSERT(!name.empty());
		if (name_map.find(name) != name_map.end()) {
			throw BinderException("table \"%s\" has duplicate column name \"%s\"", alias, name);
		}
		name_map[name] = i;
	}
}

bool Binding::TryGetBindingIndex(const string &column_name, column_t &result) {
	auto entry = name_map.find(column_name);
	if (entry == name_map.end()) {
		return false;
	}
	auto column_info = entry->second;
	result = column_info;
	return true;
}

column_t Binding::GetBindingIndex(const string &column_name) {
	column_t result;
	if (!TryGetBindingIndex(column_name, result)) {
		throw InternalException("Binding index for column \"%s\" not found", column_name);
	}
	return result;
}

bool Binding::HasMatchingBinding(const string &column_name) {
	column_t result;
	return TryGetBindingIndex(column_name, result);
}

ErrorData Binding::ColumnNotFoundError(const string &column_name) const {
	return ErrorData(ExceptionType::BINDER,
	                 StringUtil::Format("Values list \"%s\" does not have a column named \"%s\"", alias, column_name));
}

BindResult Binding::Bind(ColumnRefExpression &colref, idx_t depth) {
	column_t column_index;
	bool success = false;
	success = TryGetBindingIndex(colref.GetColumnName(), column_index);
	if (!success) {
		return BindResult(ColumnNotFoundError(colref.GetColumnName()));
	}
	ColumnBinding binding;
	binding.table_index = index;
	binding.column_index = column_index;
	LogicalType sql_type = types[column_index];
	if (colref.alias.empty()) {
		colref.alias = names[column_index];
	}
	return BindResult(make_uniq<BoundColumnRefExpression>(colref.GetName(), sql_type, binding, depth));
}

optional_ptr<StandardEntry> Binding::GetStandardEntry() {
	return nullptr;
}

EntryBinding::EntryBinding(const string &alias, vector<LogicalType> types_p, vector<string> names_p, idx_t index,
                           StandardEntry &entry)
    : Binding(BindingType::CATALOG_ENTRY, alias, std::move(types_p), std::move(names_p), index), entry(entry) {
}

optional_ptr<StandardEntry> EntryBinding::GetStandardEntry() {
	return &entry;
}

TableBinding::TableBinding(const string &alias, vector<LogicalType> types_p, vector<string> names_p,
                           vector<column_t> &bound_column_ids, optional_ptr<StandardEntry> entry, idx_t index,
                           bool add_row_id)
    : Binding(BindingType::TABLE, alias, std::move(types_p), std::move(names_p), index),
      bound_column_ids(bound_column_ids), entry(entry) {
	if (add_row_id) {
		if (name_map.find("rowid") == name_map.end()) {
			name_map["rowid"] = COLUMN_IDENTIFIER_ROW_ID;
		}
	}
}

static void ReplaceAliases(ParsedExpression &expr, const ColumnList &list,
                           const unordered_map<idx_t, string> &alias_map) {
	if (expr.type == ExpressionType::COLUMN_REF) {
		auto &colref = expr.Cast<ColumnRefExpression>();
		D_ASSERT(!colref.IsQualified());
		auto &col_names = colref.column_names;
		D_ASSERT(col_names.size() == 1);
		auto idx_entry = list.GetColumnIndex(col_names[0]);
		auto &alias = alias_map.at(idx_entry.index);
		col_names = {alias};
	}
	ParsedExpressionIterator::EnumerateChildren(
	    expr, [&](const ParsedExpression &child) { ReplaceAliases((ParsedExpression &)child, list, alias_map); });
}

static void BakeTableName(ParsedExpression &expr, const string &table_name) {
	if (expr.type == ExpressionType::COLUMN_REF) {
		auto &colref = expr.Cast<ColumnRefExpression>();
		D_ASSERT(!colref.IsQualified());
		auto &col_names = colref.column_names;
		col_names.insert(col_names.begin(), table_name);
	}
	ParsedExpressionIterator::EnumerateChildren(
	    expr, [&](const ParsedExpression &child) { BakeTableName((ParsedExpression &)child, table_name); });
}

unique_ptr<ParsedExpression> TableBinding::ExpandGeneratedColumn(const string &column_name) {
	auto catalog_entry = GetStandardEntry();
	D_ASSERT(catalog_entry); // Should only be called on a TableBinding

	D_ASSERT(catalog_entry->type == CatalogType::TABLE_ENTRY);
	auto &table_entry = catalog_entry->Cast<TableCatalogEntry>();

	// Get the index of the generated column
	auto column_index = GetBindingIndex(column_name);
	D_ASSERT(table_entry.GetColumn(LogicalIndex(column_index)).Generated());
	// Get a copy of the generated column
	auto expression = table_entry.GetColumn(LogicalIndex(column_index)).GeneratedExpression().Copy();
	unordered_map<idx_t, string> alias_map;
	for (auto &entry : name_map) {
		alias_map[entry.second] = entry.first;
	}
	ReplaceAliases(*expression, table_entry.GetColumns(), alias_map);
	BakeTableName(*expression, alias);
	return (expression);
}

const vector<column_t> &TableBinding::GetBoundColumnIds() const {
#ifdef DEBUG
	unordered_set<column_t> column_ids;
	for (auto &id : bound_column_ids) {
		auto result = column_ids.insert(id);
		// assert that all entries in the bound_column_ids are unique
		D_ASSERT(result.second);
		auto it = std::find_if(name_map.begin(), name_map.end(),
		                       [&](const std::pair<const string, column_t> &it) { return it.second == id; });
		// assert that every id appears in the name_map
		D_ASSERT(it != name_map.end());
		// the order that they appear in is not guaranteed to be sequential
	}
#endif
	return bound_column_ids;
}

ColumnBinding TableBinding::GetColumnBinding(column_t column_index) {
	auto &column_ids = bound_column_ids;
	ColumnBinding binding;

	// Locate the column_id that matches the 'column_index'
	auto it = std::find_if(column_ids.begin(), column_ids.end(),
	                       [&](const column_t &id) -> bool { return id == column_index; });
	// Get the index of it
	binding.column_index = NumericCast<idx_t>(std::distance(column_ids.begin(), it));
	// If it wasn't found, add it
	if (it == column_ids.end()) {
		column_ids.push_back(column_index);
	}

	binding.table_index = index;
	return binding;
}

BindResult TableBinding::Bind(ColumnRefExpression &colref, idx_t depth) {
	auto &column_name = colref.GetColumnName();
	column_t column_index;
	bool success = false;
	success = TryGetBindingIndex(column_name, column_index);
	if (!success) {
		return BindResult(ColumnNotFoundError(column_name));
	}
	auto entry = GetStandardEntry();
	if (entry && column_index != COLUMN_IDENTIFIER_ROW_ID) {
		D_ASSERT(entry->type == CatalogType::TABLE_ENTRY);
		// Either there is no table, or the columns category has to be standard
		auto &table_entry = entry->Cast<TableCatalogEntry>();
		auto &column_entry = table_entry.GetColumn(LogicalIndex(column_index));
		(void)table_entry;
		(void)column_entry;
		D_ASSERT(column_entry.Category() == TableColumnType::STANDARD);
	}
	// fetch the type of the column
	LogicalType col_type;
	if (column_index == COLUMN_IDENTIFIER_ROW_ID) {
		col_type = LogicalType::ROW_TYPE;
	} else {
		// normal column: fetch type from base column
		col_type = types[column_index];
		if (colref.alias.empty()) {
			colref.alias = names[column_index];
		}
	}
	ColumnBinding binding = GetColumnBinding(column_index);
	return BindResult(make_uniq<BoundColumnRefExpression>(colref.GetName(), col_type, binding, depth));
}

optional_ptr<StandardEntry> TableBinding::GetStandardEntry() {
	return entry;
}

ErrorData TableBinding::ColumnNotFoundError(const string &column_name) const {
	return ErrorData(ExceptionType::BINDER,
	                 StringUtil::Format("Table \"%s\" does not have a column named \"%s\"", alias, column_name));
}

DummyBinding::DummyBinding(vector<LogicalType> types, vector<string> names, string dummy_name)
    : Binding(BindingType::DUMMY, DummyBinding::DUMMY_NAME + dummy_name, std::move(types), std::move(names),
              DConstants::INVALID_INDEX),
      dummy_name(std::move(dummy_name)) {
}

BindResult DummyBinding::Bind(ColumnRefExpression &colref, idx_t depth) {
	column_t column_index;
	if (!TryGetBindingIndex(colref.GetColumnName(), column_index)) {
		throw InternalException("Column %s not found in bindings", colref.GetColumnName());
	}
	ColumnBinding binding(index, column_index);

	// we are binding a parameter to create the dummy binding, no arguments are supplied
	return BindResult(make_uniq<BoundColumnRefExpression>(colref.GetName(), types[column_index], binding, depth));
}

BindResult DummyBinding::Bind(LambdaRefExpression &lambdaref, idx_t depth) {
	column_t column_index;
	if (!TryGetBindingIndex(lambdaref.GetName(), column_index)) {
		throw InternalException("Column %s not found in bindings", lambdaref.GetName());
	}
	ColumnBinding binding(index, column_index);
	return BindResult(make_uniq<BoundLambdaRefExpression>(lambdaref.GetName(), types[column_index], binding,
	                                                      lambdaref.lambda_idx, depth));
}

unique_ptr<ParsedExpression> DummyBinding::ParamToArg(ColumnRefExpression &colref) {
	column_t column_index;
	if (!TryGetBindingIndex(colref.GetColumnName(), column_index)) {
		throw InternalException("Column %s not found in macro", colref.GetColumnName());
	}
	auto arg = (*arguments)[column_index]->Copy();
	arg->alias = colref.alias;
	return arg;
}

} // namespace duckdb
