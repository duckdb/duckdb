#include "planner/bind_context.hpp"

#include "planner/expression/bound_columnref_expression.hpp"
#include "planner/expression/bound_reference_expression.hpp"
#include "parser/expression/columnref_expression.hpp"
#include "parser/tableref/subqueryref.hpp"
#include "storage/column_statistics.hpp"
#include "planner/tableref/bound_basetableref.hpp"

#include <algorithm>

using namespace duckdb;
using namespace std;

static bool HasMatchingBinding(Binding *binding, const string &column_name) {
	switch (binding->type) {
	case BindingType::DUMMY:
		// always bind to dummy table
		return true;
	case BindingType::TABLE: {
		auto table_binding = (TableBinding *)binding;
		auto table = table_binding->table;
		return table->ColumnExists(column_name);
	}
	case BindingType::SUBQUERY: {
		auto subquery = (SubqueryBinding *)binding;
		auto entry = subquery->name_map.find(column_name);
		return entry != subquery->name_map.end();
	}
	case BindingType::TABLE_FUNCTION: {
		auto function_binding = (TableFunctionBinding *)binding;
		auto function = function_binding->function;
		return function->ColumnExists(column_name);
	}
	}
	return false;
}

BindContext::BindContext() {
}

string BindContext::GetMatchingBinding(const string &column_name) {
	string result;
	for (auto &kv : bindings) {
		auto binding = kv.second.get();
		if (HasMatchingBinding(binding, column_name)) {
			if (!result.empty()) {
				throw BinderException("Ambiguous reference to column name \"%s\" (use: \"%s.%s\" "
				                      "or \"%s.%s\")",
				                      column_name.c_str(), result.c_str(), column_name.c_str(), kv.first.c_str(),
				                      column_name.c_str());
			}
			result = kv.first;
		}
	}
	return result;
}

BindResult BindContext::BindColumn(unique_ptr<Expression> expr, uint32_t depth) {
	assert(expr->GetExpressionClass() == ExpressionClass::COLUMN_REF);
	auto &colref = (ColumnRefExpression &)*expr;

	if (colref.table_name.empty()) {
		return BindResult(move(expr), StringUtil::Format("Could not bind alias \"%s\"!", colref.column_name.c_str()));
	}

	auto match = bindings.find(colref.table_name);
	if (match == bindings.end()) {
		// alias not found in this BindContext
		return BindResult(move(expr),
		                  StringUtil::Format("Referenced table \"%s\" not found!", colref.table_name.c_str()));
	}
	auto binding = match->second.get();
	switch (binding->type) {
	case BindingType::DUMMY: {
		auto dummy = (DummyTableBinding *)binding;
		auto entry = dummy->bound_columns.find(colref.column_name);
		if (entry == dummy->bound_columns.end()) {
			return BindResult(move(expr), StringUtil::Format("Referenced column \"%s\" not found table!",
			                                                 colref.column_name.c_str()));
		}
		return BindResult(make_unique<BoundExpression>(colref.GetName(), entry->second->type, entry->second->oid));
	}
	case BindingType::TABLE: {
		// base table
		auto &table = *((TableBinding *)binding);
		if (!table.table->ColumnExists(colref.column_name)) {
			return BindResult(move(expr), StringUtil::Format("Table \"%s\" does not have a column named \"%s\"",
			                                                 colref.table_name.c_str(), colref.column_name.c_str()));
		}
		auto table_index = table.index;
		auto entry = &table.table->GetColumn(colref.column_name);
		auto &column_list = bound_columns[colref.table_name];
		// check if the entry already exists in the column list for the table
		ColumnBinding binding;
		binding.column_index = column_list.size();
		for (size_t i = 0; i < column_list.size(); i++) {
			auto &column = column_list[i];
			if (column == colref.column_name) {
				binding.column_index = i;
				break;
			}
		}
		if (binding.column_index == column_list.size()) {
			// column binding not found: add it to the list of bindings
			column_list.push_back(colref.column_name);
		}
		binding.table_index = table_index;
		auto result = make_unique<BoundColumnRefExpression>(colref, entry->type, binding, depth);
		// assign the base table statistics
		auto &table_stats = table.table->GetStatistics(entry->oid);
		table_stats.Initialize(result->stats);
		if (bindings.size() > 1) {
			// OUTER JOINS can introduce NULLs in the column
			result->stats.can_have_null = true;
		}
		return BindResult(move(result));
	}
	case BindingType::SUBQUERY: {
		// subquery
		auto &subquery = *((SubqueryBinding *)binding);
		auto column_entry = subquery.name_map.find(colref.column_name);
		if (column_entry == subquery.name_map.end()) {
			return BindResult(move(expr), StringUtil::Format("Subquery \"%s\" does not have a column named \"%s\"",
			                                                 match->first.c_str(), colref.column_name.c_str()));
		}
		ColumnBinding binding;
		binding.table_index = subquery.index;
		binding.column_index = column_entry->second;
		auto &select_list = subquery.subquery->GetSelectList();
		assert(column_entry->second < select_list.size());
		TypeId return_type = select_list[column_entry->second]->return_type;
		return BindResult(make_unique<BoundColumnRefExpression>(colref, return_type, binding, depth));
	}
	default: {
		assert(binding->type == BindingType::TABLE_FUNCTION);
		auto &table_function = *((TableFunctionBinding *)binding);
		auto column_entry = table_function.function->name_map.find(colref.column_name);
		if (column_entry == table_function.function->name_map.end()) {
			return BindResult(move(expr),
			                  StringUtil::Format("Table Function \"%s\" does not have a column named \"%s\"",
			                                     match->first.c_str(), colref.column_name.c_str()));
		}
		ColumnBinding binding;
		binding.table_index = table_function.index;
		binding.column_index = column_entry->second;
		TypeId return_type = table_function.function->return_values[column_entry->second].type;
		return BindResult(make_unique<BoundColumnRefExpression>(colref, return_type, binding, depth));
	}
	}
}

void BindContext::GenerateAllColumnExpressions(vector<unique_ptr<ParsedExpression>> &new_select_list) {
	if (bindings_list.size() == 0) {
		throw BinderException("SELECT * expression without FROM clause!");
	}

	// we have to bind the tables and subqueries in order of table_index
	for (auto &entry : bindings_list) {
		auto &name = entry.first;
		auto binding = entry.second;
		switch (binding->type) {
		case BindingType::DUMMY: {
			throw BinderException("Cannot use * with DUMMY table");
		}
		case BindingType::TABLE: {
			auto &table = *((TableBinding *)binding);
			for (auto &column : table.table->columns) {
				new_select_list.push_back(make_unique<ColumnRefExpression>(column.name, name));
			}
			break;
		}
		case BindingType::SUBQUERY: {
			auto &subquery = *((SubqueryBinding *)binding);
			for (auto &column_name : subquery.names) {
				new_select_list.push_back(make_unique<ColumnRefExpression>(column_name, name));
			}
			break;
		}
		case BindingType::TABLE_FUNCTION: {
			auto &table_function = *((TableFunctionBinding *)binding);
			for (auto &column : table_function.function->return_values) {
				new_select_list.push_back(make_unique<ColumnRefExpression>(column.name, name));
			}
			break;
		}
		}
	}
}

void BindContext::AddBinding(const string &alias, unique_ptr<Binding> binding) {
	if (HasAlias(alias)) {
		throw BinderException("Duplicate alias \"%s\" in query!", alias.c_str());
	}
	bindings_list.push_back(make_pair(alias, binding.get()));
	bindings[alias] = move(binding);
}

void BindContext::AddBaseTable(BoundBaseTableRef *bound, const string &alias) {
	AddBinding(alias, make_unique<TableBinding>(bound));
}

void BindContext::AddSubquery(size_t index, const string &alias, SubqueryRef &subquery) {
	AddBinding(alias, make_unique<SubqueryBinding>(subquery, index));
}

void BindContext::AddTableFunction(size_t index, const string &alias, TableFunctionCatalogEntry *function_entry) {
	AddBinding(alias, make_unique<TableFunctionBinding>(function_entry, index));
}

void BindContext::AddDummyTable(const string &alias, vector<ColumnDefinition> &columns) {
	// alias is empty for dummy table
	// initialize the OIDs of the column definitions
	for (size_t i = 0; i < columns.size(); i++) {
		columns[i].oid = i;
	}
	AddBinding(alias, make_unique<DummyTableBinding>(columns));
}

bool BindContext::HasAlias(const string &alias) {
	return bindings.find(alias) != bindings.end();
}

size_t BindContext::GetBindingIndex(const string &alias) {
	auto entry = bindings.find(alias);
	if (entry == bindings.end()) {
		throw BinderException("Could not find alias!");
	}
	return entry->second->index;
}
