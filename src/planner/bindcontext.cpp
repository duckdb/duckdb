#include "planner/bindcontext.hpp"

#include "parser/expression/bound_columnref_expression.hpp"
#include "parser/expression/bound_expression.hpp"
#include "parser/expression/columnref_expression.hpp"
#include "parser/tableref/subqueryref.hpp"
#include "storage/column_statistics.hpp"

using namespace duckdb;
using namespace std;

SubqueryBinding::SubqueryBinding(SubqueryRef &subquery_, size_t index)
    : Binding(BindingType::SUBQUERY, index), subquery(subquery_.subquery.get()) {
	auto &select_list = subquery->GetSelectList();
	if (subquery_.column_name_alias.size() > 0) {
		assert(subquery_.column_name_alias.size() == select_list.size());
		for (auto &name : subquery_.column_name_alias) {
			name_map[name] = names.size();
			names.push_back(name);
		}
	} else {
		for (auto &entry : select_list) {
			auto name = entry->GetName();
			name_map[name] = names.size();
			names.push_back(name);
		}
	}
}

SubqueryBinding::SubqueryBinding(QueryNode *select_, size_t index)
    : Binding(BindingType::SUBQUERY, index), subquery(select_) {
	// FIXME: double-check this
	auto &select_list = subquery->GetSelectList();
	for (auto &entry : select_list) {
		auto name = entry->GetName();
		name_map[name] = names.size();
		names.push_back(name);
	}
}

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

	if (result.empty() && expression_alias_map.find(column_name) != expression_alias_map.end()) {
		return result; // empty result, will be bound to alias
	}

	if (result.empty()) {
		if (parent) {
			return parent->GetMatchingBinding(column_name);
		}
		throw BinderException("Referenced column \"%s\" not found in FROM clause!", column_name.c_str());
	}
	return result;
}

unique_ptr<Expression> BindContext::BindColumn(ColumnRefExpression &expr, size_t depth) {
	if (expr.table_name.empty()) {
		// empty table name, bind to expression alias
		auto entry = expression_alias_map.find(expr.column_name);
		if (entry == expression_alias_map.end()) {
			throw BinderException("Could not bind alias \"%s\"!", expr.column_name.c_str());
		}
		return make_unique<BoundExpression>(expr.GetName(), entry->second.second->return_type, entry->second.first);
	}

	auto match = bindings.find(expr.table_name);
	if (match == bindings.end()) {
		// alias not found in this BindContext
		// if there is a parent BindContext look there
		if (parent) {
			auto expression = parent->BindColumn(expr, ++depth);
			size_t expr_depth = 0;
			if (expression->type == ExpressionType::BOUND_COLUMN_REF) {
				expr_depth = ((BoundColumnRefExpression &)*expression).depth;
			} else {
				assert(expression->type == ExpressionType::BOUND_REF);
				expr_depth = ((BoundExpression &)*expression).depth;
			}
			max_depth = max(expr_depth, max_depth);
			return expression;
		}
		throw BinderException("Referenced table \"%s\" not found!", expr.table_name.c_str());
	}
	auto binding = match->second.get();
	switch (binding->type) {
	case BindingType::DUMMY: {
		auto dummy = (DummyTableBinding *)binding;
		auto entry = dummy->bound_columns.find(expr.column_name);
		if (entry == dummy->bound_columns.end()) {
			throw BinderException("Referenced column \"%s\" not found table!", expr.column_name.c_str());
		}
		return make_unique<BoundExpression>(expr.GetName(), entry->second->type, entry->second->oid);
	}
	case BindingType::TABLE: {
		// base table
		auto &table = *((TableBinding *)binding);
		if (!table.table->ColumnExists(expr.column_name)) {
			throw BinderException("Table \"%s\" does not have a column named \"%s\"", expr.table_name.c_str(),
			                      expr.column_name.c_str());
		}
		auto table_index = table.index;
		auto entry = &table.table->GetColumn(expr.column_name);
		auto &column_list = bound_columns[expr.table_name];
		// check if the entry already exists in the column list for the table
		ColumnBinding binding;
		binding.column_index = column_list.size();
		for (size_t i = 0; i < column_list.size(); i++) {
			auto &column = column_list[i];
			if (column == expr.column_name) {
				binding.column_index = i;
				break;
			}
		}
		if (binding.column_index == column_list.size()) {
			// column binding not found: add it to the list of bindings
			column_list.push_back(expr.column_name);
		}
		binding.table_index = table_index;
		auto result = make_unique<BoundColumnRefExpression>(expr, entry->type, binding, depth);
		// assign the base table statistics
		auto &table_stats = table.table->GetStatistics(entry->oid);
		table_stats.Initialize(result->stats);
		if (bindings.size() > 1) {
			// OUTER JOINS can introduce NULLs in the column
			result->stats.can_have_null = true;
		}
		return result;
	}
	case BindingType::SUBQUERY: {
		// subquery
		auto &subquery = *((SubqueryBinding *)binding);
		auto column_entry = subquery.name_map.find(expr.column_name);
		if (column_entry == subquery.name_map.end()) {
			throw BinderException("Subquery \"%s\" does not have a column named \"%s\"", match->first.c_str(),
			                      expr.column_name.c_str());
		}
		ColumnBinding binding;
		binding.table_index = subquery.index;
		binding.column_index = column_entry->second;
		auto &select_list = subquery.subquery->GetSelectList();
		assert(column_entry->second < select_list.size());
		TypeId return_type = select_list[column_entry->second]->return_type;
		return make_unique<BoundColumnRefExpression>(expr, return_type, binding, depth);
	}
	default: {
		assert(binding->type == BindingType::TABLE_FUNCTION);
		auto &table_function = *((TableFunctionBinding *)binding);
		auto column_entry = table_function.function->name_map.find(expr.column_name);
		if (column_entry == table_function.function->name_map.end()) {
			throw BinderException("Table Function \"%s\" does not have a column named \"%s\"", match->first.c_str(),
			                      expr.column_name.c_str());
		}
		ColumnBinding binding;
		binding.table_index = table_function.index;
		binding.column_index = column_entry->second;
		TypeId return_type = table_function.function->return_values[column_entry->second].type;
		return make_unique<BoundColumnRefExpression>(expr, return_type, binding, depth);
	}
	}
}

void BindContext::GenerateAllColumnExpressions(vector<unique_ptr<Expression>> &new_select_list) {
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
size_t BindContext::AddBaseTable(const string &alias, TableCatalogEntry *table_entry) {
	size_t index = GenerateTableIndex();
	AddBinding(alias, make_unique<TableBinding>(table_entry, index));
	return index;
}

void BindContext::AddDummyTable(const string &alias, vector<ColumnDefinition> &columns) {
	// alias is empty for dummy table
	// initialize the OIDs of the column definitions
	for (size_t i = 0; i < columns.size(); i++) {
		columns[i].oid = i;
	}
	AddBinding(alias, make_unique<DummyTableBinding>(columns));
}

size_t BindContext::AddSubquery(const string &alias, SubqueryRef &subquery) {
	size_t index = GenerateTableIndex();
	AddBinding(alias, make_unique<SubqueryBinding>(subquery, index));
	return index;
}

size_t BindContext::AddTableFunction(const string &alias, TableFunctionCatalogEntry *function_entry) {
	size_t index = GenerateTableIndex();
	AddBinding(alias, make_unique<TableFunctionBinding>(function_entry, index));
	return index;
}

void BindContext::AddExpression(const string &alias, Expression *expression, size_t i) {
	expression_alias_map[alias] = make_pair(i, expression);
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

size_t BindContext::GenerateTableIndex() {
	if (parent) {
		return parent->GenerateTableIndex();
	}
	return bound_tables++;
}
