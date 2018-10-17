
#include "planner/bindcontext.hpp"

#include "parser/expression/columnref_expression.hpp"

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

string BindContext::GetMatchingBinding(const string &column_name) {
	string result;
	for (auto &kv : bindings) {
		auto binding = kv.second.get();
		if (HasMatchingBinding(binding, column_name)) {
			if (!result.empty()) {
				throw BinderException(
				    "Ambiguous reference to column name\"%s\" (use: \"%s.%s\" "
				    "or \"%s.%s\")",
				    column_name.c_str(), result.c_str(), column_name.c_str(),
				    kv.first.c_str(), column_name.c_str());
			}
			result = kv.first;
		}
	}

	if (result.empty() &&
	    expression_alias_map.find(column_name) != expression_alias_map.end()) {
		return result; // empty result, will be bound to alias
	}

	if (result.empty()) {
		if (parent) {
			return parent->GetMatchingBinding(column_name);
		}
		throw BinderException(
		    "Referenced column \"%s\" not found in FROM clause!",
		    column_name.c_str());
	}
	return result;
}

void BindContext::BindColumn(ColumnRefExpression &expr, size_t depth) {
	if (expr.table_name.empty()) {
		// empty table name, bind
		auto entry = expression_alias_map.find(expr.column_name);
		if (entry == expression_alias_map.end()) {
			throw BinderException("Could not bind alias \"%s\"!",
			                      expr.column_name.c_str());
		}
		expr.index = entry->second.first;
		expr.reference = entry->second.second;
		expr.return_type = entry->second.second->return_type;
		return;
	}

	auto match = bindings.find(expr.table_name);
	if (match == bindings.end()) {
		// alias not found in this BindContext
		// if there is a parent BindContext look there
		if (parent) {
			parent->BindColumn(expr, ++depth);
			max_depth = max(expr.depth, max_depth);
			return;
		}
		throw BinderException("Referenced table \"%s\" not found!",
		                      expr.table_name.c_str());
	}
	auto binding = match->second.get();
	switch (binding->type) {
	case BindingType::DUMMY: {
		auto dummy = (DummyTableBinding *)binding;
		auto entry = dummy->bound_columns.find(expr.column_name);
		if (entry == dummy->bound_columns.end()) {
			throw BinderException("Referenced column \"%s\" not found table!",
			                      expr.column_name.c_str());
		}
		expr.index = entry->second->oid;
		expr.return_type = entry->second->type;
		break;
	}
	case BindingType::TABLE: {
		// base table
		auto &table = *((TableBinding *)binding);
		if (!table.table->ColumnExists(expr.column_name)) {
			throw BinderException(
			    "Table \"%s\" does not have a column named \"%s\"",
			    expr.table_name.c_str(), expr.column_name.c_str());
		}
		auto table_index = table.index;
		auto entry = &table.table->GetColumn(expr.column_name);
		expr.stats = table.table->GetStatistics(entry->oid);
		auto &column_list = bound_columns[expr.table_name];
		// check if the entry already exists in the column list for the table
		expr.binding.column_index = column_list.size();
		for (size_t i = 0; i < column_list.size(); i++) {
			auto &column = column_list[i];
			if (column == expr.column_name) {
				expr.binding.column_index = i;
				break;
			}
		}
		if (expr.binding.column_index == column_list.size()) {
			// column binding not found: add it to the list of bindings
			column_list.push_back(expr.column_name);
		}
		expr.binding.table_index = table_index;
		expr.depth = depth;
		expr.return_type = entry->type;
		break;
	}
	case BindingType::SUBQUERY: {
		// subquery
		auto &subquery = *((SubqueryBinding *)binding);
		auto column_entry = subquery.name_map.find(expr.column_name);
		if (column_entry == subquery.name_map.end()) {
			throw BinderException(
			    "Subquery \"%s\" does not have a column named \"%s\"",
			    match->first.c_str(), expr.column_name.c_str());
		}

		expr.binding.table_index = subquery.index;
		expr.binding.column_index = column_entry->second;
		expr.depth = depth;
		assert(column_entry->second < subquery.subquery->select_list.size());
		expr.return_type =
		    subquery.subquery->select_list[column_entry->second]->return_type;
		break;
	}
	case BindingType::TABLE_FUNCTION: {
		auto &table_function = *((TableFunctionBinding *)binding);
		auto column_entry =
		    table_function.function->name_map.find(expr.column_name);
		if (column_entry == table_function.function->name_map.end()) {
			throw BinderException(
			    "Table Function \"%s\" does not have a column named \"%s\"",
			    match->first.c_str(), expr.column_name.c_str());
		}

		expr.binding.table_index = table_function.index;
		expr.binding.column_index = column_entry->second;
		expr.depth = depth;
		expr.return_type =
		    table_function.function->return_values[column_entry->second].type;
		break;
	}
	}
}

void BindContext::GenerateAllColumnExpressions(
    vector<unique_ptr<Expression>> &new_select_list) {
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
				new_select_list.push_back(
				    make_unique<ColumnRefExpression>(column.name, name));
			}
			break;
		}
		case BindingType::SUBQUERY: {
			auto &subquery = *((SubqueryBinding *)binding);
			for (auto &column_name : subquery.names) {
				new_select_list.push_back(
				    make_unique<ColumnRefExpression>(column_name, name));
			}
			break;
		}
		case BindingType::TABLE_FUNCTION: {
			auto &table_function = *((TableFunctionBinding *)binding);
			for (auto &column : table_function.function->return_values) {
				new_select_list.push_back(
				    make_unique<ColumnRefExpression>(column.name, name));
			}
			break;
		}
		}
	}
}

void BindContext::AddBinding(const std::string &alias,
                             unique_ptr<Binding> binding) {
	if (HasAlias(alias)) {
		throw BinderException("Duplicate alias \"%s\" in query!",
		                      alias.c_str());
	}
	bindings_list.push_back(make_pair(alias, binding.get()));
	bindings[alias] = move(binding);
}
void BindContext::AddBaseTable(const string &alias,
                               TableCatalogEntry *table_entry) {
	AddBinding(alias,
	           make_unique<TableBinding>(table_entry, GenerateTableIndex()));
}

void BindContext::AddDummyTable(const std::string &alias,
                                std::vector<ColumnDefinition> &columns) {
	// alias is empty for dummy table
	// initialize the OIDs of the column definitions
	for (size_t i = 0; i < columns.size(); i++) {
		columns[i].oid = i;
	}
	AddBinding(alias, make_unique<DummyTableBinding>(columns));
}

void BindContext::AddSubquery(const string &alias, SelectStatement *subquery) {
	AddBinding(alias,
	           make_unique<SubqueryBinding>(subquery, GenerateTableIndex()));
}

void BindContext::AddTableFunction(const std::string &alias,
                                   TableFunctionCatalogEntry *function_entry) {
	AddBinding(alias, make_unique<TableFunctionBinding>(function_entry,
	                                                    GenerateTableIndex()));
}

void BindContext::AddExpression(const string &alias, Expression *expression,
                                size_t i) {
	expression_alias_map[alias] = make_pair(i, expression);
}

bool BindContext::HasAlias(const string &alias) {
	return bindings.find(alias) != bindings.end();
}

size_t BindContext::GetBindingIndex(const std::string &alias) {
	auto entry = bindings.find(alias);
	if (entry == bindings.end()) {
		throw Exception("Could not find alias!");
	}
	return entry->second->index;
}

size_t BindContext::GenerateTableIndex() {
	if (parent) {
		return parent->GenerateTableIndex();
	}
	return bound_tables++;
}
