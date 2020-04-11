#include "duckdb/planner/bind_context.hpp"

#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/bound_query_node.hpp"

#include "duckdb/common/string_util.hpp"

#include <algorithm>

using namespace duckdb;
using namespace std;

string BindContext::GetMatchingBinding(const string &column_name) {
	string result;
	for (auto &kv : bindings) {
		auto binding = kv.second.get();
		if (binding->HasMatchingBinding(column_name)) {
			// check if the binding is ignored
			if (BindingIsHidden(kv.first, column_name)) {
				continue;
			}

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

bool BindContext::BindingIsHidden(const string &binding_name, const string &column_name) {
	string total_binding = binding_name + "." + column_name;
	return hidden_columns.find(total_binding) != hidden_columns.end();
}

unordered_set<string> BindContext::GetMatchingBindings(const string &column_name) {
	unordered_set<string> result;
	for (auto &kv : bindings) {
		auto binding = kv.second.get();
		if (binding->HasMatchingBinding(column_name)) {
			result.insert(kv.first);
		}
	}
	return result;
}

Binding *BindContext::GetCTEBinding(const string &ctename) {
	auto match = cte_bindings.find(ctename);
	if (match == cte_bindings.end()) {
		return nullptr;
	}
	return match->second.get();
}

BindResult BindContext::BindColumn(ColumnRefExpression &colref, idx_t depth) {
	if (colref.table_name.empty()) {
		return BindResult(StringUtil::Format("Could not bind alias \"%s\"!", colref.column_name.c_str()));
	}

	auto match = bindings.find(colref.table_name);
	if (match == bindings.end()) {
		// alias not found in this BindContext
		return BindResult(StringUtil::Format("Referenced table \"%s\" not found!", colref.table_name.c_str()));
	}
	auto binding = match->second.get();
	return binding->Bind(colref, depth);
}

void BindContext::GenerateAllColumnExpressions(vector<unique_ptr<ParsedExpression>> &new_select_list,
                                               string relation_name) {
	if (bindings_list.size() == 0) {
		throw BinderException("SELECT * expression without FROM clause!");
	}
	if (relation_name == "") { // SELECT * case
		// we have to bind the tables and subqueries in order of table_index
		for (auto &entry : bindings_list) {
			auto binding = entry.second;
			binding->GenerateAllColumnExpressions(*this, new_select_list);
		}
	} else { // SELECT tbl.* case
		auto match = bindings.find(relation_name);
		if (match == bindings.end()) {
			// alias not found in this BindContext
			throw BinderException("SELECT table.* expression but can't find table");
		}
		auto binding = match->second.get();
		binding->GenerateAllColumnExpressions(*this, new_select_list);
	}
}

void BindContext::AddBinding(const string &alias, unique_ptr<Binding> binding) {
	if (bindings.find(alias) != bindings.end()) {
		throw BinderException("Duplicate alias \"%s\" in query!", alias.c_str());
	}
	bindings_list.push_back(make_pair(alias, binding.get()));
	bindings[alias] = move(binding);
}

void BindContext::AddBaseTable(idx_t index, const string &alias, TableCatalogEntry &table, LogicalGet &get) {
	AddBinding(alias, make_unique<TableBinding>(alias, table, get, index));
}

void BindContext::AddSubquery(idx_t index, const string &alias, SubqueryRef &ref, BoundQueryNode &subquery) {
	vector<string> names;
	if (ref.column_name_alias.size() > subquery.names.size()) {
		throw BinderException("table \"%s\" has %lld columns available but %lld columns specified", alias.c_str(),
		                      (int64_t)subquery.names.size(), (int64_t)ref.column_name_alias.size());
	}
	// use any provided aliases from the subquery
	for (idx_t i = 0; i < ref.column_name_alias.size(); i++) {
		names.push_back(ref.column_name_alias[i]);
	}
	// if not enough aliases were provided, use the default names for remaining columns
	for (idx_t i = ref.column_name_alias.size(); i < subquery.names.size(); i++) {
		names.push_back(subquery.names[i]);
	}
	AddGenericBinding(index, alias, names, subquery.types);
}

void BindContext::AddGenericBinding(idx_t index, const string &alias, vector<string> names, vector<SQLType> types) {
	AddBinding(alias, make_unique<GenericBinding>(alias, move(types), move(names), index));
}

void BindContext::AddCTEBinding(idx_t index, const string &alias, vector<string> names, vector<SQLType> types) {
	auto binding = make_shared<GenericBinding>(alias, move(types), move(names), index);

	if (cte_bindings.find(alias) != cte_bindings.end()) {
		throw BinderException("Duplicate alias \"%s\" in query!", alias.c_str());
	}
	cte_bindings[alias] = move(binding);
	cte_references[alias] = std::make_shared<idx_t>(0);
}
