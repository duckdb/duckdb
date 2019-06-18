#include "planner/bind_context.hpp"

#include "parser/expression/columnref_expression.hpp"
#include "parser/tableref/subqueryref.hpp"
#include "planner/expression/bound_columnref_expression.hpp"
#include "planner/tableref/bound_basetableref.hpp"
#include "storage/column_statistics.hpp"

#include <algorithm>

using namespace duckdb;
using namespace std;

string BindContext::GetMatchingBinding(const string &column_name) {
	string result;
	for (auto &kv : bindings) {
		auto binding = kv.second.get();
		if (binding->HasMatchingBinding(column_name)) {
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

BindResult BindContext::BindColumn(ColumnRefExpression &colref, index_t depth) {
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

void BindContext::GenerateAllColumnExpressions(vector<unique_ptr<ParsedExpression>> &new_select_list) {
	if (bindings_list.size() == 0) {
		throw BinderException("SELECT * expression without FROM clause!");
	}

	// we have to bind the tables and subqueries in order of table_index
	for (auto &entry : bindings_list) {
		auto binding = entry.second;
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

void BindContext::AddBaseTable(BoundBaseTableRef *bound, const string &alias) {
	AddBinding(alias, make_unique<TableBinding>(alias, bound));
}

void BindContext::AddSubquery(index_t index, const string &alias, SubqueryRef &ref, BoundQueryNode &subquery) {
	AddBinding(alias, make_unique<SubqueryBinding>(alias, ref, subquery, index));
}

void BindContext::AddTableFunction(index_t index, const string &alias, TableFunctionCatalogEntry *function_entry) {
	AddBinding(alias, make_unique<TableFunctionBinding>(alias, function_entry, index));
}
