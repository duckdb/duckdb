#include "duckdb/planner/bind_context.hpp"

#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/bound_query_node.hpp"

#include "duckdb/common/string_util.hpp"

#include <algorithm>

namespace duckdb {
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
				                      column_name, result, column_name, kv.first, column_name);
			}
			result = kv.first;
		}
	}
	return result;
}

vector<string> BindContext::GetSimilarBindings(const string &column_name) {
	vector<pair<string, idx_t>> scores;
	for (auto &kv : bindings) {
		auto binding = kv.second.get();
		for (auto &name : binding->names) {
			idx_t distance = StringUtil::LevenshteinDistance(name, column_name);
			scores.push_back(make_pair(binding->alias + "." + name, distance));
		}
	}
	return StringUtil::TopNStrings(scores);
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

Binding *BindContext::GetBinding(const string &name, string &out_error) {
	auto match = bindings.find(name);
	if (match == bindings.end()) {
		// alias not found in this BindContext
		vector<string> candidates;
		for (auto &kv : bindings) {
			candidates.push_back(kv.first);
		}
		string candidate_str =
		    StringUtil::CandidatesMessage(StringUtil::TopNLevenshtein(candidates, name), "Candidate tables");
		out_error = StringUtil::Format("Referenced table \"%s\" not found!%s", name, candidate_str);
		return nullptr;
	}
	return match->second.get();
}

BindResult BindContext::BindColumn(ColumnRefExpression &colref, idx_t depth) {
	if (colref.table_name.empty()) {
		return BindResult(StringUtil::Format("Could not bind alias \"%s\"!", colref.column_name));
	}

	string error;
	auto binding = GetBinding(colref.table_name, error);
	if (!binding) {
		return BindResult(error);
	}
	return binding->Bind(binder.context, colref, depth);
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
		string error;
		auto binding = GetBinding(relation_name, error);
		if (!binding) {
			throw BinderException(error);
		}
		binding->GenerateAllColumnExpressions(*this, new_select_list);
	}
}

void BindContext::AddBinding(const string &alias, unique_ptr<Binding> binding) {
	if (bindings.find(alias) != bindings.end()) {
		throw BinderException("Duplicate alias \"%s\" in query!", alias);
	}
	bindings_list.push_back(make_pair(alias, binding.get()));
	bindings[alias] = move(binding);
}

void BindContext::AddBaseTable(idx_t index, const string &alias, vector<string> names, vector<LogicalType> types,
                               LogicalGet &get) {
	AddBinding(alias, make_unique<TableBinding>(alias, move(types), move(names), get, index, true));
}

void BindContext::AddTableFunction(idx_t index, const string &alias, vector<string> names, vector<LogicalType> types,
                                   LogicalGet &get) {
	AddBinding(alias, make_unique<TableBinding>(alias, move(types), move(names), get, index));
}

vector<string> BindContext::AliasColumnNames(string table_name, const vector<string> &names, const vector<string> &column_aliases) {
	vector<string> result;
	if (column_aliases.size() > names.size()) {
		throw BinderException("table \"%s\" has %lld columns available but %lld columns specified", table_name,
		                      names.size(), column_aliases.size());
	}
	// use any provided column aliases first
	for (idx_t i = 0; i < column_aliases.size(); i++) {
		result.push_back(column_aliases[i]);
	}
	// if not enough aliases were provided, use the default names for remaining columns
	for (idx_t i = column_aliases.size(); i < names.size(); i++) {
		result.push_back(names[i]);
	}
	return result;
}

void BindContext::AddSubquery(idx_t index, const string &alias, SubqueryRef &ref, BoundQueryNode &subquery) {
	auto names = AliasColumnNames(alias, subquery.names, ref.column_name_alias);
	AddGenericBinding(index, alias, names, subquery.types);
}

void BindContext::AddGenericBinding(idx_t index, const string &alias, vector<string> names, vector<LogicalType> types) {
	AddBinding(alias, make_unique<Binding>(alias, move(types), move(names), index));
}

void BindContext::AddCTEBinding(idx_t index, const string &alias, vector<string> names, vector<LogicalType> types) {
	auto binding = make_shared<Binding>(alias, move(types), move(names), index);

	if (cte_bindings.find(alias) != cte_bindings.end()) {
		throw BinderException("Duplicate alias \"%s\" in query!", alias);
	}
	cte_bindings[alias] = move(binding);
	cte_references[alias] = std::make_shared<idx_t>(0);
}

} // namespace duckdb
