#include "duckdb/planner/bind_context.hpp"

#include "duckdb/catalog/catalog_entry/table_column_type.hpp"
#include "duckdb/catalog/standard_entry.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/expression/positional_reference_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/bound_query_node.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/planner/expression_binder/constant_binder.hpp"

#include <algorithm>

namespace duckdb {

string BindContext::GetMatchingBinding(const string &column_name) {
	string result;
	for (auto &kv : bindings) {
		auto binding = kv.second.get();
		auto is_using_binding = GetUsingBinding(column_name, kv.first);
		if (is_using_binding) {
			continue;
		}
		if (binding->HasMatchingBinding(column_name)) {
			if (!result.empty() || is_using_binding) {
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
			idx_t distance = StringUtil::SimilarityScore(name, column_name);
			scores.emplace_back(binding->alias + "." + name, distance);
		}
	}
	return StringUtil::TopNStrings(scores);
}

void BindContext::AddUsingBinding(const string &column_name, UsingColumnSet &set) {
	using_columns[column_name].insert(set);
}

void BindContext::AddUsingBindingSet(unique_ptr<UsingColumnSet> set) {
	using_column_sets.push_back(std::move(set));
}

optional_ptr<UsingColumnSet> BindContext::GetUsingBinding(const string &column_name) {
	auto entry = using_columns.find(column_name);
	if (entry == using_columns.end()) {
		return nullptr;
	}
	auto &using_bindings = entry->second;
	if (using_bindings.size() > 1) {
		string error = "Ambiguous column reference: column \"" + column_name + "\" can refer to either:\n";
		for (auto &using_set_ref : using_bindings) {
			auto &using_set = using_set_ref.get();
			string result_bindings;
			for (auto &binding : using_set.bindings) {
				if (result_bindings.empty()) {
					result_bindings = "[";
				} else {
					result_bindings += ", ";
				}
				result_bindings += binding;
				result_bindings += ".";
				result_bindings += GetActualColumnName(binding, column_name);
			}
			error += result_bindings + "]";
		}
		throw BinderException(error);
	}
	for (auto &using_set : using_bindings) {
		return &using_set.get();
	}
	throw InternalException("Using binding found but no entries");
}

optional_ptr<UsingColumnSet> BindContext::GetUsingBinding(const string &column_name, const string &binding_name) {
	if (binding_name.empty()) {
		throw InternalException("GetUsingBinding: expected non-empty binding_name");
	}
	auto entry = using_columns.find(column_name);
	if (entry == using_columns.end()) {
		return nullptr;
	}
	auto &using_bindings = entry->second;
	for (auto &using_set_ref : using_bindings) {
		auto &using_set = using_set_ref.get();
		auto &bindings = using_set.bindings;
		if (bindings.find(binding_name) != bindings.end()) {
			return &using_set;
		}
	}
	return nullptr;
}

void BindContext::RemoveUsingBinding(const string &column_name, UsingColumnSet &set) {
	auto entry = using_columns.find(column_name);
	if (entry == using_columns.end()) {
		throw InternalException("Attempting to remove using binding that is not there");
	}
	auto &bindings = entry->second;
	if (bindings.find(set) != bindings.end()) {
		bindings.erase(set);
	}
	if (bindings.empty()) {
		using_columns.erase(column_name);
	}
}

void BindContext::TransferUsingBinding(BindContext &current_context, optional_ptr<UsingColumnSet> current_set,
                                       UsingColumnSet &new_set, const string &binding, const string &using_column) {
	AddUsingBinding(using_column, new_set);
	if (current_set) {
		current_context.RemoveUsingBinding(using_column, *current_set);
	}
}

string BindContext::GetActualColumnName(const string &binding_name, const string &column_name) {
	string error;
	auto binding = GetBinding(binding_name, error);
	if (!binding) {
		throw InternalException("No binding with name \"%s\"", binding_name);
	}
	column_t binding_index;
	if (!binding->TryGetBindingIndex(column_name, binding_index)) { // LCOV_EXCL_START
		throw InternalException("Binding with name \"%s\" does not have a column named \"%s\"", binding_name,
		                        column_name);
	} // LCOV_EXCL_STOP
	return binding->names[binding_index];
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

unique_ptr<ParsedExpression> BindContext::ExpandGeneratedColumn(const string &table_name, const string &column_name) {
	string error_message;

	auto binding = GetBinding(table_name, error_message);
	D_ASSERT(binding);
	auto &table_binding = binding->Cast<TableBinding>();
	auto result = table_binding.ExpandGeneratedColumn(column_name);
	result->alias = column_name;
	return result;
}

unique_ptr<ParsedExpression> BindContext::CreateColumnReference(const string &table_name, const string &column_name) {
	string schema_name;
	return CreateColumnReference(schema_name, table_name, column_name);
}

static bool ColumnIsGenerated(Binding &binding, column_t index) {
	if (binding.binding_type != BindingType::TABLE) {
		return false;
	}
	auto &table_binding = binding.Cast<TableBinding>();
	auto catalog_entry = table_binding.GetStandardEntry();
	if (!catalog_entry) {
		return false;
	}
	if (index == COLUMN_IDENTIFIER_ROW_ID) {
		return false;
	}
	D_ASSERT(catalog_entry->type == CatalogType::TABLE_ENTRY);
	auto &table_entry = catalog_entry->Cast<TableCatalogEntry>();
	return table_entry.GetColumn(LogicalIndex(index)).Generated();
}

unique_ptr<ParsedExpression> BindContext::CreateColumnReference(const string &catalog_name, const string &schema_name,
                                                                const string &table_name, const string &column_name) {
	string error_message;
	vector<string> names;
	if (!catalog_name.empty()) {
		names.push_back(catalog_name);
	}
	if (!schema_name.empty()) {
		names.push_back(schema_name);
	}
	names.push_back(table_name);
	names.push_back(column_name);

	auto result = make_uniq<ColumnRefExpression>(std::move(names));
	auto binding = GetBinding(table_name, error_message);
	if (!binding) {
		return std::move(result);
	}
	auto column_index = binding->GetBindingIndex(column_name);
	if (ColumnIsGenerated(*binding, column_index)) {
		return ExpandGeneratedColumn(table_name, column_name);
	} else if (column_index < binding->names.size() && binding->names[column_index] != column_name) {
		// because of case insensitivity in the binder we rename the column to the original name
		// as it appears in the binding itself
		result->alias = binding->names[column_index];
	}
	return std::move(result);
}

unique_ptr<ParsedExpression> BindContext::CreateColumnReference(const string &schema_name, const string &table_name,
                                                                const string &column_name) {
	string catalog_name;
	return CreateColumnReference(catalog_name, schema_name, table_name, column_name);
}

optional_ptr<Binding> BindContext::GetCTEBinding(const string &ctename) {
	auto match = cte_bindings.find(ctename);
	if (match == cte_bindings.end()) {
		return nullptr;
	}
	return match->second.get();
}

optional_ptr<Binding> BindContext::GetBinding(const string &name, string &out_error) {
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
	if (!colref.IsQualified()) {
		throw InternalException("Could not bind alias \"%s\"!", colref.GetColumnName());
	}

	string error;
	auto binding = GetBinding(colref.GetTableName(), error);
	if (!binding) {
		return BindResult(error);
	}
	return binding->Bind(colref, depth);
}

string BindContext::BindColumn(PositionalReferenceExpression &ref, string &table_name, string &column_name) {
	idx_t total_columns = 0;
	idx_t current_position = ref.index - 1;
	for (auto &entry : bindings_list) {
		auto &binding = entry.get();
		idx_t entry_column_count = binding.names.size();
		if (ref.index == 0) {
			// this is a row id
			table_name = binding.alias;
			column_name = "rowid";
			return string();
		}
		if (current_position < entry_column_count) {
			table_name = binding.alias;
			column_name = binding.names[current_position];
			return string();
		} else {
			total_columns += entry_column_count;
			current_position -= entry_column_count;
		}
	}
	return StringUtil::Format("Positional reference %d out of range (total %d columns)", ref.index, total_columns);
}

unique_ptr<ColumnRefExpression> BindContext::PositionToColumn(PositionalReferenceExpression &ref) {
	string table_name, column_name;

	string error = BindColumn(ref, table_name, column_name);
	if (!error.empty()) {
		throw BinderException(error);
	}
	return make_uniq<ColumnRefExpression>(column_name, table_name);
}

bool BindContext::CheckExclusionList(StarExpression &expr, const string &column_name,
                                     vector<unique_ptr<ParsedExpression>> &new_select_list,
                                     case_insensitive_set_t &excluded_columns) {
	if (expr.exclude_list.find(column_name) != expr.exclude_list.end()) {
		excluded_columns.insert(column_name);
		return true;
	}
	auto entry = expr.replace_list.find(column_name);
	if (entry != expr.replace_list.end()) {
		auto new_entry = entry->second->Copy();
		new_entry->alias = entry->first;
		excluded_columns.insert(entry->first);
		new_select_list.push_back(std::move(new_entry));
		return true;
	}
	return false;
}

void BindContext::GenerateAllColumnExpressions(StarExpression &expr,
                                               vector<unique_ptr<ParsedExpression>> &new_select_list) {
	if (bindings_list.empty()) {
		throw BinderException("* expression without FROM clause!");
	}
	case_insensitive_set_t excluded_columns;
	if (expr.relation_name.empty()) {
		// SELECT * case
		// bind all expressions of each table in-order
		reference_set_t<UsingColumnSet> handled_using_columns;
		for (auto &entry : bindings_list) {
			auto &binding = entry.get();
			for (auto &column_name : binding.names) {
				if (CheckExclusionList(expr, column_name, new_select_list, excluded_columns)) {
					continue;
				}
				// check if this column is a USING column
				auto using_binding_ptr = GetUsingBinding(column_name, binding.alias);
				if (using_binding_ptr) {
					auto &using_binding = *using_binding_ptr;
					// it is!
					// check if we have already emitted the using column
					if (handled_using_columns.find(using_binding) != handled_using_columns.end()) {
						// we have! bail out
						continue;
					}
					// we have not! output the using column
					if (using_binding.primary_binding.empty()) {
						// no primary binding: output a coalesce
						auto coalesce = make_uniq<OperatorExpression>(ExpressionType::OPERATOR_COALESCE);
						for (auto &child_binding : using_binding.bindings) {
							coalesce->children.push_back(make_uniq<ColumnRefExpression>(column_name, child_binding));
						}
						coalesce->alias = column_name;
						new_select_list.push_back(std::move(coalesce));
					} else {
						// primary binding: output the qualified column ref
						new_select_list.push_back(
						    make_uniq<ColumnRefExpression>(column_name, using_binding.primary_binding));
					}
					handled_using_columns.insert(using_binding);
					continue;
				}
				new_select_list.push_back(make_uniq<ColumnRefExpression>(column_name, binding.alias));
			}
		}
	} else {
		// SELECT tbl.* case
		// SELECT struct.* case
		string error;
		auto binding = GetBinding(expr.relation_name, error);
		bool is_struct_ref = false;
		if (!binding) {
			auto binding_name = GetMatchingBinding(expr.relation_name);
			if (binding_name.empty()) {
				throw BinderException(error);
			}
			binding = bindings[binding_name].get();
			is_struct_ref = true;
		}

		if (is_struct_ref) {
			auto col_idx = binding->GetBindingIndex(expr.relation_name);
			auto col_type = binding->types[col_idx];
			if (col_type.id() != LogicalTypeId::STRUCT) {
				throw BinderException(StringUtil::Format(
				    "Cannot extract field from expression \"%s\" because it is not a struct", expr.ToString()));
			}
			auto &struct_children = StructType::GetChildTypes(col_type);
			vector<string> column_names(3);
			column_names[0] = binding->alias;
			column_names[1] = expr.relation_name;
			for (auto &child : struct_children) {
				if (CheckExclusionList(expr, child.first, new_select_list, excluded_columns)) {
					continue;
				}
				column_names[2] = child.first;
				new_select_list.push_back(make_uniq<ColumnRefExpression>(column_names));
			}
		} else {
			for (auto &column_name : binding->names) {
				if (CheckExclusionList(expr, column_name, new_select_list, excluded_columns)) {
					continue;
				}

				new_select_list.push_back(make_uniq<ColumnRefExpression>(column_name, binding->alias));
			}
		}
	}
	for (auto &excluded : expr.exclude_list) {
		if (excluded_columns.find(excluded) == excluded_columns.end()) {
			throw BinderException("Column \"%s\" in EXCLUDE list not found in %s", excluded,
			                      expr.relation_name.empty() ? "FROM clause" : expr.relation_name.c_str());
		}
	}
	for (auto &entry : expr.replace_list) {
		if (excluded_columns.find(entry.first) == excluded_columns.end()) {
			throw BinderException("Column \"%s\" in REPLACE list not found in %s", entry.first,
			                      expr.relation_name.empty() ? "FROM clause" : expr.relation_name.c_str());
		}
	}
}

void BindContext::GetTypesAndNames(vector<string> &result_names, vector<LogicalType> &result_types) {
	for (auto &binding_entry : bindings_list) {
		auto &binding = binding_entry.get();
		D_ASSERT(binding.names.size() == binding.types.size());
		for (idx_t i = 0; i < binding.names.size(); i++) {
			result_names.push_back(binding.names[i]);
			result_types.push_back(binding.types[i]);
		}
	}
}

void BindContext::AddBinding(const string &alias, unique_ptr<Binding> binding) {
	if (bindings.find(alias) != bindings.end()) {
		throw BinderException("Duplicate alias \"%s\" in query!", alias);
	}
	bindings_list.push_back(*binding);
	bindings[alias] = std::move(binding);
}

void BindContext::AddBaseTable(idx_t index, const string &alias, const vector<string> &names,
                               const vector<LogicalType> &types, vector<column_t> &bound_column_ids,
                               StandardEntry *entry, bool add_row_id) {
	AddBinding(alias, make_uniq<TableBinding>(alias, types, names, bound_column_ids, entry, index, add_row_id));
}

void BindContext::AddTableFunction(idx_t index, const string &alias, const vector<string> &names,
                                   const vector<LogicalType> &types, vector<column_t> &bound_column_ids,
                                   StandardEntry *entry) {
	AddBinding(alias, make_uniq<TableBinding>(alias, types, names, bound_column_ids, entry, index));
}

static string AddColumnNameToBinding(const string &base_name, case_insensitive_set_t &current_names) {
	idx_t index = 1;
	string name = base_name;
	while (current_names.find(name) != current_names.end()) {
		name = base_name + ":" + std::to_string(index++);
	}
	current_names.insert(name);
	return name;
}

vector<string> BindContext::AliasColumnNames(const string &table_name, const vector<string> &names,
                                             const vector<string> &column_aliases) {
	vector<string> result;
	if (column_aliases.size() > names.size()) {
		throw BinderException("table \"%s\" has %lld columns available but %lld columns specified", table_name,
		                      names.size(), column_aliases.size());
	}
	case_insensitive_set_t current_names;
	// use any provided column aliases first
	for (idx_t i = 0; i < column_aliases.size(); i++) {
		result.push_back(AddColumnNameToBinding(column_aliases[i], current_names));
	}
	// if not enough aliases were provided, use the default names for remaining columns
	for (idx_t i = column_aliases.size(); i < names.size(); i++) {
		result.push_back(AddColumnNameToBinding(names[i], current_names));
	}
	return result;
}

void BindContext::AddSubquery(idx_t index, const string &alias, SubqueryRef &ref, BoundQueryNode &subquery) {
	auto names = AliasColumnNames(alias, subquery.names, ref.column_name_alias);
	AddGenericBinding(index, alias, names, subquery.types);
}

void BindContext::AddEntryBinding(idx_t index, const string &alias, const vector<string> &names,
                                  const vector<LogicalType> &types, StandardEntry &entry) {
	AddBinding(alias, make_uniq<EntryBinding>(alias, types, names, index, entry));
}

void BindContext::AddView(idx_t index, const string &alias, SubqueryRef &ref, BoundQueryNode &subquery,
                          ViewCatalogEntry *view) {
	auto names = AliasColumnNames(alias, subquery.names, ref.column_name_alias);
	AddEntryBinding(index, alias, names, subquery.types, view->Cast<StandardEntry>());
}

void BindContext::AddSubquery(idx_t index, const string &alias, TableFunctionRef &ref, BoundQueryNode &subquery) {
	auto names = AliasColumnNames(alias, subquery.names, ref.column_name_alias);
	AddGenericBinding(index, alias, names, subquery.types);
}

void BindContext::AddGenericBinding(idx_t index, const string &alias, const vector<string> &names,
                                    const vector<LogicalType> &types) {
	AddBinding(alias, make_uniq<Binding>(BindingType::BASE, alias, types, names, index));
}

void BindContext::AddCTEBinding(idx_t index, const string &alias, const vector<string> &names,
                                const vector<LogicalType> &types) {
	auto binding = make_shared<Binding>(BindingType::BASE, alias, types, names, index);

	if (cte_bindings.find(alias) != cte_bindings.end()) {
		throw BinderException("Duplicate alias \"%s\" in query!", alias);
	}
	cte_bindings[alias] = std::move(binding);
	cte_references[alias] = std::make_shared<idx_t>(0);
}

void BindContext::AddContext(BindContext other) {
	for (auto &binding : other.bindings) {
		if (bindings.find(binding.first) != bindings.end()) {
			throw BinderException("Duplicate alias \"%s\" in query!", binding.first);
		}
		bindings[binding.first] = std::move(binding.second);
	}
	for (auto &binding : other.bindings_list) {
		bindings_list.push_back(binding);
	}
	for (auto &entry : other.using_columns) {
		for (auto &alias : entry.second) {
#ifdef DEBUG
			for (auto &other_alias : using_columns[entry.first]) {
				for (auto &col : alias.get().bindings) {
					D_ASSERT(other_alias.get().bindings.find(col) == other_alias.get().bindings.end());
				}
			}
#endif
			using_columns[entry.first].insert(alias);
		}
	}
}

void BindContext::RemoveContext(vector<reference<Binding>> &other_bindings_list) {
	for (auto &other_binding : other_bindings_list) {
		auto it = std::remove_if(bindings_list.begin(), bindings_list.end(), [other_binding](reference<Binding> x) {
			return x.get().alias == other_binding.get().alias;
		});
		bindings_list.erase(it, bindings_list.end());
	}

	for (auto &other_binding : other_bindings_list) {
		auto &alias = other_binding.get().alias;
		if (bindings.find(alias) != bindings.end()) {
			bindings.erase(alias);
		}
	}
}

} // namespace duckdb
