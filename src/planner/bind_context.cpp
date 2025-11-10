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
#include "duckdb/planner/binder.hpp"

#include <algorithm>

namespace duckdb {

BindContext::BindContext(Binder &binder) : binder(binder) {
}

string MinimumUniqueAlias(const BindingAlias &alias, const BindingAlias &other) {
	if (!StringUtil::CIEquals(alias.GetAlias(), other.GetAlias())) {
		return alias.GetAlias();
	}
	if (!StringUtil::CIEquals(alias.GetSchema(), other.GetSchema())) {
		return alias.GetSchema() + "." + alias.GetAlias();
	}
	return alias.ToString();
}

optional_ptr<Binding> BindContext::GetMatchingBinding(const string &column_name) {
	optional_ptr<Binding> result;
	for (auto &binding_ptr : bindings_list) {
		auto &binding = *binding_ptr;
		auto is_using_binding = GetUsingBinding(column_name, binding.GetBindingAlias());
		if (is_using_binding) {
			continue;
		}
		if (binding.HasMatchingBinding(column_name)) {
			if (result || is_using_binding) {
				throw BinderException(
				    "Ambiguous reference to column name \"%s\" (use: \"%s.%s\" "
				    "or \"%s.%s\")",
				    column_name, MinimumUniqueAlias(result->GetBindingAlias(), binding.GetBindingAlias()), column_name,
				    MinimumUniqueAlias(binding.GetBindingAlias(), result->GetBindingAlias()), column_name);
			}
			result = &binding;
		}
	}
	return result;
}

vector<string> BindContext::GetSimilarBindings(const string &column_name) {
	vector<pair<string, double>> scores;
	for (auto &binding_ptr : bindings_list) {
		auto &binding = *binding_ptr;
		for (auto &name : binding.GetColumnNames()) {
			double distance = StringUtil::SimilarityRating(name, column_name);
			// check if we need to qualify the column
			auto matching_bindings = GetMatchingBindings(name);
			if (matching_bindings.size() > 1) {
				scores.emplace_back(binding.GetAlias() + "." + name, distance);
			} else {
				scores.emplace_back(name, distance);
			}
		}
	}
	return StringUtil::TopNStrings(scores);
}

void BindContext::AddUsingBinding(const string &column_name, UsingColumnSet &set) {
	using_columns[column_name].insert(set);
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
				result_bindings += binding.GetAlias();
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

optional_ptr<UsingColumnSet> BindContext::GetUsingBinding(const string &column_name, const BindingAlias &binding) {
	if (!binding.IsSet()) {
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
		for (auto &using_binding : bindings) {
			if (using_binding == binding) {
				return &using_set;
			}
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
                                       UsingColumnSet &new_set, const string &using_column) {
	AddUsingBinding(using_column, new_set);
	if (current_set) {
		current_context.RemoveUsingBinding(using_column, *current_set);
	}
}

string BindContext::GetActualColumnName(Binding &binding, const string &column_name) {
	column_t binding_index;
	if (!binding.TryGetBindingIndex(column_name, binding_index)) { // LCOV_EXCL_START
		throw InternalException("Binding with name \"%s\" does not have a column named \"%s\"", binding.GetAlias(),
		                        column_name);
	} // LCOV_EXCL_STOP
	return binding.GetColumnNames()[binding_index];
}

string BindContext::GetActualColumnName(const BindingAlias &binding_alias, const string &column_name) {
	ErrorData error;
	auto binding = GetBinding(binding_alias, error);
	if (!binding) {
		throw InternalException("No binding with name \"%s\": %s", binding_alias.GetAlias(), error.RawMessage());
	}
	return GetActualColumnName(*binding, column_name);
}

vector<reference<Binding>> BindContext::GetMatchingBindings(const string &column_name) {
	vector<reference<Binding>> result;
	for (auto &binding_ptr : bindings_list) {
		auto &binding = *binding_ptr;
		if (binding.HasMatchingBinding(column_name)) {
			result.push_back(binding);
		}
	}
	return result;
}

unique_ptr<ParsedExpression> BindContext::ExpandGeneratedColumn(TableBinding &table_binding,
                                                                const string &column_name) {
	auto result = table_binding.ExpandGeneratedColumn(column_name);
	result->SetAlias(column_name);
	return result;
}

unique_ptr<ParsedExpression> BindContext::CreateColumnReference(const BindingAlias &table_alias,
                                                                const string &column_name, ColumnBindType bind_type) {
	return CreateColumnReference(table_alias.GetCatalog(), table_alias.GetSchema(), table_alias.GetAlias(), column_name,
	                             bind_type);
}

unique_ptr<ParsedExpression> BindContext::CreateColumnReference(const string &table_name, const string &column_name,
                                                                ColumnBindType bind_type) {
	string schema_name;
	return CreateColumnReference(schema_name, table_name, column_name, bind_type);
}

static bool ColumnIsGenerated(Binding &binding, column_t index) {
	if (binding.GetBindingType() != BindingType::TABLE) {
		return false;
	}
	auto &table_binding = binding.Cast<TableBinding>();
	auto catalog_entry = table_binding.GetStandardEntry();
	if (!catalog_entry) {
		return false;
	}
	if (IsVirtualColumn(index)) {
		return false;
	}
	D_ASSERT(catalog_entry->type == CatalogType::TABLE_ENTRY);
	auto &table_entry = catalog_entry->Cast<TableCatalogEntry>();
	return table_entry.GetColumn(LogicalIndex(index)).Generated();
}

unique_ptr<ParsedExpression> BindContext::CreateColumnReference(const string &catalog_name, const string &schema_name,
                                                                const string &table_name, const string &column_name,
                                                                ColumnBindType bind_type) {
	ErrorData error;
	vector<string> names;
	if (!catalog_name.empty()) {
		names.push_back(catalog_name);
	}
	if (!schema_name.empty()) {
		names.push_back(schema_name);
	}
	names.push_back(table_name);
	names.push_back(column_name);

	BindingAlias alias(catalog_name, schema_name, table_name);
	auto result = make_uniq<ColumnRefExpression>(std::move(names));
	auto binding = GetBinding(alias, column_name, error);
	if (!binding) {
		return std::move(result);
	}
	auto column_index = binding->GetBindingIndex(column_name);
	if (bind_type == ColumnBindType::EXPAND_GENERATED_COLUMNS && ColumnIsGenerated(*binding, column_index)) {
		return ExpandGeneratedColumn(binding->Cast<TableBinding>(), column_name);
	}
	auto &column_names = binding->GetColumnNames();
	if (column_index < column_names.size() && column_names[column_index] != column_name) {
		// because of case insensitivity in the binder we rename the column to the original name
		// as it appears in the binding itself
		result->SetAlias(column_names[column_index]);
	}
	return std::move(result);
}

unique_ptr<ParsedExpression> BindContext::CreateColumnReference(const string &schema_name, const string &table_name,
                                                                const string &column_name, ColumnBindType bind_type) {
	string catalog_name;
	return CreateColumnReference(catalog_name, schema_name, table_name, column_name, bind_type);
}

string GetCandidateAlias(const BindingAlias &main_alias, const BindingAlias &new_alias) {
	string candidate;
	if (!main_alias.GetCatalog().empty() && !new_alias.GetCatalog().empty()) {
		candidate += new_alias.GetCatalog() + ".";
	}
	if (!main_alias.GetSchema().empty() && !new_alias.GetSchema().empty()) {
		candidate += new_alias.GetSchema() + ".";
	}
	candidate += new_alias.GetAlias();
	return candidate;
}

vector<reference<Binding>> BindContext::GetBindings(const BindingAlias &alias, ErrorData &out_error) {
	if (!alias.IsSet()) {
		throw InternalException("BindingAlias is not set");
	}
	vector<reference<Binding>> matching_bindings;
	for (auto &binding : bindings_list) {
		if (binding->GetBindingAlias().Matches(alias)) {
			matching_bindings.push_back(*binding);
		}
	}
	if (matching_bindings.empty()) {
		// alias not found in this BindContext
		vector<string> candidates;
		for (auto &binding : bindings_list) {
			candidates.push_back(GetCandidateAlias(alias, binding->GetBindingAlias()));
		}
		auto main_alias = GetCandidateAlias(alias, alias);
		string candidate_str =
		    StringUtil::CandidatesMessage(StringUtil::TopNJaroWinkler(candidates, main_alias), "Candidate tables");
		out_error = ErrorData(ExceptionType::BINDER,
		                      StringUtil::Format("Referenced table \"%s\" not found!%s", main_alias, candidate_str));
	}
	return matching_bindings;
}

string BindContext::AmbiguityException(const BindingAlias &alias, const vector<reference<Binding>> &bindings) {
	D_ASSERT(bindings.size() > 1);
	// found multiple matching aliases
	string result = "(use: ";
	for (idx_t i = 0; i < bindings.size(); i++) {
		if (i > 0) {
			if (i + 1 == bindings.size()) {
				result += " or ";
			} else {
				result += ", ";
			}
		}
		// find the minimum alias that uniquely describes this table reference
		auto &current_alias = bindings[i].get().GetBindingAlias();
		string minimum_alias;
		bool duplicate_alias = false;
		for (idx_t k = 0; k < bindings.size(); k++) {
			if (k == i) {
				continue;
			}
			auto &other_alias = bindings[k].get().GetBindingAlias();
			if (current_alias == other_alias) {
				duplicate_alias = true;
			}
			string new_minimum_alias = MinimumUniqueAlias(current_alias, other_alias);
			if (new_minimum_alias.size() > minimum_alias.size()) {
				minimum_alias = std::move(new_minimum_alias);
			}
		}
		if (duplicate_alias) {
			result = "(duplicate alias \"" + alias.ToString() +
			         "\", explicitly alias one of the tables using \"AS my_alias\"";
		} else {
			result += minimum_alias;
		}
	}
	result += ")";
	return result;
}

optional_ptr<Binding> BindContext::GetBinding(const BindingAlias &alias, const string &column_name,
                                              ErrorData &out_error) {
	auto matching_bindings = GetBindings(alias, out_error);
	if (matching_bindings.empty()) {
		// no bindings found
		return nullptr;
	}

	optional_ptr<Binding> result;
	// find the binding that this column name belongs to
	for (auto &binding_ref : matching_bindings) {
		auto &binding = binding_ref.get();
		if (!binding.HasMatchingBinding(column_name)) {
			continue;
		}
		if (result) {
			// we found multiple bindings that this column name belongs to - ambiguity
			string helper_message = AmbiguityException(alias, matching_bindings);
			throw BinderException("Ambiguous reference to table \"%s\" %s", alias.ToString(), helper_message);
		} else {
			result = &binding;
		}
	}
	if (!result) {
		// found the table binding - but could not find the column
		out_error = matching_bindings[0].get().ColumnNotFoundError(column_name);
	}
	return result;
}

optional_ptr<Binding> BindContext::GetBinding(const BindingAlias &alias, ErrorData &out_error) {
	auto matching_bindings = GetBindings(alias, out_error);
	if (matching_bindings.empty()) {
		return nullptr;
	}
	if (matching_bindings.size() > 1) {
		string helper_message = AmbiguityException(alias, matching_bindings);
		throw BinderException("Ambiguous reference to table \"%s\" %s", alias.ToString(), helper_message);
	}
	// found a single matching alias
	return &matching_bindings[0].get();
}

optional_ptr<Binding> BindContext::GetBinding(const string &name, ErrorData &out_error) {
	return GetBinding(BindingAlias(name), out_error);
}

BindingAlias GetBindingAlias(ColumnRefExpression &colref) {
	if (colref.column_names.size() <= 1 || colref.column_names.size() > 4) {
		throw InternalException("Cannot get binding alias from column ref unless it has 2..4 entries");
	}
	if (colref.column_names.size() >= 4) {
		return BindingAlias(colref.column_names[0], colref.column_names[1], colref.column_names[2]);
	}
	if (colref.column_names.size() == 3) {
		return BindingAlias(colref.column_names[0], colref.column_names[1]);
	}
	return BindingAlias(colref.column_names[0]);
}

BindResult BindContext::BindColumn(ColumnRefExpression &colref, idx_t depth) {
	if (!colref.IsQualified()) {
		throw InternalException("Could not bind alias \"%s\"!", colref.GetColumnName());
	}

	ErrorData error;
	BindingAlias alias;
	auto binding = GetBinding(GetBindingAlias(colref), colref.GetColumnName(), error);
	if (!binding) {
		return BindResult(std::move(error));
	}
	return binding->Bind(colref, depth);
}

string BindContext::BindColumn(PositionalReferenceExpression &ref, string &table_name, string &column_name) {
	idx_t total_columns = 0;
	idx_t current_position = ref.index - 1;
	for (auto &entry : bindings_list) {
		auto &binding = *entry;
		auto &column_names = binding.GetColumnNames();
		idx_t entry_column_count = column_names.size();
		if (ref.index == 0) {
			// this is a row id
			table_name = binding.GetAlias();
			column_name = "rowid";
			return string();
		}
		if (current_position < entry_column_count) {
			table_name = binding.GetAlias();
			column_name = column_names[current_position];
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

struct ExclusionListInfo {
	explicit ExclusionListInfo(vector<unique_ptr<ParsedExpression>> &new_select_list)
	    : new_select_list(new_select_list) {
	}

	vector<unique_ptr<ParsedExpression>> &new_select_list;
	case_insensitive_set_t excluded_columns;
	qualified_column_set_t excluded_qualified_columns;
};

bool CheckExclusionList(StarExpression &expr, const QualifiedColumnName &qualified_name, ExclusionListInfo &info) {
	if (expr.exclude_list.find(qualified_name) != expr.exclude_list.end()) {
		info.excluded_qualified_columns.insert(qualified_name);
		return true;
	}
	auto entry = expr.replace_list.find(qualified_name.column);
	if (entry != expr.replace_list.end()) {
		auto new_entry = entry->second->Copy();
		new_entry->SetAlias(entry->first);
		info.excluded_columns.insert(entry->first);
		info.new_select_list.push_back(std::move(new_entry));
		return true;
	}
	return false;
}

void HandleRename(StarExpression &expr, const QualifiedColumnName &qualified_name, ParsedExpression &new_expr) {
	auto rename_entry = expr.rename_list.find(qualified_name);
	if (rename_entry != expr.rename_list.end()) {
		new_expr.SetAlias(rename_entry->second);
	}
}

void BindContext::GenerateAllColumnExpressions(StarExpression &expr,
                                               vector<unique_ptr<ParsedExpression>> &new_select_list) {
	if (bindings_list.empty()) {
		throw BinderException("* expression without FROM clause!");
	}
	ExclusionListInfo exclusion_info(new_select_list);
	if (expr.relation_name.empty()) {
		// SELECT * case
		// bind all expressions of each table in-order
		reference_set_t<UsingColumnSet> handled_using_columns;
		for (auto &entry : bindings_list) {
			auto &binding = *entry;
			auto &column_names = binding.GetColumnNames();
			auto &binding_alias = binding.GetBindingAlias();
			for (auto &column_name : column_names) {
				QualifiedColumnName qualified_column(binding_alias, column_name);
				if (CheckExclusionList(expr, qualified_column, exclusion_info)) {
					continue;
				}
				// check if this column is a USING column
				auto using_binding_ptr = GetUsingBinding(column_name, binding_alias);
				if (using_binding_ptr) {
					auto &using_binding = *using_binding_ptr;
					// it is!
					// check if we have already emitted the using column
					if (handled_using_columns.find(using_binding) != handled_using_columns.end()) {
						// we have! bail out
						continue;
					}
					// we have not! output the using column
					if (!using_binding.primary_binding.IsSet()) {
						// no primary binding: output a coalesce
						auto coalesce = make_uniq<OperatorExpression>(ExpressionType::OPERATOR_COALESCE);
						for (auto &child_binding : using_binding.bindings) {
							coalesce->children.push_back(make_uniq<ColumnRefExpression>(column_name, child_binding));
						}
						coalesce->SetAlias(column_name);
						HandleRename(expr, qualified_column, *coalesce);
						new_select_list.push_back(std::move(coalesce));
					} else {
						// primary binding: output the qualified column ref
						auto new_expr = make_uniq<ColumnRefExpression>(column_name, using_binding.primary_binding);
						HandleRename(expr, qualified_column, *new_expr);
						new_select_list.push_back(std::move(new_expr));
					}
					handled_using_columns.insert(using_binding);
					continue;
				}
				auto new_expr =
				    CreateColumnReference(binding_alias, column_name, ColumnBindType::DO_NOT_EXPAND_GENERATED_COLUMNS);
				HandleRename(expr, qualified_column, *new_expr);
				new_select_list.push_back(std::move(new_expr));
			}
		}
	} else {
		// SELECT tbl.* case
		// SELECT struct.* case
		ErrorData error;
		auto binding = GetBinding(expr.relation_name, error);
		bool is_struct_ref = false;
		if (!binding) {
			binding = GetMatchingBinding(expr.relation_name);
			if (!binding) {
				error.Throw();
			}
			is_struct_ref = true;
		}
		auto &binding_alias = binding->GetBindingAlias();
		auto &column_names = binding->GetColumnNames();
		auto &column_types = binding->GetColumnTypes();

		if (is_struct_ref) {
			auto col_idx = binding->GetBindingIndex(expr.relation_name);
			auto col_type = column_types[col_idx];
			if (col_type.id() != LogicalTypeId::STRUCT) {
				throw BinderException(StringUtil::Format(
				    "Cannot extract field from expression \"%s\" because it is not a struct", expr.ToString()));
			}
			auto &struct_children = StructType::GetChildTypes(col_type);
			vector<string> column_names(3);
			column_names[0] = binding->GetAlias();
			column_names[1] = expr.relation_name;
			for (auto &child : struct_children) {
				QualifiedColumnName qualified_name(child.first);
				if (CheckExclusionList(expr, qualified_name, exclusion_info)) {
					continue;
				}
				column_names[2] = child.first;
				auto new_expr = make_uniq<ColumnRefExpression>(column_names);
				HandleRename(expr, qualified_name, *new_expr);
				new_select_list.push_back(std::move(new_expr));
			}
		} else {
			for (auto &column_name : column_names) {
				QualifiedColumnName qualified_name(binding_alias, column_name);
				if (CheckExclusionList(expr, qualified_name, exclusion_info)) {
					continue;
				}
				auto new_expr =
				    CreateColumnReference(binding_alias, column_name, ColumnBindType::DO_NOT_EXPAND_GENERATED_COLUMNS);
				HandleRename(expr, qualified_name, *new_expr);
				new_select_list.push_back(std::move(new_expr));
			}
		}
	}

	if (binder.GetBindingMode() == BindingMode::EXTRACT_NAMES ||
	    binder.GetBindingMode() == BindingMode::EXTRACT_QUALIFIED_NAMES) {
		//! We only care about extracting the names of the referenced columns
		//! remove the exclude + replace lists
		expr.exclude_list.clear();
		expr.replace_list.clear();
	}

	//! Verify correctness of the exclude list
	for (auto &excluded : expr.exclude_list) {
		if (exclusion_info.excluded_qualified_columns.find(excluded) ==
		    exclusion_info.excluded_qualified_columns.end()) {
			throw BinderException("Column \"%s\" in EXCLUDE list not found in %s", excluded.ToString(),
			                      expr.relation_name.empty() ? "FROM clause" : expr.relation_name.c_str());
		}
	}

	//! Verify correctness of the replace list
	for (auto &entry : expr.replace_list) {
		if (exclusion_info.excluded_columns.find(entry.first) == exclusion_info.excluded_columns.end()) {
			throw BinderException("Column \"%s\" in REPLACE list not found in %s", entry.first,
			                      expr.relation_name.empty() ? "FROM clause" : expr.relation_name.c_str());
		}
	}
}

void BindContext::GetTypesAndNames(vector<string> &result_names, vector<LogicalType> &result_types) {
	for (auto &binding_entry : bindings_list) {
		auto &binding = *binding_entry;
		auto &column_names = binding.GetColumnNames();
		auto &column_types = binding.GetColumnTypes();
		D_ASSERT(column_names.size() == column_types.size());
		for (idx_t i = 0; i < column_names.size(); i++) {
			result_names.push_back(column_names[i]);
			result_types.push_back(column_types[i]);
		}
	}
}

void BindContext::AddBinding(unique_ptr<Binding> binding) {
	bindings_list.push_back(std::move(binding));
}

void BindContext::AddBaseTable(idx_t index, const string &alias, const vector<string> &names,
                               const vector<LogicalType> &types, vector<ColumnIndex> &bound_column_ids,
                               TableCatalogEntry &entry, virtual_column_map_t virtual_columns) {
	AddBinding(
	    make_uniq<TableBinding>(alias, types, names, bound_column_ids, &entry, index, std::move(virtual_columns)));
}

void BindContext::AddBaseTable(idx_t index, const string &alias, const vector<string> &names,
                               const vector<LogicalType> &types, vector<ColumnIndex> &bound_column_ids,
                               TableCatalogEntry &entry, bool add_virtual_columns) {
	virtual_column_map_t virtual_columns;
	if (add_virtual_columns) {
		virtual_columns = entry.GetVirtualColumns();
	}
	AddBaseTable(index, alias, names, types, bound_column_ids, entry, std::move(virtual_columns));
}

void BindContext::AddBaseTable(idx_t index, const string &alias, const vector<string> &names,
                               const vector<LogicalType> &types, vector<ColumnIndex> &bound_column_ids,
                               const string &table_name) {
	virtual_column_map_t virtual_columns;
	AddBinding(make_uniq<TableBinding>(alias.empty() ? table_name : alias, types, names, bound_column_ids, nullptr,
	                                   index, std::move(virtual_columns)));
}

void BindContext::AddTableFunction(idx_t index, const string &alias, const vector<string> &names,
                                   const vector<LogicalType> &types, vector<ColumnIndex> &bound_column_ids,
                                   optional_ptr<StandardEntry> entry, virtual_column_map_t virtual_columns) {
	AddBinding(
	    make_uniq<TableBinding>(alias, types, names, bound_column_ids, entry, index, std::move(virtual_columns)));
}

static string AddColumnNameToBinding(const string &base_name, case_insensitive_set_t &current_names) {
	idx_t index = 1;
	string name = base_name;
	while (current_names.find(name) != current_names.end()) {
		name = base_name + "_" + std::to_string(index++);
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

void BindContext::AddSubquery(idx_t index, const string &alias, SubqueryRef &ref, BoundStatement &subquery) {
	auto names = AliasColumnNames(alias, subquery.names, ref.column_name_alias);
	AddGenericBinding(index, alias, names, subquery.types);
}

void BindContext::AddEntryBinding(idx_t index, const string &alias, const vector<string> &names,
                                  const vector<LogicalType> &types, StandardEntry &entry) {
	AddBinding(make_uniq<EntryBinding>(alias, types, names, index, entry));
}

void BindContext::AddView(idx_t index, const string &alias, SubqueryRef &ref, BoundStatement &subquery,
                          ViewCatalogEntry &view) {
	auto names = AliasColumnNames(alias, subquery.names, ref.column_name_alias);
	AddEntryBinding(index, alias, names, subquery.types, view.Cast<StandardEntry>());
}

void BindContext::AddSubquery(idx_t index, const string &alias, TableFunctionRef &ref, BoundStatement &subquery) {
	auto names = AliasColumnNames(alias, subquery.names, ref.column_name_alias);
	AddGenericBinding(index, alias, names, subquery.types);
}

void BindContext::AddGenericBinding(idx_t index, const string &alias, const vector<string> &names,
                                    const vector<LogicalType> &types) {
	AddBinding(make_uniq<Binding>(BindingType::BASE, BindingAlias(alias), types, names, index));
}

void BindContext::AddCTEBinding(unique_ptr<CTEBinding> binding) {
	for (auto &cte_binding : cte_bindings) {
		if (cte_binding->GetBindingAlias() == binding->GetBindingAlias()) {
			throw BinderException("Duplicate CTE binding \"%s\" in query!", binding->GetBindingAlias().ToString());
		}
	}
	cte_bindings.push_back(std::move(binding));
}

void BindContext::AddCTEBinding(idx_t index, BindingAlias alias_p, const vector<string> &names,
                                const vector<LogicalType> &types, CTEType cte_type) {
	auto binding = make_uniq<CTEBinding>(std::move(alias_p), types, names, index, cte_type);
	AddCTEBinding(std::move(binding));
}

optional_ptr<CTEBinding> BindContext::GetCTEBinding(const BindingAlias &ctename) {
	for (auto &binding : cte_bindings) {
		if (binding->GetBindingAlias().Matches(ctename)) {
			return binding.get();
		}
	}
	return nullptr;
}

void BindContext::AddContext(BindContext other) {
	for (auto &binding : other.bindings_list) {
		AddBinding(std::move(binding));
	}
	for (auto &entry : other.using_columns) {
		for (auto &alias : entry.second) {
			using_columns[entry.first].insert(alias);
		}
	}
}

vector<BindingAlias> BindContext::GetBindingAliases() {
	vector<BindingAlias> result;
	for (auto &binding : bindings_list) {
		result.push_back(binding->GetBindingAlias());
	}
	return result;
}

void BindContext::RemoveContext(const vector<BindingAlias> &aliases) {
	for (auto &alias : aliases) {
		// remove the binding from any USING columns
		vector<string> removed_using_columns;
		for (auto &using_sets : using_columns) {
			for (auto &using_set_ref : using_sets.second) {
				auto &using_set = using_set_ref.get();
				auto it = std::remove_if(using_set.bindings.begin(), using_set.bindings.end(),
				                         [&](const BindingAlias &using_alias) { return using_alias == alias; });
				using_set.bindings.erase(it, using_set.bindings.end());
				if (using_set.bindings.empty() || using_set.primary_binding == alias) {
					// if the using column is no longer referred to - remove it entirely
					removed_using_columns.push_back(using_sets.first);
				}
			}
		}
		for (auto &removed_col : removed_using_columns) {
			using_columns.erase(removed_col);
		}

		// remove the binding from the list of bindings
		auto it = std::remove_if(bindings_list.begin(), bindings_list.end(),
		                         [&](unique_ptr<Binding> &x) { return x->GetBindingAlias() == alias; });
		bindings_list.erase(it, bindings_list.end());
	}
}

} // namespace duckdb
