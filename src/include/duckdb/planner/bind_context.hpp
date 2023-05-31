//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/bind_context.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/qualified_name_set.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/planner/table_binding.hpp"

namespace duckdb {
class Binder;
class LogicalGet;
class BoundQueryNode;

class StarExpression;

class TableCatalogEntry;
class TableFunctionCatalogEntry;

struct UsingColumnSet {
	string primary_binding;
	unordered_set<string> bindings;
};

//! The BindContext object keeps track of all the tables and columns that are
//! encountered during the binding process.
class BindContext {
public:
	//! Keep track of recursive CTE references
	case_insensitive_map_t<std::shared_ptr<idx_t>> cte_references;

public:
	//! Given a column name, find the matching table it belongs to. Throws an
	//! exception if no table has a column of the given name.
	string GetMatchingBinding(const string &column_name);
	//! Like GetMatchingBinding, but instead of throwing an error if multiple tables have the same binding it will
	//! return a list of all the matching ones
	unordered_set<string> GetMatchingBindings(const string &column_name);
	//! Like GetMatchingBindings, but returns the top 3 most similar bindings (in levenshtein distance) instead of the
	//! matching ones
	vector<string> GetSimilarBindings(const string &column_name);

	optional_ptr<Binding> GetCTEBinding(const string &ctename);
	//! Binds a column expression to the base table. Returns the bound expression
	//! or throws an exception if the column could not be bound.
	BindResult BindColumn(ColumnRefExpression &colref, idx_t depth);
	string BindColumn(PositionalReferenceExpression &ref, string &table_name, string &column_name);
	unique_ptr<ColumnRefExpression> PositionToColumn(PositionalReferenceExpression &ref);

	unique_ptr<ParsedExpression> ExpandGeneratedColumn(const string &table_name, const string &column_name);

	unique_ptr<ParsedExpression> CreateColumnReference(const string &table_name, const string &column_name);
	unique_ptr<ParsedExpression> CreateColumnReference(const string &schema_name, const string &table_name,
	                                                   const string &column_name);
	unique_ptr<ParsedExpression> CreateColumnReference(const string &catalog_name, const string &schema_name,
	                                                   const string &table_name, const string &column_name);

	//! Generate column expressions for all columns that are present in the
	//! referenced tables. This is used to resolve the * expression in a
	//! selection list.
	void GenerateAllColumnExpressions(StarExpression &expr, vector<unique_ptr<ParsedExpression>> &new_select_list);
	//! Check if the given (binding, column_name) is in the exclusion/replacement lists.
	//! Returns true if it is in one of these lists, and should therefore be skipped.
	bool CheckExclusionList(StarExpression &expr, const string &column_name,
	                        vector<unique_ptr<ParsedExpression>> &new_select_list,
	                        case_insensitive_set_t &excluded_columns);

	const vector<reference<Binding>> &GetBindingsList() {
		return bindings_list;
	}

	void GetTypesAndNames(vector<string> &result_names, vector<LogicalType> &result_types);

	//! Adds a base table with the given alias to the BindContext.
	void AddBaseTable(idx_t index, const string &alias, const vector<string> &names, const vector<LogicalType> &types,
	                  vector<column_t> &bound_column_ids, StandardEntry *entry, bool add_row_id = true);
	//! Adds a call to a table function with the given alias to the BindContext.
	void AddTableFunction(idx_t index, const string &alias, const vector<string> &names,
	                      const vector<LogicalType> &types, vector<column_t> &bound_column_ids, StandardEntry *entry);
	//! Adds a table view with a given alias to the BindContext.
	void AddView(idx_t index, const string &alias, SubqueryRef &ref, BoundQueryNode &subquery, ViewCatalogEntry *view);
	//! Adds a subquery with a given alias to the BindContext.
	void AddSubquery(idx_t index, const string &alias, SubqueryRef &ref, BoundQueryNode &subquery);
	//! Adds a subquery with a given alias to the BindContext.
	void AddSubquery(idx_t index, const string &alias, TableFunctionRef &ref, BoundQueryNode &subquery);
	//! Adds a binding to a catalog entry with a given alias to the BindContext.
	void AddEntryBinding(idx_t index, const string &alias, const vector<string> &names,
	                     const vector<LogicalType> &types, StandardEntry &entry);
	//! Adds a base table with the given alias to the BindContext.
	void AddGenericBinding(idx_t index, const string &alias, const vector<string> &names,
	                       const vector<LogicalType> &types);

	//! Adds a base table with the given alias to the CTE BindContext.
	//! We need this to correctly bind recursive CTEs with multiple references.
	void AddCTEBinding(idx_t index, const string &alias, const vector<string> &names, const vector<LogicalType> &types);

	//! Add an implicit join condition (e.g. USING (x))
	void AddUsingBinding(const string &column_name, UsingColumnSet &set);

	void AddUsingBindingSet(unique_ptr<UsingColumnSet> set);

	//! Returns any using column set for the given column name, or nullptr if there is none. On conflict (multiple using
	//! column sets with the same name) throw an exception.
	optional_ptr<UsingColumnSet> GetUsingBinding(const string &column_name);
	//! Returns any using column set for the given column name, or nullptr if there is none
	optional_ptr<UsingColumnSet> GetUsingBinding(const string &column_name, const string &binding_name);
	//! Erase a using binding from the set of using bindings
	void RemoveUsingBinding(const string &column_name, UsingColumnSet &set);
	//! Transfer a using binding from one bind context to this bind context
	void TransferUsingBinding(BindContext &current_context, optional_ptr<UsingColumnSet> current_set,
	                          UsingColumnSet &new_set, const string &binding, const string &using_column);

	//! Fetch the actual column name from the given binding, or throws if none exists
	//! This can be different from "column_name" because of case insensitivity
	//! (e.g. "column_name" might return "COLUMN_NAME")
	string GetActualColumnName(const string &binding, const string &column_name);

	case_insensitive_map_t<std::shared_ptr<Binding>> GetCTEBindings() {
		return cte_bindings;
	}
	void SetCTEBindings(case_insensitive_map_t<std::shared_ptr<Binding>> bindings) {
		cte_bindings = bindings;
	}

	//! Alias a set of column names for the specified table, using the original names if there are not enough aliases
	//! specified.
	static vector<string> AliasColumnNames(const string &table_name, const vector<string> &names,
	                                       const vector<string> &column_aliases);

	//! Add all the bindings from a BindContext to this BindContext. The other BindContext is destroyed in the process.
	void AddContext(BindContext other);
	//! For semi and anti joins we remove the binding context of the right table after binding the condition.
	void RemoveContext(vector<reference<Binding>> &other_bindings_list);

	//! Gets a binding of the specified name. Returns a nullptr and sets the out_error if the binding could not be
	//! found.
	optional_ptr<Binding> GetBinding(const string &name, string &out_error);

private:
	void AddBinding(const string &alias, unique_ptr<Binding> binding);

private:
	//! The set of bindings
	case_insensitive_map_t<unique_ptr<Binding>> bindings;
	//! The list of bindings in insertion order
	vector<reference<Binding>> bindings_list;
	//! The set of columns used in USING join conditions
	case_insensitive_map_t<reference_set_t<UsingColumnSet>> using_columns;
	//! Using column sets
	vector<unique_ptr<UsingColumnSet>> using_column_sets;

	//! The set of CTE bindings
	case_insensitive_map_t<std::shared_ptr<Binding>> cte_bindings;
};
} // namespace duckdb
