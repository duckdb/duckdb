//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/table_binding.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {
class BindContext;
class BoundQueryNode;
class ColumnRefExpression;
class SubqueryRef;
class LogicalGet;
class TableCatalogEntry;
class TableFunctionCatalogEntry;
class BoundTableFunction;

//! A Binding represents a binding to a table, table-producing function or subquery with a specified table index.
struct Binding {
	Binding(const string &alias, vector<LogicalType> types, vector<string> names, idx_t index);
	Binding(const string &alias, idx_t index);
	virtual ~Binding() = default;

	//! The alias of the binding
	string alias;
	//! The table index of the binding
	idx_t index;
	vector<LogicalType> types;
	//! Column names of the subquery
	vector<string> names;
	//! Name -> index for the names
	unordered_map<string, column_t> name_map;
public:
	bool HasMatchingBinding(const string &column_name);
	virtual BindResult Bind(ColumnRefExpression &colref, idx_t depth);
	void GenerateAllColumnExpressions(BindContext &context,
	                                          vector<unique_ptr<ParsedExpression>> &select_list);
};

//! TableBinding is exactly like the Binding, except it keeps track of which columns were bound in the linked LogicalGet
//! node for projection pushdown purposes.
struct TableBinding : public Binding {
	TableBinding(const string &alias, vector<LogicalType> types, vector<string> names, LogicalGet &get, idx_t index);
	TableBinding(const string &alias, vector<LogicalType> types, vector<string> names, unordered_map<string, column_t> name_map, LogicalGet &get, idx_t index);

	//! the underlying LogicalGet
	LogicalGet &get;

public:
	BindResult Bind(ColumnRefExpression &colref, idx_t depth) override;
};

} // namespace duckdb
