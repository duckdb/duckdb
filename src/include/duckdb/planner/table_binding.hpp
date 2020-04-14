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

enum class BindingType : uint8_t { TABLE = 0, SUBQUERY = 1, TABLE_FUNCTION = 2, GENERIC = 3 };

//! A Binding represents a binding to a table, table-producing function or subquery with a specified table index. Used
//! in the binder.
struct Binding {
	Binding(BindingType type, const string &alias, idx_t index) : type(type), alias(alias), index(index) {
	}
	virtual ~Binding() = default;

	BindingType type;
	string alias;
	idx_t index;

public:
	virtual bool HasMatchingBinding(const string &column_name) = 0;
	virtual BindResult Bind(ColumnRefExpression &colref, idx_t depth) = 0;
	virtual void GenerateAllColumnExpressions(BindContext &context,
	                                          vector<unique_ptr<ParsedExpression>> &select_list) = 0;
};

//! Represents a binding to a base table
struct TableBinding : public Binding {
	TableBinding(const string &alias, TableCatalogEntry &table, LogicalGet &get, idx_t index);

	TableCatalogEntry &table;
	LogicalGet &get;

public:
	bool HasMatchingBinding(const string &column_name) override;
	BindResult Bind(ColumnRefExpression &colref, idx_t depth) override;
	void GenerateAllColumnExpressions(BindContext &context, vector<unique_ptr<ParsedExpression>> &select_list) override;
};

//! Represents a generic binding with types and names
struct GenericBinding : public Binding {
	GenericBinding(const string &alias, vector<SQLType> types, vector<string> names, idx_t index);

	vector<SQLType> types;
	//! Column names of the subquery
	vector<string> names;
	//! Name -> index for the names
	unordered_map<string, uint64_t> name_map;

public:
	bool HasMatchingBinding(const string &column_name) override;
	BindResult Bind(ColumnRefExpression &colref, idx_t depth) override;
	void GenerateAllColumnExpressions(BindContext &context, vector<unique_ptr<ParsedExpression>> &select_list) override;
};

} // namespace duckdb
