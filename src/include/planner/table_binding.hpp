//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/table_binding.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"
#include "common/unordered_map.hpp"
#include "parser/column_definition.hpp"
#include "parser/parsed_expression.hpp"
#include "planner/expression_binder.hpp"

namespace duckdb {
class BindContext;
class BoundBaseTableRef;
class BoundQueryNode;
class ColumnRefExpression;
class SubqueryRef;
class TableCatalogEntry;
class TableFunctionCatalogEntry;

enum class BindingType : uint8_t { TABLE = 0, SUBQUERY = 1, TABLE_FUNCTION = 2 };

//! A Binding represents a binding to a table, table-producing function or subquery with a specified table index. Used
//! in the binder.
struct Binding {
	Binding(BindingType type, const string &alias, index_t index) : type(type), alias(alias), index(index) {
	}
	virtual ~Binding() = default;

	BindingType type;
	string alias;
	index_t index;

public:
	virtual bool HasMatchingBinding(const string &column_name) = 0;
	virtual BindResult Bind(ColumnRefExpression &colref, index_t depth) = 0;
	virtual void GenerateAllColumnExpressions(BindContext &context,
	                                          vector<unique_ptr<ParsedExpression>> &select_list) = 0;
};

//! Represents a binding to a base table
struct TableBinding : public Binding {
	TableBinding(const string &alias, BoundBaseTableRef *bound);

	BoundBaseTableRef *bound;

public:
	bool HasMatchingBinding(const string &column_name) override;
	BindResult Bind(ColumnRefExpression &colref, index_t depth) override;
	void GenerateAllColumnExpressions(BindContext &context, vector<unique_ptr<ParsedExpression>> &select_list) override;
};

//! Represents a binding to a subquery
struct SubqueryBinding : public Binding {
	SubqueryBinding(const string &alias, SubqueryRef &ref, BoundQueryNode &subquery, index_t index);

	BoundQueryNode &subquery;
	//! Column names of the subquery
	vector<string> names;
	//! Name -> index for the names
	unordered_map<string, uint64_t> name_map;

public:
	bool HasMatchingBinding(const string &column_name) override;
	BindResult Bind(ColumnRefExpression &colref, index_t depth) override;
	void GenerateAllColumnExpressions(BindContext &context, vector<unique_ptr<ParsedExpression>> &select_list) override;

private:
	void AddName(string &name);
};

//! Represents a binding to a table-producing function
struct TableFunctionBinding : public Binding {
	TableFunctionBinding(const string &alias, TableFunctionCatalogEntry *function, index_t index);

	bool HasMatchingBinding(const string &column_name) override;
	BindResult Bind(ColumnRefExpression &colref, index_t depth) override;
	void GenerateAllColumnExpressions(BindContext &context, vector<unique_ptr<ParsedExpression>> &select_list) override;

	TableFunctionCatalogEntry *function;
};

} // namespace duckdb
