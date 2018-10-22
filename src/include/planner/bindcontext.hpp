//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// planner/bindcontext.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "catalog/catalog.hpp"
#include "catalog/catalog_entry/table_catalog_entry.hpp"
#include "catalog/catalog_entry/table_function_catalog_entry.hpp"

#include "parser/column_definition.hpp"
#include "parser/expression.hpp"
#include "parser/sql_statement.hpp"
#include "parser/statement/select_statement.hpp"

namespace duckdb {

enum class BindingType : uint8_t {
	DUMMY = 0,
	TABLE = 1,
	SUBQUERY = 2,
	TABLE_FUNCTION = 3
};

struct Binding {
	Binding(BindingType type, size_t index) : type(type), index(index) {
	}
	virtual ~Binding() {
	}

	BindingType type;
	size_t index;
};

struct DummyTableBinding : public Binding {
	std::unordered_map<std::string, ColumnDefinition *> bound_columns;

	DummyTableBinding(std::vector<ColumnDefinition> &columns)
	    : Binding(BindingType::DUMMY, 0) {
		for (auto &it : columns) {
			bound_columns[it.name] = &it;
		}
	}
	virtual ~DummyTableBinding() {
	}
};

struct TableBinding : public Binding {
	TableCatalogEntry *table;

	TableBinding(TableCatalogEntry *table, size_t index)
	    : Binding(BindingType::TABLE, index), table(table) {
	}
	virtual ~TableBinding() {
	}
};

struct SubqueryBinding : public Binding {
	SelectStatement *subquery;
	//! Column names of the subquery
	std::vector<std::string> names;
	//! Name -> index for the names
	std::unordered_map<std::string, size_t> name_map;

	SubqueryBinding(SelectStatement *subquery_, size_t index)
	    : Binding(BindingType::SUBQUERY, index), subquery(subquery_) {
		for (auto &entry : subquery->select_list) {
			auto name = entry->GetName();
			name_map[name] = names.size();
			names.push_back(name);
		}
	}
	virtual ~SubqueryBinding() {
	}
};

struct TableFunctionBinding : public Binding {
	TableFunctionCatalogEntry *function;

	TableFunctionBinding(TableFunctionCatalogEntry *function, size_t index)
	    : Binding(BindingType::TABLE_FUNCTION, index), function(function) {
	}
};

//! The BindContext object keeps track of all the tables and columns that are
//! encountered during the binding process.
class BindContext {
  public:
	BindContext() : bound_tables(0), max_depth(0) {
	}

	//! Given a column name, find the matching table it belongs to. Throws an
	//! exception if no table has a column of the given name.
	std::string GetMatchingBinding(const std::string &column_name);
	//! Binds a column expression to the base table. Returns the column catalog
	//! entry or throws an exception if the column could not be bound.
	void BindColumn(ColumnRefExpression &expr, size_t depth = 0);

	//! Generate column expressions for all columns that are present in the
	//! referenced tables. This is used to resolve the * expression in a
	//! selection list.
	void GenerateAllColumnExpressions(
	    std::vector<std::unique_ptr<Expression>> &new_select_list);

	//! Adds a base table with the given alias to the BindContext.
	void AddBaseTable(const std::string &alias, TableCatalogEntry *table_entry);
	//! Adds a dummy table with the given set of columns to the BindContext.
	void AddDummyTable(const std::string &alias,
	                   std::vector<ColumnDefinition> &columns);
	//! Adds a subquery with a given alias to the BindContext.
	void AddSubquery(const std::string &alias, SelectStatement *subquery);
	//! Adds a table function with a given alias to the BindContext
	void AddTableFunction(const std::string &alias,
	                      TableFunctionCatalogEntry *function_entry);
	//! Adds an expression that has an alias to the BindContext
	void AddExpression(const std::string &alias, Expression *expression,
	                   size_t i);

	//! Returns true if the table/subquery alias exists, false otherwise.
	bool HasAlias(const std::string &alias);
	//! Gets the binding index of a given alias. Throws an exception if the
	//! alias was not found.
	size_t GetBindingIndex(const std::string &alias);
	//! Returns the maximum depth of column references in the current context
	size_t GetMaxDepth() {
		return max_depth;
	}

	//! The set of columns that are bound for each table/subquery alias
	std::unordered_map<std::string, std::vector<std::string>> bound_columns;

	BindContext *parent = nullptr;

  private:
	void AddBinding(const std::string &alias, std::unique_ptr<Binding> binding);

	size_t GenerateTableIndex();

	size_t bound_tables;
	size_t max_depth;

	//! The set of expression aliases
	std::unordered_map<std::string, std::pair<size_t, Expression *>>
	    expression_alias_map;
	//! The set of bindings
	std::unordered_map<std::string, std::unique_ptr<Binding>> bindings;
	//! The list of bindings in insertion order
	std::vector<std::pair<std::string, Binding *>> bindings_list;
};
} // namespace duckdb
