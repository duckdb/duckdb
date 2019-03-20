//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/table_binding.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"
#include "parser/column_definition.hpp"


namespace duckdb {
class BoundBaseTableRef;
class QueryNode;
class SubqueryRef;
class TableCatalogEntry;
class TableFunctionCatalogEntry;

enum class BindingType : uint8_t {
	DUMMY = 0,
	TABLE = 1,
	SUBQUERY = 2,
	TABLE_FUNCTION = 3
};

//! A Binding represents a binding to a table, table-producing function or subquery with a specified table index. Used in the binder.
struct Binding {
	Binding(BindingType type, size_t index) :
		type(type), index(index) {
	}
	virtual ~Binding() {}

	BindingType type;
	size_t index;
};

//! Represents a binding to a base table
struct TableBinding : public Binding {
	TableBinding(BoundBaseTableRef *bound);

	BoundBaseTableRef *bound;
};

//! Represents a binding to a subquery
struct SubqueryBinding : public Binding {
	SubqueryBinding(SubqueryRef &subquery_, size_t index);
	SubqueryBinding(QueryNode *select_, size_t index);

	QueryNode *subquery;
	//! Column names of the subquery
	vector<string> names;
	//! Name -> index for the names
	unordered_map<string, size_t> name_map;
};

//! Represents a binding to a table-producing function
struct TableFunctionBinding : public Binding {
	TableFunctionBinding(TableFunctionCatalogEntry *function, size_t index);

	TableFunctionCatalogEntry *function;
};

//! Represents a dummy binding created from the set of columns
struct DummyTableBinding : public Binding {
	DummyTableBinding(vector<ColumnDefinition> &columns);

	unordered_map<string, ColumnDefinition *> bound_columns;
};

}