//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry/column_dependency_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/parser/column_definition.hpp"

namespace duckdb {

//! Dependency Manager local to a table, responsible for keeping track of generated column dependencies

class ColumnDependencyManager {
public:
	ColumnDependencyManager();

public:
	//! Adds a connection between the dependent and its dependencies
	void AddGeneratedColumn(ColumnDefinition &column);
	void RemoveColumn(ColumnDefinition &column);
	void RenameColumn(TableColumnType category, const string &old_name, const string &new_name);

	bool IsDependencyOf(const string &gcol, const string &col) const;
	bool HasDependencies(const string &col) const;
	const unordered_set<string> &GetDependencies(const string &name) const;

	bool HasDependents(const string &col) const;
	const unordered_set<string> &GetDependents(const string &name) const;

private:
	void RemoveStandardColumn(const string &name);
	void RemoveGeneratedColumn(const string &name);

	void InnerRenameColumn(case_insensitive_map_t<unordered_set<string>> &dependents_map,
	                       case_insensitive_map_t<unordered_set<string>> &dependencies_map, const string &old_name,
	                       const string &new_name);

private:
	//! A map of column dependency to generated column(s)
	case_insensitive_map_t<unordered_set<string>> dependencies_map;
	//! A map of generated column name to (potentially generated)column dependencies
	case_insensitive_map_t<unordered_set<string>> dependents_map;
};

} // namespace duckdb
