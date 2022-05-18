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
	void AddGeneratedColumn(ColumnDefinition& column);
	void RemoveColumn(ColumnDefinition& column);
	void RenameColumn(TableColumnType category, string old_name, string new_name);

	bool IsDependencyOf(string gcol, string col) const;
	bool HasDependencies(string col) const;

	const unordered_set<string>& GetDependencies(string name) const;

private:
	void RemoveStandardColumn(ColumnDefinition& column);
	void RemoveGeneratedColumn(ColumnDefinition& column);

	void InnerRenameColumn(case_insensitive_map_t<unordered_set<string>>& dependents,
	    case_insensitive_map_t<unordered_set<string>>& dependencies, string old_name, string new_name);
private:
	//! A map of column dependency to generated column(s)
	case_insensitive_map_t<unordered_set<string>> dependencies;
	//! A map of generated column name to (potentially generated)column dependencies
	case_insensitive_map_t<unordered_set<string>> dependents;
};

} //namespace duckdb
