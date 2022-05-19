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
#include "duckdb/common/set.hpp"

namespace duckdb {

//! Dependency Manager local to a table, responsible for keeping track of generated column dependencies

class ColumnDependencyManager {
public:
	ColumnDependencyManager();

public:
	//! Adds a connection between the dependent and its dependencies
	void AddGeneratedColumn(ColumnDefinition &column, vector<column_t> indices);
	void RemoveColumn(column_t index);
	// void RenameColumn(TableColumnType category, const string &old_name, const string &new_name);

	bool IsDependencyOf(column_t dependent, column_t dependency) const;
	bool HasDependencies(column_t index) const;
	const unordered_set<column_t> &GetDependencies(column_t index) const;

	bool HasDependents(column_t index) const;
	const unordered_set<column_t> &GetDependents(column_t index) const;

private:
	void RemoveStandardColumn(column_t index);
	void RemoveGeneratedColumn(column_t index);

	void AdjustSingle(column_t idx, idx_t offset);
	// Clean up the gaps created by a Remove operation
	void CleanupInternals();

private:
	//! A map of column dependency to generated column(s)
	unordered_map<column_t, unordered_set<column_t>> dependencies_map;
	//! A map of generated column name to (potentially generated)column dependencies
	unordered_map<column_t, unordered_set<column_t>> dependents_map;
	set<column_t> deleted_columns;
};

} // namespace duckdb
