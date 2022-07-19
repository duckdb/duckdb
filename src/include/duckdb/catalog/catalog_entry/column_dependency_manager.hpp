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
#include "duckdb/common/stack.hpp"

namespace duckdb {

//! Dependency Manager local to a table, responsible for keeping track of generated column dependencies

class ColumnDependencyManager {
public:
	DUCKDB_API ColumnDependencyManager();
	DUCKDB_API ~ColumnDependencyManager();
	DUCKDB_API ColumnDependencyManager(ColumnDependencyManager &&other) = default;
	ColumnDependencyManager(const ColumnDependencyManager &other) = delete;

public:
	//! Get the bind order that ensures dependencies are resolved before dependents are
	stack<column_t> GetBindOrder(const vector<ColumnDefinition> &columns);

	//! Adds a connection between the dependent and its dependencies
	void AddGeneratedColumn(column_t index, const vector<column_t> &indices, bool root = true);
	//! Add a generated column from a column definition
	void AddGeneratedColumn(const ColumnDefinition &column, const case_insensitive_map_t<column_t> &name_map);

	//! Removes the column(s) and outputs the new column indices
	vector<column_t> RemoveColumn(column_t index, column_t column_amount);

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
	vector<column_t> CleanupInternals(column_t column_amount);

private:
	//! A map of column dependency to generated column(s)
	unordered_map<column_t, unordered_set<column_t>> dependencies_map;
	//! A map of generated column name to (potentially generated)column dependencies
	unordered_map<column_t, unordered_set<column_t>> dependents_map;
	//! For resolve-order purposes, keep track of the 'direct' (not inherited) dependencies of a generated column
	unordered_map<column_t, unordered_set<column_t>> direct_dependencies;
	set<column_t> deleted_columns;
};

} // namespace duckdb
