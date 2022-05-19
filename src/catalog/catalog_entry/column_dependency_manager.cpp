#include "duckdb/catalog/catalog_entry/column_dependency_manager.hpp"
#include "duckdb/parser/column_definition.hpp"

#include <queue>

namespace duckdb {

using std::queue;

ColumnDependencyManager::ColumnDependencyManager() {
}

void ColumnDependencyManager::AddGeneratedColumn(ColumnDefinition &column) {
	D_ASSERT(column.Generated());

	vector<string> deps;
	column.GetListOfDependencies(deps);
	if (deps.empty()) {
		return;
	}
	auto &list = dependents_map[column.name];
	for (auto &dep : deps) {
		// Add this column as a dependency of the new column
		list.insert(dep);
		// Add the new column as a dependent of the column
		dependencies_map[dep].insert(column.name);
		// Inherit the dependencies
		if (HasDependencies(dep)) {
			auto &inherited_deps = dependents_map[dep];
			D_ASSERT(!inherited_deps.empty());
			for (auto &inherited_dep : inherited_deps) {
				list.insert(inherited_dep);
				dependencies_map[inherited_dep].insert(column.name);
			}
		}
	}
}

void ColumnDependencyManager::RemoveColumn(ColumnDefinition &column) {
	switch (column.category) {
	case TableColumnType::GENERATED: {
		RemoveGeneratedColumn(column.name);
		break;
	}
	case TableColumnType::STANDARD: {
		RemoveStandardColumn(column.name);
		break;
	}
	default: {
		throw NotImplementedException("RemoveColumn not implemented for this TableColumnType");
	}
	}
}

bool ColumnDependencyManager::IsDependencyOf(const string &gcol, const string &col) const {
	auto entry = dependents_map.find(gcol);
	if (entry == dependents_map.end()) {
		return false;
	}
	auto &list = entry->second;
	return list.count(col);
}

bool ColumnDependencyManager::HasDependencies(const string &name) const {
	auto entry = dependents_map.find(name);
	if (entry == dependents_map.end()) {
		return false;
	}
	return true;
}

const unordered_set<string> &ColumnDependencyManager::GetDependencies(const string &name) const {
	auto entry = dependents_map.find(name);
	D_ASSERT(entry != dependents_map.end());
	return entry->second;
}

bool ColumnDependencyManager::HasDependents(const string &name) const {
	auto entry = dependencies_map.find(name);
	if (entry == dependencies_map.end()) {
		return false;
	}
	return true;
}

const unordered_set<string> &ColumnDependencyManager::GetDependents(const string &name) const {
	auto entry = dependencies_map.find(name);
	D_ASSERT(entry != dependencies_map.end());
	return entry->second;
}

void ColumnDependencyManager::RenameColumn(TableColumnType category, const string &old_name, const string &new_name) {
	switch (category) {
	case TableColumnType::GENERATED: {
		InnerRenameColumn(dependents_map, dependencies_map, old_name, new_name);
		break;
	}
	case TableColumnType::STANDARD: {
		InnerRenameColumn(dependencies_map, dependents_map, old_name, new_name);
		break;
	}
	default: {
		throw NotImplementedException("RenameColumn not implemented for this TableColumnType");
	}
	}
}

void ColumnDependencyManager::RemoveStandardColumn(const string &name) {
	if (!HasDependents(name)) {
		return;
	}
	auto dependents = dependencies_map[name];
	for (auto &gcol : dependents) {
		RemoveGeneratedColumn(gcol);
	}
	// Remove this column from the dependencies map
	dependencies_map.erase(name);
}

void ColumnDependencyManager::RemoveGeneratedColumn(const string &name) {
	auto &dependencies = dependents_map[name];
	for (auto &col : dependencies) {
		// Remove this generated column from the list of this column
		auto &col_dependents = dependencies_map[col];
		D_ASSERT(col_dependents.count(name));
		col_dependents.erase(name);
		// If the resulting list is empty, remove the column from the dependencies map altogether
		if (col_dependents.empty()) {
			dependencies_map.erase(col);
		}
	}
	// Remove this column from the dependents_map map
	dependents_map.erase(name);
	// Treat it as a standard column now, to remove the (potential) dependents
	RemoveStandardColumn(name);
}

// Used for both generated and standard column, to avoid code duplication
void ColumnDependencyManager::InnerRenameColumn(case_insensitive_map_t<unordered_set<string>> &dependents_map,
                                                case_insensitive_map_t<unordered_set<string>> &dependencies_map,
                                                const string &old_name, const string &new_name) {
	auto &list = dependents_map[old_name];
	for (auto &col : list) {
		auto &deps = dependencies_map[col];
		// Replace the old name with the new name
		deps.erase(old_name);
		deps.insert(new_name);
	}
	// Move the list of columns that connect to this column
	dependents_map[new_name] = move(dependents_map[old_name]);
	dependents_map.erase(old_name);
}

} // namespace duckdb
