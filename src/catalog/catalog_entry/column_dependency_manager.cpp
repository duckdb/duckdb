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
	auto &list = dependents[column.name];
	for (auto &dep : deps) {
		list.insert(dep);
		dependencies[dep].insert(column.name);
	}
}

void ColumnDependencyManager::RemoveColumn(ColumnDefinition &column) {
	switch (column.category) {
	case TableColumnType::GENERATED: {
		RemoveGeneratedColumn(column);
		break;
	}
	case TableColumnType::STANDARD: {
		RemoveStandardColumn(column);
		break;
	}
	default: {
		throw NotImplementedException("RemoveColumn not implemented for this TableColumnType");
	}
	}
}

bool ColumnDependencyManager::IsDependencyOf(string gcol, string col) const {
	auto entry = dependents.find(gcol);
	if (entry == dependents.end()) {
		return false;
	}
	auto &list = entry->second;
	return list.count(col);
}

bool ColumnDependencyManager::HasDependencies(string name) const {
	auto entry = dependents.find(name);
	if (entry == dependents.end()) {
		return false;
	}
	return true;
}

const unordered_set<string> &ColumnDependencyManager::GetDependencies(string name) const {
	auto entry = dependents.find(name);
	D_ASSERT(entry != dependents.end());
	return entry->second;
}

bool ColumnDependencyManager::HasDependents(string name) const {
	auto entry = dependencies.find(name);
	if (entry == dependencies.end()) {
		return false;
	}
	return true;
}

const unordered_set<string> &ColumnDependencyManager::GetDependents(string name) const {
	auto entry = dependencies.find(name);
	D_ASSERT(entry != dependencies.end());
	return entry->second;
}

void ColumnDependencyManager::RenameColumn(TableColumnType category, string old_name, string new_name) {
	switch (category) {
	case TableColumnType::GENERATED: {
		InnerRenameColumn(dependents, dependencies, old_name, new_name);
		break;
	}
	case TableColumnType::STANDARD: {
		InnerRenameColumn(dependencies, dependents, old_name, new_name);
		break;
	}
	default: {
		throw NotImplementedException("RenameColumn not implemented for this TableColumnType");
	}
	}
}

void ColumnDependencyManager::RemoveStandardColumn(ColumnDefinition &column) {
	auto &list = dependencies[column.name];
	for (auto &gcol : list) {
		// Remove this column from the dependencies list of this generated column
		dependents[gcol].erase(column.name);
	}
	// Remove this column from the dependencies map
	dependencies.erase(column.name);
}

void ColumnDependencyManager::RemoveGeneratedColumn(ColumnDefinition &column) {
	auto &list = dependents[column.name];
	for (auto &col : list) {
		// Remove this generated column from the list of this column
		dependencies[col].erase(column.name);
		// If the resulting list is empty, remove the column from the dependencies map altogether
		if (dependencies[col].empty()) {
			dependencies.erase(col);
		}
	}
}

// Used for both generated and standard column, to avoid code duplication
void ColumnDependencyManager::InnerRenameColumn(case_insensitive_map_t<unordered_set<string>> &dependents,
                                                case_insensitive_map_t<unordered_set<string>> &dependencies,
                                                string old_name, string new_name) {
	auto &list = dependents[old_name];
	for (auto &col : list) {
		auto &deps = dependencies[col];
		// Replace the old name with the new name
		deps.erase(old_name);
		deps.insert(new_name);
	}
	// Move the list of columns that connect to this column
	dependents[new_name] = move(dependents[old_name]);
	dependents.erase(old_name);
}

// FIXME: if dependencies are copied on create, we dont need this ?
unordered_set<string> ColumnDependencyManager::GetDependencyChain(string col) const {
	unordered_set<string> chain;
	queue<string> to_check;

	// Start by checking the column
	to_check.push(move(col));
	while (!to_check.empty()) {
		auto column = move(to_check.front());
		to_check.pop();

		// Add the dependents to the list of columns to check
		if (HasDependents(column)) {
			auto &col_dependents = GetDependents(column);
			for (auto &dep : col_dependents) {
				to_check.push(dep);
			}
		}
		// Finally add the dependent column to the set
		chain.insert(move(column));
	}
	return chain;
}

} // namespace duckdb
