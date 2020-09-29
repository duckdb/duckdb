#include "duckdb/catalog/dependency_manager.hpp"

#include "duckdb/catalog/catalog.hpp"

namespace duckdb {
using namespace std;

Dependency::Dependency(CatalogEntry *entry) {
	this->set = entry->set;
	this->entry_index = set->GetEntryIndex(entry);
}

idx_t Dependency::Hash() const {
	return entry_index;
}

DependencyManager::DependencyManager(Catalog &catalog) : catalog(catalog) {
}

void DependencyManager::AddObject(Transaction &transaction, CatalogEntry *object,
                                  unordered_set<CatalogEntry *> &dependency_entries) {
	Dependency object_dependency(object);
	// check for each object in the sources if they were not deleted yet
	for (auto &dependency : dependency_entries) {
		if (CatalogSet::HasConflict(transaction, *dependency)) {
			// transaction conflict with this entry
			throw TransactionException("Catalog write-write conflict on create with \"%s\"", object->name);
		}
	}
	// add the object to the dependents_map of each object that it depends on
	dependency_set_t dependencies;
	for (auto &dependency : dependency_entries) {
		Dependency dep(dependency);
		dependents_map[dep].insert(object_dependency);
		dependencies.insert(dep);
	}
	// create the dependents map for this object: it starts out empty
	dependents_map[object_dependency] = dependency_set_t();
	dependencies_map[object_dependency] = move(dependencies);
	reference_count[object_dependency]++;
}

void DependencyManager::DropObject(Transaction &transaction, CatalogEntry *object, bool cascade,
                                   set_lock_map_t &lock_set) {
	assert(dependents_map.find(object) != dependents_map.end());

	// first check the objects that depend on this object
	auto &dependent_objects = dependents_map[object];
	for (auto &dep : dependent_objects) {
		// look up the entry in the catalog set
		CatalogEntry *entry;
		auto &catalog_set = *dep.set;
		if (!catalog_set.GetEntryInternal(transaction, dep.entry_index, entry)) {
			continue;
		}
		// conflict: attempting to delete this object but the dependent object still exists
		if (cascade) {
			// cascade: drop the dependent object
			catalog_set.DropEntryInternal(transaction, dep.entry_index, *entry, cascade, lock_set);
		} else {
			// no cascade and there are objects that depend on this object: throw error
			throw CatalogException("Cannot drop entry \"%s\" because there are entries that "
			                       "depend on it. Use DROP...CASCADE to drop all dependents.",
			                       object->name);
		}
	}
}

void DependencyManager::AlterObject(Transaction &transaction, CatalogEntry *object) {
	Dependency dep(object);
	assert(dependents_map.find(dep) != dependents_map.end());

	// first check the objects that depend on this object
	auto &dependent_objects = dependents_map[dep];
	for (auto &dep : dependent_objects) {
		// look up the entry in the catalog set
		CatalogEntry *entry;
		auto &catalog_set = *dep.set;
		if (!catalog_set.GetEntryInternal(transaction, dep.entry_index, entry)) {
			continue;
		}
		throw CatalogException("Cannot alter entry \"%s\" because there are entries that depend on it.",
								object->name);
	}
	reference_count[dep]++;
}

void DependencyManager::EraseObject(CatalogEntry *catalog_entry) {
	// obtain the writing lock
	lock_guard<mutex> write_lock(catalog.write_lock);
	Dependency object(catalog_entry);
	auto entry = reference_count.find(object);
	assert(entry != reference_count.end());
	entry->second--;
	if (entry->second == 0) {
		EraseObjectInternal(catalog_entry);
	}
}

void DependencyManager::EraseObjectInternal(CatalogEntry *entry) {
	Dependency object(entry);
	if (dependents_map.find(object) == dependents_map.end()) {
		// dependencies already removed
		return;
	}
	assert(dependents_map.find(object) != dependents_map.end());
	assert(dependencies_map.find(object) != dependencies_map.end());
	// now for each of the dependencies, erase the entries from the dependents_map
	for (auto &dependency : dependencies_map[object]) {
		auto entry = dependents_map.find(dependency);
		if (entry != dependents_map.end()) {
			assert(entry->second.find(object) != entry->second.end());
			entry->second.erase(object);
		}
	}
	// erase the dependents and dependencies for this object
	dependents_map.erase(object);
	dependencies_map.erase(object);
}

void DependencyManager::ClearDependencies(CatalogSet &set) {
	// obtain the writing lock
	lock_guard<mutex> write_lock(catalog.write_lock);

	// iterate over the objects in the CatalogSet
	for (auto &entry : set.entries) {
		CatalogEntry *centry = entry.second.get();
		while (centry) {
			EraseObjectInternal(centry);
			centry = centry->child.get();
		}
	}
}

} // namespace duckdb
