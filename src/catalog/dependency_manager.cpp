#include "duckdb/catalog/dependency_manager.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/catalog/mapping_value.hpp"
#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/common/queue.hpp"

namespace duckdb {

DependencyManager::DependencyManager(DuckCatalog &catalog) : catalog(catalog) {
}

void DependencyManager::AddObject(CatalogTransaction transaction, CatalogEntry &object, DependencyList &dependencies) {
	// check for each object in the sources if they were not deleted yet
	for (auto &dep : dependencies.set) {
		auto &dependency = dep.get();
		if (&dependency.ParentCatalog() != &object.ParentCatalog()) {
			throw DependencyException(
			    "Error adding dependency for object \"%s\" - dependency \"%s\" is in catalog "
			    "\"%s\", which does not match the catalog \"%s\".\nCross catalog dependencies are not supported.",
			    object.name, dependency.name, dependency.ParentCatalog().GetName(), object.ParentCatalog().GetName());
		}
		if (!dependency.set) {
			throw InternalException("Dependency has no set");
		}
		auto catalog_entry = dependency.set->GetEntryInternal(transaction, dependency.name, nullptr);
		if (!catalog_entry) {
			throw InternalException("Dependency has already been deleted?");
		}
	}
	// indexes do not require CASCADE to be dropped, they are simply always dropped along with the table
	auto dependency_type = object.type == CatalogType::INDEX_ENTRY ? DependencyType::DEPENDENCY_AUTOMATIC
	                                                               : DependencyType::DEPENDENCY_REGULAR;
	// add the object to the dependents_map of each object that it depends on
	for (auto &dependency : dependencies.set) {
		auto &set = dependents_map[dependency];
		set.insert(Dependency(object, dependency_type));
	}
	// create the dependents map for this object: it starts out empty
	dependents_map[object] = dependency_set_t();
	dependencies_map[object] = dependencies.set;
}

void DependencyManager::DropObject(CatalogTransaction transaction, CatalogEntry &object, bool cascade) {
	D_ASSERT(dependents_map.find(object) != dependents_map.end());

	// first check the objects that depend on this object
	auto &dependent_objects = dependents_map[object];
	for (auto &dep : dependent_objects) {
		// look up the entry in the catalog set
		auto &entry = dep.entry.get();
		auto &catalog_set = *entry.set;
		auto mapping_value = catalog_set.GetMapping(transaction, entry.name, true /* get_latest */);
		if (mapping_value == nullptr) {
			continue;
		}
		auto dependency_entry = catalog_set.GetEntryInternal(transaction, mapping_value->index);
		if (!dependency_entry) {
			// the dependent object was already deleted, no conflict
			continue;
		}
		// conflict: attempting to delete this object but the dependent object still exists
		if (cascade || dep.dependency_type == DependencyType::DEPENDENCY_AUTOMATIC ||
		    dep.dependency_type == DependencyType::DEPENDENCY_OWNS) {
			// cascade: drop the dependent object
			catalog_set.DropEntryInternal(transaction, mapping_value->index.Copy(), *dependency_entry, cascade);
		} else {
			// no cascade and there are objects that depend on this object: throw error
			throw DependencyException("Cannot drop entry \"%s\" because there are entries that "
			                          "depend on it. Use DROP...CASCADE to drop all dependents.",
			                          object.name);
		}
	}
}

void DependencyManager::AlterObject(CatalogTransaction transaction, CatalogEntry &old_obj, CatalogEntry &new_obj) {
	D_ASSERT(dependents_map.find(old_obj) != dependents_map.end());
	D_ASSERT(dependencies_map.find(old_obj) != dependencies_map.end());

	// first check the objects that depend on this object
	catalog_entry_vector_t owned_objects_to_add;
	auto &dependent_objects = dependents_map[old_obj];
	for (auto &dep : dependent_objects) {
		// look up the entry in the catalog set
		auto &entry = dep.entry.get();
		auto &catalog_set = *entry.set;
		auto dependency_entry = catalog_set.GetEntryInternal(transaction, entry.name, nullptr);
		if (!dependency_entry) {
			// the dependent object was already deleted, no conflict
			continue;
		}
		if (dep.dependency_type == DependencyType::DEPENDENCY_OWNS) {
			// the dependent object is owned by the current object
			owned_objects_to_add.push_back(dep.entry);
			continue;
		}
		// conflict: attempting to alter this object but the dependent object still exists
		// no cascade and there are objects that depend on this object: throw error
		throw DependencyException("Cannot alter entry \"%s\" because there are entries that "
		                          "depend on it.",
		                          old_obj.name);
	}
	// add the new object to the dependents_map of each object that it depends on
	auto &old_dependencies = dependencies_map[old_obj];
	for (auto &dep : old_dependencies) {
		auto &dependency = dep.get();
		dependents_map[dependency].insert(new_obj);
	}

	// We might have to add a type dependency
	// add the new object to the dependency manager
	dependents_map[new_obj] = dependency_set_t();
	dependencies_map[new_obj] = old_dependencies;

	for (auto &dependency : owned_objects_to_add) {
		dependents_map[new_obj].insert(Dependency(dependency, DependencyType::DEPENDENCY_OWNS));
		dependents_map[dependency].insert(Dependency(new_obj, DependencyType::DEPENDENCY_OWNED_BY));
		dependencies_map[new_obj].insert(dependency);
	}
}

void DependencyManager::EraseObject(CatalogEntry &object) {
	// obtain the writing lock
	EraseObjectInternal(object);
}

optional_ptr<catalog_entry_set_t> DependencyManager::GetEntriesThatObjectDependsOn(CatalogEntry &object) {
	auto entry = dependencies_map.find(object);
	if (entry == dependencies_map.end()) {
		return nullptr;
	}
	return &entry->second;
}

optional_ptr<dependency_set_t> DependencyManager::GetEntriesThatDependOnObject(CatalogEntry &object) {
	auto entry = dependents_map.find(object);
	if (entry == dependents_map.end()) {
		return nullptr;
	}
	return &entry->second;
}

void DependencyManager::EraseObjectInternal(CatalogEntry &object) {
	if (!GetEntriesThatDependOnObject(object)) {
		// dependencies already removed
		return;
	}

	D_ASSERT(dependents_map.find(object) != dependents_map.end());

	auto entries = GetEntriesThatObjectDependsOn(object);
	D_ASSERT(entries);
	for (auto &other : *entries) {
		// For every entry that 'object' is dependent on, clean up this connection
		auto dependencies = GetEntriesThatDependOnObject(other);
		if (!dependencies) {
			continue;
		}

		auto dependent_entry = dependencies->find(object);
		D_ASSERT(dependent_entry != dependencies->end());

		// Remove the dependency of 'object' on 'other'
		dependencies->erase(dependent_entry);
	}
	// erase the dependents and dependencies for this object
	dependents_map.erase(object);
	dependencies_map.erase(object);
}

void DependencyManager::PrintDependencyMap() {
	Printer::Print("DEPENDENCIES_MAP");
	for (auto &entry : dependencies_map) {
		Printer::Print(entry.first.get().ToSQL());
		for (auto &other : entry.second) {
			Printer::Print("\t" + other.get().ToSQL());
		}
	}
}

void DependencyManager::PrintDependentsMap() {
	Printer::Print("DEPENDENTS_MAP");
	for (auto &entry : dependents_map) {
		Printer::Print(entry.first.get().ToSQL());
		for (auto &other : entry.second) {
			Printer::Print("\t" + other.entry.get().ToSQL());
		}
	}
}

bool DependencyManager::AllExportDependenciesWritten(CatalogEntry &object, catalog_entry_set_t &dependencies,
                                                     catalog_entry_set_t &exported) {
	for (auto &entry : dependencies) {
		// This is an entry that needs to be written before 'object' can be written
		if (exported.find(entry) == exported.end()) {
			// It has not been written yet, abort
			return false;
		}
		// We do not need to check recursively, if the object is written
		// that means that the objects it depends on have also been written
	}
	return true;
}

catalog_entry_vector_t DependencyManager::GetExportOrder() {
	catalog_entry_set_t entries;
	catalog_entry_vector_t export_order;

	queue<reference<CatalogEntry>> backlog;
	// Populate the backlog with every entry in the dependencies map
	for (auto &entry : dependencies_map) {
		backlog.push(entry.first);
	}

	// First populate our backlog with every entry in dependencies_map
	while (!backlog.empty()) {
		auto &object = backlog.front();
		backlog.pop();
		if (entries.count(object)) {
			// This entry has already been written
			continue;
		}
		auto entry = dependencies_map.find(object);
		if (entry == dependencies_map.end() || AllExportDependenciesWritten(object, entry->second, entries)) {
			// All dependencies written, we can write this now
			auto insert_result = entries.insert(object);
			(void)insert_result;
			D_ASSERT(insert_result.second);
			export_order.push_back(object);
		} else {
			for (auto &dependency : entry->second) {
				backlog.emplace(dependency);
			}
			backlog.emplace(object);
		}
	}
	return export_order;
}

void DependencyManager::Scan(const std::function<void(CatalogEntry &, CatalogEntry &, DependencyType)> &callback) {
	lock_guard<mutex> write_lock(catalog.GetWriteLock());
	for (auto &entry : dependents_map) {
		for (auto &dependent : entry.second) {
			callback(entry.first, dependent.entry, dependent.dependency_type);
		}
	}
}

void DependencyManager::AddOwnership(CatalogTransaction transaction, CatalogEntry &owner, CatalogEntry &entry) {
	// lock the catalog for writing
	lock_guard<mutex> write_lock(catalog.GetWriteLock());

	// If the owner is already owned by something else, throw an error
	for (auto &dep : dependents_map[owner]) {
		if (dep.dependency_type == DependencyType::DEPENDENCY_OWNED_BY) {
			throw DependencyException(owner.name + " already owned by " + dep.entry.get().name);
		}
	}

	// If the entry is already owned, throw an error
	for (auto &dep : dependents_map[entry]) {
		// if the entry is already owned, throw error
		if (&dep.entry.get() != &owner) {
			throw DependencyException(entry.name + " already depends on " + dep.entry.get().name);
		}
		// if the entry owns the owner, throw error
		if (&dep.entry.get() == &owner && dep.dependency_type == DependencyType::DEPENDENCY_OWNS) {
			throw DependencyException(entry.name + " already owns " + owner.name +
			                          ". Cannot have circular dependencies");
		}
	}

	// Emplace guarantees that the same object cannot be inserted twice in the unordered_set
	// In the case AddOwnership is called twice, because of emplace, the object will not be repeated in the set.
	// We use an automatic dependency because if the Owner gets deleted, then the owned objects are also deleted
	dependents_map[owner].emplace(entry, DependencyType::DEPENDENCY_OWNS);
	dependents_map[entry].emplace(owner, DependencyType::DEPENDENCY_OWNED_BY);
	dependencies_map[owner].emplace(entry);
}

} // namespace duckdb
