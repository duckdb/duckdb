#include "duckdb/catalog/dependency_manager.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/catalog/mapping_value.hpp"
#include "duckdb/catalog/dependency_list.hpp"

namespace duckdb {

DependencyManager::DependencyManager(Catalog &catalog) : catalog(catalog) {
}

void DependencyManager::AddObject(CatalogTransaction transaction, CatalogEntry *object, DependencyList &dependencies) {
	// check for each object in the sources if they were not deleted yet
	for (auto &dependency : dependencies.set) {
		CatalogEntry *catalog_entry;
		if (dependency->catalog != object->catalog) {
			throw DependencyException(
			    "Error adding dependency for object \"%s\" - dependency \"%s\" is in catalog "
			    "\"%s\", which does not match the catalog \"%s\".\nCross catalog dependencies are not supported.",
			    object->name, dependency->name, dependency->catalog->GetName(), object->catalog->GetName());
		}
		if (!dependency->set) {
			throw InternalException("Dependency has no set");
		}
		if (!dependency->set->GetEntryInternal(transaction, dependency->name, nullptr, catalog_entry)) {
			throw InternalException("Dependency has already been deleted?");
		}
	}
	// indexes do not require CASCADE to be dropped, they are simply always dropped along with the table
	auto dependency_type = object->type == CatalogType::INDEX_ENTRY ? DependencyType::DEPENDENCY_AUTOMATIC
	                                                                : DependencyType::DEPENDENCY_REGULAR;
	// add the object to the dependents_map of each object that it depends on
	for (auto &dependency : dependencies.set) {
		dependents_map[dependency].insert(Dependency(object, dependency_type));
	}
	// create the dependents map for this object: it starts out empty
	dependents_map[object] = dependency_set_t();
	dependencies_map[object] = dependencies.set;
}

void DependencyManager::DropObject(CatalogTransaction transaction, CatalogEntry *object, bool cascade) {
	D_ASSERT(dependents_map.find(object) != dependents_map.end());

	// first check the objects that depend on this object
	auto &dependent_objects = dependents_map[object];
	for (auto &dep : dependent_objects) {
		// look up the entry in the catalog set
		auto &catalog_set = *dep.entry->set;
		auto mapping_value = catalog_set.GetMapping(transaction, dep.entry->name, true /* get_latest */);
		if (mapping_value == nullptr) {
			continue;
		}
		CatalogEntry *dependency_entry;

		if (!catalog_set.GetEntryInternal(transaction, mapping_value->index, dependency_entry)) {
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
			                          object->name);
		}
	}
}

void DependencyManager::AlterObject(CatalogTransaction transaction, CatalogEntry *old_obj, CatalogEntry *new_obj) {
	D_ASSERT(dependents_map.find(old_obj) != dependents_map.end());
	D_ASSERT(dependencies_map.find(old_obj) != dependencies_map.end());

	// first check the objects that depend on this object
	vector<CatalogEntry *> owned_objects_to_add;
	auto &dependent_objects = dependents_map[old_obj];
	for (auto &dep : dependent_objects) {
		// look up the entry in the catalog set
		auto &catalog_set = *dep.entry->set;
		CatalogEntry *dependency_entry;
		if (!catalog_set.GetEntryInternal(transaction, dep.entry->name, nullptr, dependency_entry)) {
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
		                          old_obj->name);
	}
	// add the new object to the dependents_map of each object that it depends on
	auto &old_dependencies = dependencies_map[old_obj];
	vector<CatalogEntry *> to_delete;
	for (auto &dependency : old_dependencies) {
		if (dependency->type == CatalogType::TYPE_ENTRY) {
			auto user_type = (TypeCatalogEntry *)dependency;
			auto table = (TableCatalogEntry *)new_obj;
			bool deleted_dependency = true;
			for (auto &column : table->columns.Logical()) {
				if (column.Type() == user_type->user_type) {
					deleted_dependency = false;
					break;
				}
			}
			if (deleted_dependency) {
				to_delete.push_back(dependency);
				continue;
			}
		}
		dependents_map[dependency].insert(new_obj);
	}
	for (auto &dependency : to_delete) {
		old_dependencies.erase(dependency);
		dependents_map[dependency].erase(old_obj);
	}

	// We might have to add a type dependency
	vector<CatalogEntry *> to_add;
	if (new_obj->type == CatalogType::TABLE_ENTRY) {
		auto table = (TableCatalogEntry *)new_obj;
		for (auto &column : table->columns.Logical()) {
			auto user_type_catalog = LogicalType::GetCatalog(column.Type());
			if (user_type_catalog) {
				to_add.push_back(user_type_catalog);
			}
		}
	}
	// add the new object to the dependency manager
	dependents_map[new_obj] = dependency_set_t();
	dependencies_map[new_obj] = old_dependencies;

	for (auto &dependency : to_add) {
		dependencies_map[new_obj].insert(dependency);
		dependents_map[dependency].insert(new_obj);
	}

	for (auto &dependency : owned_objects_to_add) {
		dependents_map[new_obj].insert(Dependency(dependency, DependencyType::DEPENDENCY_OWNS));
		dependents_map[dependency].insert(Dependency(new_obj, DependencyType::DEPENDENCY_OWNED_BY));
		dependencies_map[new_obj].insert(dependency);
	}
}

void DependencyManager::EraseObject(CatalogEntry *object) {
	// obtain the writing lock
	EraseObjectInternal(object);
}

void DependencyManager::EraseObjectInternal(CatalogEntry *object) {
	if (dependents_map.find(object) == dependents_map.end()) {
		// dependencies already removed
		return;
	}
	D_ASSERT(dependents_map.find(object) != dependents_map.end());
	D_ASSERT(dependencies_map.find(object) != dependencies_map.end());
	// now for each of the dependencies, erase the entries from the dependents_map
	for (auto &dependency : dependencies_map[object]) {
		auto entry = dependents_map.find(dependency);
		if (entry != dependents_map.end()) {
			D_ASSERT(entry->second.find(object) != entry->second.end());
			entry->second.erase(object);
		}
	}
	// erase the dependents and dependencies for this object
	dependents_map.erase(object);
	dependencies_map.erase(object);
}

void DependencyManager::Scan(const std::function<void(CatalogEntry *, CatalogEntry *, DependencyType)> &callback) {
	lock_guard<mutex> write_lock(catalog.write_lock);
	for (auto &entry : dependents_map) {
		for (auto &dependent : entry.second) {
			callback(entry.first, dependent.entry, dependent.dependency_type);
		}
	}
}

void DependencyManager::AddOwnership(CatalogTransaction transaction, CatalogEntry *owner, CatalogEntry *entry) {
	// lock the catalog for writing
	lock_guard<mutex> write_lock(catalog.write_lock);

	// If the owner is already owned by something else, throw an error
	for (auto &dep : dependents_map[owner]) {
		if (dep.dependency_type == DependencyType::DEPENDENCY_OWNED_BY) {
			throw DependencyException(owner->name + " already owned by " + dep.entry->name);
		}
	}

	// If the entry is already owned, throw an error
	for (auto &dep : dependents_map[entry]) {
		// if the entry is already owned, throw error
		if (dep.entry != owner) {
			throw DependencyException(entry->name + " already depends on " + dep.entry->name);
		}
		// if the entry owns the owner, throw error
		if (dep.entry == owner && dep.dependency_type == DependencyType::DEPENDENCY_OWNS) {
			throw DependencyException(entry->name + " already owns " + owner->name +
			                          ". Cannot have circular dependencies");
		}
	}

	// Emplace guarantees that the same object cannot be inserted twice in the unordered_set
	// In the case AddOwnership is called twice, because of emplace, the object will not be repeated in the set.
	// We use an automatic dependency because if the Owner gets deleted, then the owned objects are also deleted
	dependents_map[owner].emplace(Dependency(entry, DependencyType::DEPENDENCY_OWNS));
	dependents_map[entry].emplace(Dependency(owner, DependencyType::DEPENDENCY_OWNED_BY));
	dependencies_map[owner].emplace(entry);
}

} // namespace duckdb
