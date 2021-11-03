#include "duckdb/catalog/dependency_manager.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"

namespace duckdb {

DependencyManager::DependencyManager(Catalog &catalog) : catalog(catalog) {
}

void DependencyManager::AddObject(ClientContext &context, CatalogEntry *object,
                                  unordered_set<CatalogEntry *> &dependencies) {
	// check for each object in the sources if they were not deleted yet
	for (auto &dependency : dependencies) {
		idx_t entry_index;
		CatalogEntry *catalog_entry;
		if (!dependency->set->GetEntryInternal(context, dependency->name, entry_index, catalog_entry)) {
			throw InternalException("Dependency has already been deleted?");
		}
	}
	// indexes do not require CASCADE to be dropped, they are simply always dropped along with the table
	auto dependency_type = object->type == CatalogType::INDEX_ENTRY ? DependencyType::DEPENDENCY_AUTOMATIC
	                                                                : DependencyType::DEPENDENCY_REGULAR;
	// add the object to the owns_map of each object that it depends on
	for (auto &dependency : dependencies) {
		owns_map[dependency].insert(Dependency(object, dependency_type));
	}
	// create the dependents map for this object: it starts out empty
	owns_map[object] = dependency_set_t();
	owned_by_map[object] = dependencies;
}

void DependencyManager::DropObject(ClientContext &context, CatalogEntry *object, bool cascade,
                                   set_lock_map_t &lock_set) {
	D_ASSERT(owns_map.find(object) != owns_map.end());

	// first check the objects that depend on this object
	auto &dependent_objects = owns_map[object];
	for (auto &dep : dependent_objects) {
		// look up the entry in the catalog set
		auto &catalog_set = *dep.entry->set;
		auto mapping_value = catalog_set.GetMapping(context, dep.entry->name, true /* get_latest */);
		if (mapping_value == nullptr) {
			continue;
		}
		idx_t entry_index = mapping_value->index;
		CatalogEntry *dependency_entry;

		if (!catalog_set.GetEntryInternal(context, entry_index, dependency_entry)) {
			// the dependent object was already deleted, no conflict
			continue;
		}
		// conflict: attempting to delete this object but the dependent object still exists
		if (cascade || dep.dependency_type == DependencyType::DEPENDENCY_AUTOMATIC) {
			// cascade: drop the dependent object
			catalog_set.DropEntryInternal(context, entry_index, *dependency_entry, cascade, lock_set);
		} else {
			// no cascade and there are objects that depend on this object: throw error
			throw CatalogException("Cannot drop entry \"%s\" because there are entries that "
			                       "depend on it. Use DROP...CASCADE to drop all dependents.",
			                       object->name);
		}
	}
}

void DependencyManager::AlterObject(ClientContext &context, CatalogEntry *old_obj, CatalogEntry *new_obj) {
	D_ASSERT(owns_map.find(old_obj) != owns_map.end());
	D_ASSERT(owned_by_map.find(old_obj) != owned_by_map.end());

	// first check the objects that depend on this object
	auto &dependent_objects = owns_map[old_obj];
	for (auto &dep : dependent_objects) {
		// look up the entry in the catalog set
		auto &catalog_set = *dep.entry->set;
		idx_t entry_index;
		CatalogEntry *dependency_entry;
		if (!catalog_set.GetEntryInternal(context, dep.entry->name, entry_index, dependency_entry)) {
			// the dependent object was already deleted, no conflict
			continue;
		}
		// conflict: attempting to alter this object but the dependent object still exists
		// no cascade and there are objects that depend on this object: throw error
		throw CatalogException("Cannot alter entry \"%s\" because there are entries that "
		                       "depend on it.",
		                       old_obj->name);
	}
	// add the new object to the owns_map of each object that it depends on
	auto &old_dependencies = owned_by_map[old_obj];
	vector<CatalogEntry *> to_delete;
	for (auto &dependency : old_dependencies) {
		if (dependency->type == CatalogType::TYPE_ENTRY) {
			auto user_type = (TypeCatalogEntry *)dependency;
			auto table = (TableCatalogEntry *)new_obj;
			bool deleted_dependency = true;
			for (auto &column : table->columns) {
				if (column.type == *user_type->user_type) {
					deleted_dependency = false;
					break;
				}
			}
			if (deleted_dependency) {
				to_delete.push_back(dependency);
				continue;
			}
		}
		owns_map[dependency].insert(new_obj);
	}
	for (auto &dependency : to_delete) {
		old_dependencies.erase(dependency);
		owns_map[dependency].erase(old_obj);
	}

	// We might have to add a type dependency
	vector<CatalogEntry *> to_add;
	if (new_obj->type == CatalogType::TABLE_ENTRY) {
		auto table = (TableCatalogEntry *)new_obj;
		for (auto &column : table->columns) {
			if (column.type.id() == LogicalTypeId::ENUM) {
				auto enum_type_catalog = EnumType::GetCatalog(column.type);
				if (enum_type_catalog) {
					to_add.push_back(enum_type_catalog);
				}
			}
		}
	}
	// add the new object to the dependency manager
	owns_map[new_obj] = dependency_set_t();
	owned_by_map[new_obj] = old_dependencies;

	for (auto &dependency : to_add) {
		owned_by_map[new_obj].insert(dependency);
		owns_map[dependency].insert(new_obj);
	}
}

void DependencyManager::EraseObject(CatalogEntry *object) {
	// obtain the writing lock
	EraseObjectInternal(object);
}

void DependencyManager::EraseObjectInternal(CatalogEntry *object) {
	if (owns_map.find(object) == owns_map.end()) {
		// dependencies already removed
		return;
	}
	D_ASSERT(owns_map.find(object) != owns_map.end());
	D_ASSERT(owned_by_map.find(object) != owned_by_map.end());
	// now for each of the dependencies, erase the entries from the owns_map
	for (auto &dependency : owned_by_map[object]) {
		auto entry = owns_map.find(dependency);
		if (entry != owns_map.end()) {
			D_ASSERT(entry->second.find(object) != entry->second.end());
			entry->second.erase(object);
		}
	}
	// erase the dependents and dependencies for this object
	owns_map.erase(object);
	owned_by_map.erase(object);
}

void DependencyManager::Scan(const std::function<void(CatalogEntry *, CatalogEntry *, DependencyType)> &callback) {
	lock_guard<mutex> write_lock(catalog.write_lock);
	for (auto &entry : owns_map) {
		for (auto &dependent : entry.second) {
			callback(entry.first, dependent.entry, dependent.dependency_type);
		}
	}
}

void DependencyManager::AddDependencyToObject(ClientContext &context, CatalogEntry *object, CatalogEntry *entry) {
	owns_map[object].emplace(entry);
	owned_by_map[entry].emplace(object);
}

void DependencyManager::RemoveDependencyFromObject(ClientContext &context, CatalogEntry *object, CatalogEntry *entry) {
	// Erase returns 0 if the key is not present, or the number of deleted keys
	owns_map[object].erase(entry);
	owned_by_map[entry].erase(object);
}

dependency_set_t DependencyManager::GetOwns(ClientContext &context, CatalogEntry *object) {
	return owns_map[object];
}

std::unordered_set<CatalogEntry *> DependencyManager::GetOwnedBy(ClientContext &context, CatalogEntry *object) {
	return owned_by_map[object];
}

void DependencyManager::CopyDependencies(ClientContext &context, CatalogEntry *from, CatalogEntry *to) {

	owns_map[to] = owns_map[from];
	for (auto object : owns_map[to]) {
		owned_by_map[object.entry].emplace(to);
	}

	owned_by_map[to] = owned_by_map[from];
	for (auto object : owned_by_map[to]) {
		owns_map[object].emplace(to);
	}
}

} // namespace duckdb
