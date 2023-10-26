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
#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/catalog/catalog_entry/dependency_set_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/dependency_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_schema_entry.hpp"

namespace duckdb {

DependencyManager::DependencyManager(DuckCatalog &catalog) : catalog(catalog), connections(catalog) {
}

static string GetSchema(CatalogEntry &entry) {
	if (entry.type == CatalogType::SCHEMA_ENTRY) {
		return entry.name;
	}
	return entry.ParentSchema().name;
}

static string GetMangledNameFromDependency(CatalogEntry &entry) {
	D_ASSERT(entry.type == CatalogType::DEPENDENCY_ENTRY);
	auto &dependency_entry = entry.Cast<DependencyCatalogEntry>();

	auto &type = dependency_entry.type;
	auto &name = dependency_entry.name;
	auto &schema = dependency_entry.schema;
	return StringUtil::Format("%s\0%s\0%s", type, schema, name);
}

static string GetMangledName(CatalogEntry &entry) {
	if (entry.type == CatalogType::DEPENDENCY_ENTRY) {
		return GetMangledNameFromDependency(entry);
	}
	auto schema = GetSchema(entry);
	return StringUtil::Format("%s\0%s\0%s", CatalogTypeToString(entry.type), schema, entry.name);
}

optional_ptr<DependencySetCatalogEntry> DependencyManager::GetDependencySet(CatalogEntry &object) {
	auto name = GetMangledName(object);
	auto mapping = connections.GetLatestMapping(name);
	if (!mapping) {
		return nullptr;
	}
	auto it = connections.entries.find(mapping->index.GetIndex());
	if (it == connections.entries.end()) {
		return nullptr;
	}
	auto &entry_value = it->second;
	auto &dependency_set_entry = entry_value.Entry();

	D_ASSERT(dependency_set_entry.type == CatalogType::DEPENDENCY_SET);
	return dynamic_cast<DependencySetCatalogEntry *>(&dependency_set_entry);
}

optional_ptr<DependencySetCatalogEntry> DependencyManager::GetDependencySet(CatalogTransaction transaction,
                                                                            CatalogEntry &object) {
	auto name = GetMangledName(object);
	auto connection_p = connections.GetEntry(transaction, name);
	if (!connection_p) {
		return nullptr;
	}
	D_ASSERT(connection_p->type == CatalogType::DEPENDENCY_SET);
	return dynamic_cast<DependencySetCatalogEntry *>(connection_p.get());
}

DependencySetCatalogEntry &DependencyManager::GetOrCreateDependencySet(CatalogTransaction transaction,
                                                                       CatalogEntry &object) {
	auto name = GetMangledName(object);
	auto connection_p = connections.GetEntry(transaction, name);
	if (!connection_p) {
		auto new_connection = make_uniq<DependencySetCatalogEntry>(catalog, name);
		auto &connection = *new_connection;
		DependencyList empty_dependencies;
		auto res = connections.CreateEntry(transaction, name, std::move(new_connection), empty_dependencies);
		(void)res;
		D_ASSERT(res);
		return connection;
	}
	D_ASSERT(connection_p->type == CatalogType::DEPENDENCY_SET);
	return connection_p->Cast<DependencySetCatalogEntry>();
}

bool DependencyManager::IsDependencyEntry(CatalogEntry &entry) const {
	return entry.type == CatalogType::DEPENDENCY_SET || entry.type == CatalogType::DEPENDENCY_ENTRY;
}

void DependencyManager::AddObject(CatalogTransaction transaction, CatalogEntry &object, DependencyList &dependencies) {
	if (IsDependencyEntry(object)) {
		// Don't do anything for this
		return;
	}

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
		// NEW
		auto &dependency_connections = GetOrCreateDependencySet(transaction, dependency);
		dependency_connections.AddDependent(transaction, object, dependency_type);

		// OLD
		auto &set = dependents_map[dependency];
		set.insert(Dependency(object, dependency_type));
	}
	// create the dependents map for this object: it starts out empty

	// NEW
	auto &object_connections = GetOrCreateDependencySet(transaction, object);
	object_connections.AddDependencies(transaction, dependencies);

	// OLD
	dependents_map[object] = dependency_set_t();
	dependencies_map[object] = dependencies.set;
}

static bool CascadeDrop(bool cascade, DependencyType dependency_type) {
	if (cascade) {
		return true;
	}
	if (dependency_type == DependencyType::DEPENDENCY_AUTOMATIC) {
		// These dependencies are automatically dropped implicitly
		return true;
	}
	if (dependency_type == DependencyType::DEPENDENCY_OWNS) {
		// The object has explicit ownership over the dependency
		return true;
	}
	return false;
}

// Always performs the callback, it's up to the callback to determine what to do based on the lookup result
void DependencyManager::LookupEntry(CatalogTransaction transaction, CatalogEntry &dependency,
                                    lookup_callback_t callback) {
	D_ASSERT(dependency.type == CatalogType::DEPENDENCY_ENTRY);
	auto &dependency_entry = dependency.Cast<DependencyCatalogEntry>();

	auto &schema = dependency_entry.schema;
	auto &name = dependency_entry.name;
	auto &type = dependency_entry.type;

	// Lookup the schema
	auto schema_entry_p = catalog.GetSchema(transaction, schema, OnEntryNotFound::THROW_EXCEPTION);
	if (type == CatalogType::SCHEMA_ENTRY) {
		// This is a schema entry, perform the callback only providing the schema
		auto entry = dynamic_cast<CatalogEntry *>(schema_entry_p.get());
		callback(entry, nullptr, nullptr);
		return;
	}
	auto &schema_entry = schema_entry_p->Cast<DuckSchemaEntry>();

	// Lookup the catalog set
	auto &catalog_set = schema_entry.GetCatalogSet(type);

	// Get the mapping from name -> index
	auto mapping_value = catalog_set.GetMapping(transaction, name, /* get_latest = */ true);
	if (!mapping_value) {
		callback(nullptr, &catalog_set, nullptr);
		return;
	}
	// Use the index to find the actual entry
	auto entry = catalog_set.GetEntryInternal(transaction, mapping_value->index);
	callback(entry, &catalog_set, mapping_value);
	return;
}

void DependencyManager::DropObjectInternalNew(CatalogTransaction transaction, CatalogEntry &object, bool cascade) {
	// first check the objects that depend on this object
	auto object_connections_p = GetDependencySet(transaction, object);
	if (!object_connections_p) {
		return;
	}
	auto &object_connections = *object_connections_p;

	auto &dependents = object_connections.Dependents();
	dependents.Scan([&](CatalogEntry &other) {
		D_ASSERT(other.type == CatalogType::DEPENDENCY_ENTRY);
		auto &other_entry = other.Cast<DependencyCatalogEntry>();
		auto other_connections_p = GetDependencySet(transaction, other);
		if (!other_connections_p) {
			// Already deleted
			return;
		}
		auto &other_connections = *other_connections_p;
		D_ASSERT(other_connections.HasDependencyOn(object));

		if (!CascadeDrop(cascade, other_entry.dependency_type)) {
			// no cascade and there are objects that depend on this object: throw error
			throw DependencyException("Cannot drop entry \"%s\" because there are entries that "
			                          "depend on it. Use DROP...CASCADE to drop all dependents.",
			                          object.name);
		}

		// It makes no sense to have a schema depend on anything
		D_ASSERT(other_entry.entry_type != CatalogType::SCHEMA_ENTRY);

		LookupEntry(
		    transaction, other_entry,
		    [&](optional_ptr<CatalogEntry> entry, optional_ptr<CatalogSet> set, optional_ptr<MappingValue> mapping) {
			    if (!entry) {
				    return;
			    }
			    set->DropEntryInternal(transaction, mapping->index.Copy(), *entry, cascade);
		    });
	});
}

void DependencyManager::DropObjectInternalOld(CatalogTransaction transaction, CatalogEntry &object, bool cascade) {
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

void DependencyManager::DropObject(CatalogTransaction transaction, CatalogEntry &object, bool cascade) {
	if (IsDependencyEntry(object)) {
		// Don't do anything for this
		return;
	}
	DropObjectInternalNew(transaction, object, cascade);
	DropObjectInternalOld(transaction, object, cascade);
}

void DependencyManager::AlterObjectInternalNew(CatalogTransaction transaction, CatalogEntry &old_obj,
                                               CatalogEntry &new_obj) {
	auto old_connections_p = GetDependencySet(transaction, old_obj);
	if (!old_connections_p) {
		// Nothing depends on this object and this object doesn't depend on anything either
		return;
	}
	auto &old_connections = *old_connections_p;

	// FIXME: what if we change the type of a column, gaining/losing a dependency on a type entry??

	dependency_set_t preserved_dependents;
	auto &dependents = old_connections.Dependents();
	dependents.Scan([&](CatalogEntry &other) {
		D_ASSERT(other.type == CatalogType::DEPENDENCY_ENTRY);
		auto &other_entry = other.Cast<DependencyCatalogEntry>();
		auto other_connections_p = GetDependencySet(transaction, other);
		if (!other_connections_p) {
			// Already deleted
			return;
		}
		D_ASSERT(other_connections_p->HasDependencyOn(old_obj));

		// It makes no sense to have a schema depend on anything
		D_ASSERT(other_entry.entry_type != CatalogType::SCHEMA_ENTRY);

		LookupEntry(
		    transaction, other_entry,
		    [&](optional_ptr<CatalogEntry> entry, optional_ptr<CatalogSet> set, optional_ptr<MappingValue> mapping) {
			    if (!entry) {
				    return;
			    }
			    if (other_entry.dependency_type == DependencyType::DEPENDENCY_OWNS) {
				    preserved_dependents.insert(Dependency(*entry, other_entry.dependency_type));
				    return;
			    }
			    // conflict: attempting to alter this object but the dependent object still exists
			    // no cascade and there are objects that depend on this object: throw error
			    throw DependencyException("Cannot alter entry \"%s\" because there are entries that "
			                              "depend on it.",
			                              old_obj.name);
		    });
	});

	// Keep old dependencies
	auto &old_dependencies = old_connections.Dependencies();
	dependency_set_t dependency_list;
	old_dependencies.Scan([&](CatalogEntry &other) {
		D_ASSERT(other.type == CatalogType::DEPENDENCY_ENTRY);
		auto &other_entry = other.Cast<DependencyCatalogEntry>();
		auto other_connections_p = GetDependencySet(transaction, other);
		if (!other_connections_p) {
			// Already deleted
			return;
		}
		auto &other_connections = *other_connections_p;

		LookupEntry(
		    transaction, other_entry,
		    [&](optional_ptr<CatalogEntry> entry, optional_ptr<CatalogSet> set, optional_ptr<MappingValue> mapping) {
			    if (!entry) {
				    return;
			    }
			    dependency_list.insert(Dependency(*entry, other_entry.dependency_type));
			    // Register that the new version of this object still has this dependency.
			    // FIXME: what should the dependency type be???
			    other_connections.AddDependent(transaction, new_obj, DependencyType::DEPENDENCY_REGULAR);
		    });
	});
	// Add the dependencies to the new object
	auto &connections = GetOrCreateDependencySet(transaction, new_obj);
	for (auto &dep : preserved_dependents) {
		auto &entry = dep.entry.get();
		// Create a regular dependency on 'entry', so the drop of 'entry' is blocked by the object
		dependency_list.insert(Dependency(entry));
	}
	connections.AddDependencies(transaction, dependency_list);

	// Add the dependents that did not block the Alter
	connections.AddDependents(transaction, preserved_dependents);

	for (auto &dependency : preserved_dependents) {
		auto &entry = dependency.entry.get();
		auto &dependency_connections = GetOrCreateDependencySet(transaction, entry);

		dependency_connections.AddDependent(transaction, entry, DependencyType::DEPENDENCY_OWNED_BY);
		// new_obj has a "dependency" on the object, by the type of DEPENDENCY_OWNED_BY
		dependents_map[entry].insert(Dependency(new_obj, DependencyType::DEPENDENCY_OWNED_BY));
	}
}

void DependencyManager::AlterObjectInternalOld(CatalogTransaction transaction, CatalogEntry &old_obj,
                                               CatalogEntry &new_obj) {
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

void DependencyManager::AlterObject(CatalogTransaction transaction, CatalogEntry &old_obj, CatalogEntry &new_obj) {
	if (IsDependencyEntry(new_obj)) {
		D_ASSERT(IsDependencyEntry(old_obj));
		// Don't do anything for this
		return;
	}

	AlterObjectInternalNew(transaction, old_obj, new_obj);
	AlterObjectInternalOld(transaction, old_obj, new_obj);
}

void DependencyManager::EraseObject(CatalogEntry &object) {
	if (IsDependencyEntry(object)) {
		// Don't do anything for this
		return;
	}

	// obtain the writing lock
	EraseObjectInternal(object);
}

void DependencyManager::EraseObjectInternal(CatalogEntry &object) {
	// NOTE: I think this is no longer a necessary step?

	//D_ASSERT(!IsDependencyEntry(object));

	//auto connections_p = GetDependencySet(object);
	//if (!connections_p) {
	//	// dependencies already removed
	//	return;
	//}
	//auto &connections = *connections_p;

	//auto &dependencies = connections.Dependencies();
	//dependencies.Scan([&](CatalogEntry &other) {
	//	auto other_connections_p = GetDependencySet(other);
	//	if (!other_connections_p) {
	//		return;
	//	}
	//	auto &other_connections = *other_connections_p;
	//	other_connections.IsDependencyOf(object);
	//});
	//// now for each of the dependencies, erase the entries from the dependents_map
	//for (auto &dependency : dependencies_map[object]) {
	//	auto entry = dependents_map.find(dependency);
	//	if (entry != dependents_map.end()) {
	//		D_ASSERT(entry->second.find(object) != entry->second.end());
	//		entry->second.erase(object);
	//	}
	//}
	//// erase the dependents and dependencies for this object
	//dependents_map.erase(object);
	//dependencies_map.erase(object);
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
	D_ASSERT(!IsDependencyEntry(entry));
	D_ASSERT(!IsDependencyEntry(owner));

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
