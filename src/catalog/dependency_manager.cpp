#include "duckdb/catalog/dependency_manager.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/catalog/catalog_entry/dependency_set_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/dependency_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_schema_entry.hpp"

namespace duckdb {

static void AssertMangledName(const string &mangled_name, idx_t expected_null_bytes) {
#ifdef DEBUG
	idx_t nullbyte_count = 0;
	for (auto &ch : mangled_name) {
		nullbyte_count += ch == '\0';
	}
	D_ASSERT(nullbyte_count == expected_null_bytes);
#endif
}

MangledEntryName::MangledEntryName(CatalogType type, const string &schema, const string &name) {
	static const auto NULL_BYTE = string(1, '\0');
	this->name = CatalogTypeToString(type) + NULL_BYTE + schema + NULL_BYTE + name;
	AssertMangledName(this->name, 2);
}

MangledDependencyName::MangledDependencyName(const MangledEntryName &from, const MangledEntryName &to) {
	static const auto NULL_BYTE = string(1, '\0');
	this->name = from.name + NULL_BYTE + to.name;
	AssertMangledName(this->name, 5);
}

DependencyManager::DependencyManager(DuckCatalog &catalog)
    : catalog(catalog), dependencies(catalog), dependents(catalog) {
}

string DependencyManager::GetSchema(CatalogEntry &entry) {
	if (entry.type == CatalogType::SCHEMA_ENTRY) {
		return entry.name;
	}
	return entry.ParentSchema().name;
}

MangledEntryName DependencyManager::MangleName(CatalogType type, const string &schema, const string &name) {
	return MangledEntryName(type, schema, name);
}

MangledEntryName DependencyManager::MangleName(CatalogEntry &entry) {
	CatalogType type = CatalogType::INVALID;
	string schema;
	string name;

	if (entry.type == CatalogType::DEPENDENCY_ENTRY) {
		auto &dependency_entry = entry.Cast<DependencyCatalogEntry>();
		return dependency_entry.MangledName();
	} else {
		type = entry.type;
		schema = GetSchema(entry);
		name = entry.name;
	}
	D_ASSERT(type != CatalogType::INVALID);
	return MangleName(type, schema, name);
}

DependencySetCatalogEntry DependencyManager::GetDependencySet(CatalogTransaction transaction, CatalogType type,
                                                              const string &schema, const string &name) {
	DependencySetCatalogEntry set(catalog, *this, type, schema, name);
	return set;
}

DependencySetCatalogEntry DependencyManager::GetDependencySet(CatalogTransaction transaction, CatalogEntry &object) {
	CatalogType entry_type;
	string entry_schema;
	string entry_name;

	GetLookupProperties(object, entry_schema, entry_name, entry_type);
	return GetDependencySet(transaction, entry_type, entry_schema, entry_name);
}

bool DependencyManager::IsSystemEntry(CatalogEntry &entry) const {
	if (entry.internal) {
		return true;
	}

	switch (entry.type) {
	case CatalogType::DEPENDENCY_ENTRY:
	case CatalogType::DATABASE_ENTRY:
	case CatalogType::RENAMED_ENTRY:
		return true;
	default:
		return false;
	}
}

void DependencyManager::AddObject(CatalogTransaction transaction, CatalogEntry &object,
                                  const DependencyList &dependencies) {
	if (IsSystemEntry(object)) {
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
		auto catalog_entry = dependency.set->GetEntry(transaction, dependency.name);
		if (!catalog_entry) {
			throw InternalException("Dependency has already been deleted?");
		}
	}

	// indexes do not require CASCADE to be dropped, they are simply always dropped along with the table
	auto dependency_type = object.type == CatalogType::INDEX_ENTRY ? DependencyType::DEPENDENCY_AUTOMATIC
	                                                               : DependencyType::DEPENDENCY_REGULAR;
	// add the object to the dependents_map of each object that it depends on
	for (auto &dependency : dependencies.set) {
		auto dependency_set = GetDependencySet(transaction, dependency);
		// Create the dependent and complete the link by creating the dependency as well
		dependency_set.AddDependent(transaction, object, dependency_type).CompleteLink(transaction);
	}
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

void DependencyManager::GetLookupProperties(CatalogEntry &entry, string &schema, string &name, CatalogType &type) {
	if (entry.type == CatalogType::DEPENDENCY_ENTRY) {
		auto &dependency_entry = entry.Cast<DependencyCatalogEntry>();

		schema = dependency_entry.EntrySchema();
		name = dependency_entry.EntryName();
		type = dependency_entry.EntryType();
	} else {
		schema = DependencyManager::GetSchema(entry);
		name = entry.name;
		type = entry.type;
	}
}

optional_ptr<CatalogEntry> DependencyManager::LookupEntry(CatalogTransaction transaction, CatalogEntry &dependency) {
	string schema;
	string name;
	CatalogType type;
	GetLookupProperties(dependency, schema, name, type);

	// Lookup the schema
	auto schema_entry = catalog.GetSchema(transaction, schema, OnEntryNotFound::RETURN_NULL);
	if (type == CatalogType::SCHEMA_ENTRY || !schema_entry) {
		// This is a schema entry, perform the callback only providing the schema
		return reinterpret_cast<CatalogEntry *>(schema_entry.get());
	}
	auto entry = schema_entry->GetEntry(transaction, type, name);
	return entry;
}

void DependencyManager::CleanupDependencies(CatalogTransaction transaction, CatalogEntry &object) {
	auto dependency_set = GetDependencySet(transaction, object);

	// Collect the dependencies
	catalog_entry_set_t dependencies_to_remove;
	dependency_set.ScanDependencies(transaction,
	                                [&](DependencyCatalogEntry &dep) { dependencies_to_remove.insert(dep); });
	// Also collect the dependents
	catalog_entry_set_t dependents_to_remove;
	dependency_set.ScanDependents(transaction, [&](DependencyCatalogEntry &dep) { dependents_to_remove.insert(dep); });

	// Remove the dependency entries
	for (auto &dependency : dependencies_to_remove) {
		auto other_dependency_set = GetDependencySet(transaction, dependency);

		other_dependency_set.RemoveDependent(transaction, dependency_set);
		dependency_set.RemoveDependency(transaction, dependency);
	}
	// Remove the dependent entries
	for (auto &dependent : dependents_to_remove) {
		auto other_dependency_set = GetDependencySet(transaction, dependent);

		other_dependency_set.RemoveDependency(transaction, dependency_set);
		dependency_set.RemoveDependent(transaction, dependent);
	}
}

void DependencyManager::DropObject(CatalogTransaction transaction, CatalogEntry &object, bool cascade) {
	if (IsSystemEntry(object)) {
		// Don't do anything for this
		return;
	}

	// Check if there are any dependencies registered on this object
	auto object_dependency_set = GetDependencySet(transaction, object);

	// Check if there are any entries that block the DROP because they still depend on the object
	catalog_entry_set_t to_drop;
	object_dependency_set.ScanDependents(transaction, [&](DependencyCatalogEntry &dep) {
		// It makes no sense to have a schema depend on anything
		D_ASSERT(dep.EntryType() != CatalogType::SCHEMA_ENTRY);
		auto entry = LookupEntry(transaction, dep);
		if (!entry) {
			return;
		}

		if (!CascadeDrop(cascade, dep.Type())) {
			// no cascade and there are objects that depend on this object: throw error
			throw DependencyException("Cannot drop entry \"%s\" because there are entries that "
			                          "depend on it. Use DROP...CASCADE to drop all dependents.",
			                          object.name);
		}
		to_drop.insert(*entry);
	});

	CleanupDependencies(transaction, object);

	for (auto &entry : to_drop) {
		auto set = entry.get().set;
		D_ASSERT(set);
		set->DropEntry(transaction, entry.get().name, cascade);
	}
}

void DependencyManager::AlterObject(CatalogTransaction transaction, CatalogEntry &old_obj, CatalogEntry &new_obj) {
	if (IsSystemEntry(new_obj)) {
		D_ASSERT(IsSystemEntry(old_obj));
		// Don't do anything for this
		return;
	}

	auto old_dependency_set = GetDependencySet(transaction, old_obj);

	dependency_set_t owned_objects;
	old_dependency_set.ScanDependents(transaction, [&](DependencyCatalogEntry &dep) {
		// It makes no sense to have a schema depend on anything
		D_ASSERT(dep.EntryType() != CatalogType::SCHEMA_ENTRY);

		auto entry = LookupEntry(transaction, dep);
		if (!entry) {
			return;
		}
		if (dep.Type() == DependencyType::DEPENDENCY_OWNS) {
			owned_objects.insert(Dependency(*entry, dep.Type()));
			return;
		}
		// conflict: attempting to alter this object but the dependent object still exists
		// no cascade and there are objects that depend on this object: throw error
		throw DependencyException("Cannot alter entry \"%s\" because there are entries that "
		                          "depend on it.",
		                          old_obj.name);
	});

	// Keep old dependencies
	dependency_set_t dependents;
	old_dependency_set.ScanDependencies(transaction, [&](DependencyCatalogEntry &dep) {
		auto entry = LookupEntry(transaction, dep);
		if (!entry) {
			return;
		}
		auto other_dependency_set = GetDependencySet(transaction, dep);
		// Find the dependent entry so we can properly restore the type it has
		auto &dependent = other_dependency_set.GetDependent(transaction, old_obj);
		dependents.insert(Dependency(*entry, dependent.Type()));
	});

	// FIXME: we should update dependencies in the future
	// some alters could cause dependencies to change (imagine types of table columns)
	// or DEFAULT depending on a sequence
	if (StringUtil::CIEquals(old_obj.name, new_obj.name)) {
		// The name was not changed, we do not need to recreate the dependency links
		return;
	}
	CleanupDependencies(transaction, old_obj);

	for (auto &dep : dependents) {
		auto &other = dep.entry.get();
		auto other_dependency_set = GetDependencySet(transaction, other);
		other_dependency_set.AddDependent(transaction, new_obj).CompleteLink(transaction);
	}

	auto dependency_set = GetDependencySet(transaction, new_obj);

	// For all the objects we own, re establish the dependency of the owner on the object
	for (auto &object : owned_objects) {
		auto &entry = object.entry.get();
		auto other_dependency_set = GetDependencySet(transaction, entry);

		other_dependency_set.AddDependent(transaction, new_obj, DependencyType::DEPENDENCY_OWNED_BY)
		    .CompleteLink(transaction);
		dependency_set.AddDependent(transaction, entry, DependencyType::DEPENDENCY_OWNS).CompleteLink(transaction);
	}
}

void DependencyManager::Scan(ClientContext &context,
                             const std::function<void(CatalogEntry &, CatalogEntry &, DependencyType)> &callback) {
	lock_guard<mutex> write_lock(catalog.GetWriteLock());
	auto transaction = catalog.GetCatalogTransaction(context);

	// All the objects registered in the dependency manager
	catalog_entry_set_t entries;
	dependents.Scan(transaction, [&](CatalogEntry &set) {
		auto entry = LookupEntry(transaction, set);
		entries.insert(*entry);
	});

	// For every registered entry, get the dependents
	for (auto &entry : entries) {
		auto set = GetDependencySet(transaction, entry);
		// Scan all the dependents of the entry
		set.ScanDependents(transaction, [&](DependencyCatalogEntry &dependent) {
			auto dep = LookupEntry(transaction, dependent);
			if (!dep) {
				return;
			}
			auto &dependent_entry = *dep;
			callback(entry, dependent_entry, dependent.Type());
		});
	}
}

void DependencyManager::AddOwnership(CatalogTransaction transaction, CatalogEntry &owner, CatalogEntry &entry) {
	if (IsSystemEntry(entry) || IsSystemEntry(owner)) {
		return;
	}

	// If the owner is already owned by something else, throw an error
	auto owner_dependency_set = GetDependencySet(transaction, owner);
	owner_dependency_set.ScanDependents(transaction, [&](DependencyCatalogEntry &dep) {
		if (dep.Type() == DependencyType::DEPENDENCY_OWNED_BY) {
			throw DependencyException(owner.name + " already owned by " + dep.EntryName());
		}
	});

	// If the entry is already owned, throw an error
	auto entry_dependency_set = GetDependencySet(transaction, entry);
	entry_dependency_set.ScanDependents(transaction, [&](DependencyCatalogEntry &other) {
		auto dependency_type = other.Type();

		auto dependent_entry = LookupEntry(transaction, other);
		if (!dependent_entry) {
			return;
		}
		auto &dep = *dependent_entry;

		// FIXME: should this not check for DEPENDENCY_OWNS first??

		// if the entry is already owned, throw error
		if (&dep != &owner) {
			throw DependencyException(entry.name + " already depends on " + dep.name);
		}

		// if the entry owns the owner, throw error
		if (&dep == &owner && dependency_type == DependencyType::DEPENDENCY_OWNS) {
			throw DependencyException(entry.name + " already owns " + owner.name +
			                          ". Cannot have circular dependencies");
		}
	});
	entry_dependency_set.AddDependent(transaction, owner, DependencyType::DEPENDENCY_OWNED_BY)
	    .CompleteLink(transaction);
	owner_dependency_set.AddDependent(transaction, entry, DependencyType::DEPENDENCY_OWNS).CompleteLink(transaction);
}

} // namespace duckdb
