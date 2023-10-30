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

DependencyManager::DependencyManager(DuckCatalog &catalog) : catalog(catalog), dependency_sets(catalog) {
}

string DependencyManager::GetSchema(CatalogEntry &entry) {
	if (entry.type == CatalogType::SCHEMA_ENTRY) {
		return entry.name;
	}
	return entry.ParentSchema().name;
}

string DependencyManager::MangleName(CatalogType type, const string &schema, const string &name) {
	auto null_byte = string(1, '\0');
	return CatalogTypeToString(type) + null_byte + schema + null_byte + name;
}

string DependencyManager::MangleName(CatalogEntry &entry) {
	CatalogType type = CatalogType::INVALID;
	string schema;
	string name;

	if (entry.type == CatalogType::DEPENDENCY_ENTRY) {
		auto &dependency_entry = entry.Cast<DependencyCatalogEntry>();
		return dependency_entry.MangledName();
	} else if (entry.type == CatalogType::DEPENDENCY_SET) {
		auto &dependency_set = entry.Cast<DependencySetCatalogEntry>();
		return dependency_set.MangledName();
	} else {
		type = entry.type;
		schema = GetSchema(entry);
		name = entry.name;
	}
	D_ASSERT(type != CatalogType::INVALID);
	return MangleName(type, schema, name);
}

void DependencyManager::DropDependencySet(CatalogTransaction transaction, CatalogEntry &object) {
	auto name = MangleName(object);
	dependency_sets.DropEntry(transaction, name, false);
}

static void AssertMangledName(const string &mangled_name) {
	idx_t nullbyte_count = 0;
	for (auto &ch : mangled_name) {
		nullbyte_count += ch == '\0';
	}
	(void)nullbyte_count;
	D_ASSERT(nullbyte_count == 2);
}

optional_ptr<DependencySetCatalogEntry> DependencyManager::GetDependencySet(CatalogTransaction transaction,
                                                                            const string &mangled_name) {
	AssertMangledName(mangled_name);
	auto dependency_set_p = dependency_sets.GetEntry(transaction, mangled_name);
	if (!dependency_set_p) {
		return nullptr;
	}
	D_ASSERT(dependency_set_p->type == CatalogType::DEPENDENCY_SET);
	return dynamic_cast<DependencySetCatalogEntry *>(dependency_set_p.get());
}

optional_ptr<DependencySetCatalogEntry> DependencyManager::GetDependencySet(CatalogTransaction transaction,
                                                                            CatalogEntry &object) {
	auto mangled_name = MangleName(object);
	return GetDependencySet(transaction, mangled_name);
}

DependencySetCatalogEntry &DependencyManager::GetOrCreateDependencySet(CatalogTransaction transaction,
                                                                       CatalogType entry_type,
                                                                       const string &entry_schema,
                                                                       const string &entry_name) {
	auto mangled_name = MangleName(entry_type, entry_schema, entry_name);
	auto dependency_set_p = GetDependencySet(transaction, mangled_name);
	if (dependency_set_p) {
		return *dependency_set_p;
	}
	static const DependencyList EMPTY_DEPENDENCIES;
	auto new_dependency_set =
	    make_uniq<DependencySetCatalogEntry>(catalog, *this, entry_type, entry_schema, entry_name);
	if (catalog.IsTemporaryCatalog()) {
		new_dependency_set->temporary = true;
	}
	auto &dependency_set = *new_dependency_set;
	auto name = new_dependency_set->MangledName();
	auto res = dependency_sets.CreateEntry(transaction, name, std::move(new_dependency_set), EMPTY_DEPENDENCIES);
	(void)res;
	D_ASSERT(res);
	return dependency_set;
}

DependencySetCatalogEntry &DependencyManager::GetOrCreateDependencySet(CatalogTransaction transaction,
                                                                       CatalogEntry &object) {
	string entry_schema;
	string entry_name;
	CatalogType entry_type;

	DependencyManager::GetLookupProperties(object, entry_schema, entry_name, entry_type);
	return GetOrCreateDependencySet(transaction, entry_type, entry_schema, entry_name);
}

bool DependencyManager::IsSystemEntry(CatalogEntry &entry) const {
	if (entry.internal) {
		return true;
	}

	switch (entry.type) {
	case CatalogType::DEPENDENCY_ENTRY:
	case CatalogType::DEPENDENCY_SET:
	case CatalogType::DATABASE_ENTRY:
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
		auto &dependency_set = GetOrCreateDependencySet(transaction, dependency);
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
	} else if (entry.type == CatalogType::DEPENDENCY_SET) {
		auto &dependency_set = entry.Cast<DependencySetCatalogEntry>();

		schema = dependency_set.EntrySchema();
		name = dependency_set.EntryName();
		type = dependency_set.EntryType();
	} else {
		schema = DependencyManager::GetSchema(entry);
		name = entry.name;
		type = entry.type;
	}
}

// Always performs the callback, it's up to the callback to determine what to do based on the lookup result
optional_ptr<CatalogEntry> DependencyManager::LookupEntry(CatalogTransaction transaction, CatalogEntry &dependency) {
	string schema;
	string name;
	CatalogType type;
	GetLookupProperties(dependency, schema, name, type);

	// Lookup the schema
	auto schema_entry = catalog.schemas->GetEntry(transaction, schema);
	if (type == CatalogType::SCHEMA_ENTRY || !schema_entry) {
		// This is a schema entry, perform the callback only providing the schema
		return schema_entry;
	}
	auto &duck_schema_entry = schema_entry->Cast<DuckSchemaEntry>();

	// Lookup the catalog set
	auto &catalog_set = duck_schema_entry.GetCatalogSet(type);

	// Use the index to find the actual entry
	auto entry = catalog_set.GetEntry(transaction, name);
	return entry;
}

void DependencyManager::CleanupDependencies(CatalogTransaction transaction, CatalogEntry &object) {
	auto dependency_set_p = GetDependencySet(transaction, object);
	D_ASSERT(dependency_set_p);
	auto &dependency_set = *dependency_set_p;

	// Collect the dependencies
	catalog_entry_set_t dependencies_to_remove;
	dependency_set.ScanDependencies(transaction,
	                                [&](DependencyCatalogEntry &dep) { dependencies_to_remove.insert(dep); });
	// Also collect the dependents
	catalog_entry_set_t dependents_to_remove;
	dependency_set.ScanDependents(transaction, [&](DependencyCatalogEntry &dep) { dependents_to_remove.insert(dep); });

	// Remove the dependency entries
	for (auto &dependency : dependencies_to_remove) {
		auto other_dependency_set_p = GetDependencySet(transaction, dependency);
		auto &other_dependency_set = *other_dependency_set_p;

		other_dependency_set.RemoveDependent(transaction, dependency_set);
		dependency_set.RemoveDependency(transaction, dependency);
	}
	// Remove the dependent entries
	for (auto &dependent : dependents_to_remove) {
		auto other_dependency_set_p = GetDependencySet(transaction, dependent);
		auto &other_dependency_set = *other_dependency_set_p;

		other_dependency_set.RemoveDependency(transaction, dependency_set);
		dependency_set.RemoveDependent(transaction, dependent);
	}

	DropDependencySet(transaction, object);
}

void DependencyManager::DropObject(CatalogTransaction transaction, CatalogEntry &object, bool cascade) {
	if (IsSystemEntry(object)) {
		// Don't do anything for this
		return;
	}

	// Check if there are any dependencies registered on this object
	auto object_dependency_set_p = GetDependencySet(transaction, object);
	if (!object_dependency_set_p) {
		return;
	}
	auto &object_dependency_set = *object_dependency_set_p;

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

	auto old_dependency_set_p = GetDependencySet(transaction, old_obj);
	if (!old_dependency_set_p) {
		// Nothing depends on this object and this object doesn't depend on anything either
		return;
	}
	auto &old_dependency_set = *old_dependency_set_p;

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
		auto &dependent = other_dependency_set->GetDependent(transaction, old_obj);
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
		other_dependency_set->AddDependent(transaction, new_obj).CompleteLink(transaction);
	}

	auto &dependency_set = GetOrCreateDependencySet(transaction, new_obj);

	// For all the objects we own, re establish the dependency of the owner on the object
	for (auto &object : owned_objects) {
		auto &entry = object.entry.get();
		auto other_dependency_set = GetDependencySet(transaction, entry);
		D_ASSERT(other_dependency_set);

		auto &dependent_to =
		    other_dependency_set->AddDependent(transaction, new_obj, DependencyType::DEPENDENCY_OWNED_BY);
		dependent_to.CompleteLink(transaction);

		auto &dependent_from = dependency_set.AddDependent(transaction, entry, DependencyType::DEPENDENCY_OWNS);
		dependent_from.CompleteLink(transaction);
	}
}

void DependencyManager::Scan(ClientContext &context,
                             const std::function<void(CatalogEntry &, CatalogEntry &, DependencyType)> &callback) {
	lock_guard<mutex> write_lock(catalog.GetWriteLock());
	auto transaction = catalog.GetCatalogTransaction(context);

	// All the objects registered in the dependency manager
	catalog_entry_set_t entries;
	dependency_sets.Scan(transaction, [&](CatalogEntry &set) {
		auto entry = LookupEntry(transaction, set);
		entries.insert(*entry);
	});

	// For every registered entry, get the dependents
	for (auto &entry : entries) {
		auto set = GetDependencySet(transaction, entry);
		// Scan all the dependents of the entry
		set->ScanDependents(transaction, [&](DependencyCatalogEntry &dependent) {
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
	auto &owner_dependency_set = GetOrCreateDependencySet(transaction, owner);
	owner_dependency_set.ScanDependents(transaction, [&](DependencyCatalogEntry &dep) {
		if (dep.Type() == DependencyType::DEPENDENCY_OWNED_BY) {
			throw DependencyException(owner.name + " already owned by " + dep.EntryName());
		}
	});

	// If the entry is already owned, throw an error
	auto &entry_dependency_set = GetOrCreateDependencySet(transaction, entry);
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
