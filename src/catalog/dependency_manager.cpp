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
#include "duckdb/catalog/catalog_entry/dependency_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_schema_entry.hpp"
#include "duckdb/catalog/dependency_catalog_set.hpp"

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

MangledEntryName::MangledEntryName(const CatalogEntryInfo &info) {
	static const auto NULL_BYTE = string(1, '\0');

	auto &type = info.type;
	auto &schema = info.schema;
	auto &name = info.name;

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

MangledEntryName DependencyManager::MangleName(const CatalogEntryInfo &info) {
	return MangledEntryName(info);
}

MangledEntryName DependencyManager::MangleName(CatalogEntry &entry) {
	if (entry.type == CatalogType::DEPENDENCY_ENTRY) {
		auto &dependency_entry = entry.Cast<DependencyCatalogEntry>();
		return dependency_entry.MangledName();
	}
	auto type = entry.type;
	auto schema = GetSchema(entry);
	auto name = entry.name;
	CatalogEntryInfo info {type, schema, name};

	return MangleName(info);
}

DependencyInfo DependencyInfo::FromDependency(DependencyCatalogEntry &dep) {
	return DependencyInfo {/*dependent = */ dep.EntryInfo(),
	                       /*dependency = */ dep.FromInfo(),
	                       /*dependent_type = */ DependencyType::DEPENDENCY_REGULAR,
	                       /*dependency_type = */ dep.Type()};
}

DependencyInfo DependencyInfo::FromDependent(DependencyCatalogEntry &dep) {
	return DependencyInfo {/*dependent = */ dep.FromInfo(),
	                       /*dependency = */ dep.EntryInfo(),
	                       /*dependent_type = */ dep.Type(),
	                       /*dependency_type = */ DependencyType::DEPENDENCY_REGULAR};
}

// ----------- DEPENDENCY_MANAGER -----------

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

CatalogSet &DependencyManager::Dependents() {
	return dependents;
}

CatalogSet &DependencyManager::Dependencies() {
	return dependencies;
}

void DependencyManager::ScanSetInternal(CatalogTransaction transaction, const CatalogEntryInfo &info,
                                        bool scan_dependency, dependency_callback_t &callback) {
	catalog_entry_set_t other_entries;
	DependencyCatalogSet dependents(Dependents(), info);
	DependencyCatalogSet dependencies(Dependencies(), info);

	auto cb = [&](CatalogEntry &other) {
		D_ASSERT(other.type == CatalogType::DEPENDENCY_ENTRY);
		auto &other_entry = other.Cast<DependencyCatalogEntry>();

		other_entries.insert(other_entry);
		callback(other_entry);
	};

	if (scan_dependency) {
		dependencies.Scan(transaction, cb);
	} else {
		dependents.Scan(transaction, cb);
	}

#ifdef DEBUG
	// Verify some invariants
	// Every dependency should have a matching dependent in the other set
	// And vice versa
	auto mangled_name = MangleName(info);

	if (scan_dependency) {
		for (auto &entry : other_entries) {
			auto other_info = GetLookupProperties(entry);
			DependencyCatalogSet other_dependents(Dependents(), other_info);

			// Verify that the other half of the dependency also exists
			auto dependent = other_dependents.GetEntryDetailed(transaction, mangled_name);
			D_ASSERT(dependent.reason != CatalogSet::EntryLookup::FailureReason::NOT_PRESENT);
		}
	} else {
		for (auto &entry : other_entries) {
			auto other_info = GetLookupProperties(entry);
			DependencyCatalogSet other_dependencies(Dependencies(), other_info);

			// Verify that the other half of the dependent also exists
			auto dependency = other_dependencies.GetEntryDetailed(transaction, mangled_name);
			D_ASSERT(dependency.reason != CatalogSet::EntryLookup::FailureReason::NOT_PRESENT);
		}
	}
#endif
}

void DependencyManager::ScanDependents(CatalogTransaction transaction, const CatalogEntryInfo &info,
                                       dependency_callback_t &callback) {
	ScanSetInternal(transaction, info, false, callback);
}

void DependencyManager::ScanDependencies(CatalogTransaction transaction, const CatalogEntryInfo &info,
                                         dependency_callback_t &callback) {
	ScanSetInternal(transaction, info, true, callback);
}

void DependencyManager::RemoveDependency(CatalogTransaction transaction, const DependencyInfo &info) {
	auto &dependent = info.dependent;
	auto &dependency = info.dependency;

	// The dependents of the dependency (target)
	DependencyCatalogSet dependents(Dependents(), dependency);
	// The dependencies of the dependent (initiator)
	DependencyCatalogSet dependencies(Dependencies(), dependent);

	auto dependent_mangled = MangledEntryName(dependent);
	auto dependency_mangled = MangledEntryName(dependency);

	auto dependent_p = dependents.GetEntry(transaction, dependent_mangled);
	if (dependent_p) {
		// 'dependent' is no longer inhibiting the deletion of 'dependency'
		dependents.DropEntry(transaction, dependent_mangled, false);
	}
	auto dependency_p = dependencies.GetEntry(transaction, dependency_mangled);
	if (dependency_p) {
		// 'dependency' is no longer required by 'dependent'
		dependencies.DropEntry(transaction, dependency_mangled, false);
	}
}

void DependencyManager::CreateDependencyInternal(CatalogTransaction transaction, CatalogSet &catalog_set,
                                                 const CatalogEntryInfo &to, const CatalogEntryInfo &from,
                                                 DependencyType type) {
	DependencyCatalogSet dependents(catalog_set, from);

	auto dependent_p = make_uniq<DependencyCatalogEntry>(catalog, to, from, type);
	auto &dependent_name = dependent_p->MangledName();
	auto existing = dependents.GetEntry(transaction, dependent_name);
	if (existing) {
		return;
	}

	D_ASSERT(!StringUtil::CIEquals(dependent_name.name, MangleName(from).name));
	if (catalog.IsTemporaryCatalog()) {
		dependent_p->temporary = true;
	}
	dependents.CreateEntry(transaction, dependent_name, std::move(dependent_p));
}

void DependencyManager::CreateDependency(CatalogTransaction transaction, const DependencyInfo &info) {
	auto &dependent = info.dependent;
	auto &dependency = info.dependency;

	// Create an entry in the dependents map of the object that is the target of the dependency
	CreateDependencyInternal(transaction, Dependents(), dependent, dependency, info.dependent_type);
	// Create an entry in the dependencies map of the object that is targeting another entry
	CreateDependencyInternal(transaction, Dependencies(), dependency, dependent, info.dependency_type);
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
		// Create the dependent and complete the link by creating the dependency as well
		DependencyInfo info {/*dependent = */ GetLookupProperties(object),
		                     /*dependency = */ GetLookupProperties(dependency),
		                     /*dependent_type = */ dependency_type,
		                     /*dependency_type = */ DependencyType::DEPENDENCY_REGULAR};
		CreateDependency(transaction, info);
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

CatalogEntryInfo DependencyManager::GetLookupProperties(CatalogEntry &entry) {
	if (entry.type == CatalogType::DEPENDENCY_ENTRY) {
		auto &dependency_entry = entry.Cast<DependencyCatalogEntry>();

		auto &schema = dependency_entry.EntrySchema();
		auto &name = dependency_entry.EntryName();
		auto type = dependency_entry.EntryType();
		return CatalogEntryInfo {type, schema, name};
	} else {
		auto schema = DependencyManager::GetSchema(entry);
		auto &name = entry.name;
		auto &type = entry.type;
		return CatalogEntryInfo {type, schema, name};
	}
}

optional_ptr<CatalogEntry> DependencyManager::LookupEntry(CatalogTransaction transaction, CatalogEntry &dependency) {
	auto info = GetLookupProperties(dependency);

	auto &type = info.type;
	auto &schema = info.schema;
	auto &name = info.name;

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
	// Collect the dependencies
	vector<DependencyInfo> to_remove;

	auto info = GetLookupProperties(object);
	ScanDependencies(transaction, info,
	                 [&](DependencyCatalogEntry &dep) { to_remove.push_back(DependencyInfo::FromDependency(dep)); });
	ScanDependents(transaction, info,
	               [&](DependencyCatalogEntry &dep) { to_remove.push_back(DependencyInfo::FromDependent(dep)); });

	// Remove the dependency entries
	for (auto &dep : to_remove) {
		RemoveDependency(transaction, dep);
	}
}

void DependencyManager::DropObject(CatalogTransaction transaction, CatalogEntry &object, bool cascade) {
	if (IsSystemEntry(object)) {
		// Don't do anything for this
		return;
	}

	auto info = GetLookupProperties(object);
	// Check if there are any entries that block the DROP because they still depend on the object
	catalog_entry_set_t to_drop;
	ScanDependents(transaction, info, [&](DependencyCatalogEntry &dep) {
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

	auto info = GetLookupProperties(old_obj);
	dependency_set_t owned_objects;
	ScanDependents(transaction, info, [&](DependencyCatalogEntry &dep) {
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
	ScanDependencies(transaction, info, [&](DependencyCatalogEntry &dep) {
		auto entry = LookupEntry(transaction, dep);
		if (!entry) {
			return;
		}
		DependencyCatalogSet other_dependents(Dependents(), dep.EntryInfo());

		// Find the dependent entry so we can properly restore the type it has
		auto old_mangled = MangleName(old_obj);
		auto &dependent = other_dependents.GetEntry(transaction, old_mangled)->Cast<DependencyCatalogEntry>();
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
		DependencyInfo info {/*dependent = */ GetLookupProperties(new_obj),
		                     /*dependency = */ GetLookupProperties(other),
		                     /*dependent_type = */ dep.dependency_type,
		                     /*dependency_type = */ DependencyType::DEPENDENCY_REGULAR};
		CreateDependency(transaction, info);
	}

	// For all the objects we own, re establish the dependency of the owner on the object
	for (auto &object : owned_objects) {
		auto &entry = object.entry.get();
		{
			DependencyInfo info {/*dependent = */ GetLookupProperties(new_obj),
			                     /*dependency = */ GetLookupProperties(entry),
			                     /*dependent_type = */ DependencyType::DEPENDENCY_OWNED_BY,
			                     /*dependency_type = */ DependencyType::DEPENDENCY_REGULAR};
			CreateDependency(transaction, info);
		}
		{
			DependencyInfo info {/*dependent = */ GetLookupProperties(entry),
			                     /*dependency = */ GetLookupProperties(new_obj),
			                     /*dependent_type = */ DependencyType::DEPENDENCY_OWNS,
			                     /*dependency_type = */ DependencyType::DEPENDENCY_REGULAR};
			CreateDependency(transaction, info);
		}
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
		auto entry_info = GetLookupProperties(entry);
		// Scan all the dependents of the entry
		ScanDependents(transaction, entry_info, [&](DependencyCatalogEntry &dependent) {
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
	auto owner_info = GetLookupProperties(owner);
	ScanDependents(transaction, owner_info, [&](DependencyCatalogEntry &dep) {
		if (dep.Type() == DependencyType::DEPENDENCY_OWNED_BY) {
			throw DependencyException(owner.name + " already owned by " + dep.EntryName());
		}
	});

	// If the entry is already owned, throw an error
	auto entry_info = GetLookupProperties(entry);
	ScanDependents(transaction, entry_info, [&](DependencyCatalogEntry &other) {
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
	{
		DependencyInfo info {/*dependent = */ GetLookupProperties(owner),
		                     /*dependency = */ GetLookupProperties(entry),
		                     /*dependent_type = */ DependencyType::DEPENDENCY_OWNED_BY,
		                     /*dependency_type = */ DependencyType::DEPENDENCY_REGULAR};
		CreateDependency(transaction, info);
	}
	{
		DependencyInfo info {/*dependent = */ GetLookupProperties(entry),
		                     /*dependency = */ GetLookupProperties(owner),
		                     /*dependent_type = */ DependencyType::DEPENDENCY_OWNS,
		                     /*dependency_type = */ DependencyType::DEPENDENCY_REGULAR};
		CreateDependency(transaction, info);
	}
}

} // namespace duckdb
