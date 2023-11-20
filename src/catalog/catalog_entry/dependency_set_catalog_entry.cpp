#include "duckdb/catalog/catalog_entry/dependency_set_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/dependency_catalog_entry.hpp"
#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/catalog/dependency_manager.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/duck_catalog.hpp"

namespace duckdb {

DependencySetCatalogEntry::DependencySetCatalogEntry(DuckCatalog &catalog, DependencyManager &dependency_manager,
                                                     CatalogEntryInfo info)
    : catalog(catalog), mangled_name(DependencyManager::MangleName(info.type, info.schema, info.name)), info(info),
      dependencies(dependency_manager.dependencies, mangled_name, info),
      dependents(dependency_manager.dependents, mangled_name, info), dependency_manager(dependency_manager) {
}

DependencyCatalogSet &DependencySetCatalogEntry::Dependencies() {
	return dependencies;
}

DependencyCatalogSet &DependencySetCatalogEntry::Dependents() {
	return dependents;
}

DependencyManager &DependencySetCatalogEntry::Manager() {
	return dependency_manager;
}

void DependencySetCatalogEntry::ScanSetInternal(CatalogTransaction transaction, bool dependencies,
                                                dependency_callback_t &callback) {
	catalog_entry_set_t other_entries;
	auto cb = [&](CatalogEntry &other) {
		D_ASSERT(other.type == CatalogType::DEPENDENCY_ENTRY);
		auto &other_entry = other.Cast<DependencyCatalogEntry>();

		other_entries.insert(other_entry);
		callback(other_entry);
	};

	if (dependencies) {
		this->dependencies.Scan(transaction, cb);
	} else {
		this->dependents.Scan(transaction, cb);
	}

#ifdef DEBUG
	// Verify some invariants
	// Every dependency should have a matching dependent in the other set
	// And vice versa
	if (dependencies) {
		for (auto &entry : other_entries) {
			auto dependency_set = dependency_manager.GetDependencySet(transaction, entry);
			D_ASSERT(dependency_set.IsDependencyOf(transaction, *this));
		}
	} else {
		for (auto &entry : other_entries) {
			auto dependency_set = dependency_manager.GetDependencySet(transaction, entry);
			D_ASSERT(dependency_set.HasDependencyOn(transaction, *this));
		}
	}
#endif
}

void DependencySetCatalogEntry::ScanDependents(CatalogTransaction transaction, dependency_callback_t &callback) {
	ScanSetInternal(transaction, false, callback);
}

void DependencySetCatalogEntry::ScanDependencies(CatalogTransaction transaction, dependency_callback_t &callback) {
	ScanSetInternal(transaction, true, callback);
}

const MangledEntryName &DependencySetCatalogEntry::MangledName() const {
	return mangled_name;
}

CatalogType DependencySetCatalogEntry::EntryType() const {
	return info.type;
}

const string &DependencySetCatalogEntry::EntrySchema() const {
	return info.schema;
}

const string &DependencySetCatalogEntry::EntryName() const {
	return info.name;
}

// Add from a single CatalogEntry
DependencyCatalogEntry &DependencySetCatalogEntry::AddDependency(CatalogTransaction transaction,
                                                                 const MangledEntryName &mangled_name,
                                                                 CatalogEntryInfo info, DependencyType type) {
	{
		auto dep = dependencies.GetEntry(transaction, mangled_name);
		if (dep) {
			return dep->Cast<DependencyCatalogEntry>();
		}
	}

	auto dependency_p =
	    make_uniq<DependencyCatalogEntry>(DependencyLinkSide::DEPENDENCY, catalog, dependency_manager, info, type);
	auto &dependency_name = dependency_p->MangledName();
	D_ASSERT(!StringUtil::CIEquals(dependency_name.name, MangledName().name));
	if (catalog.IsTemporaryCatalog()) {
		dependency_p->temporary = true;
	}
	auto &dependency = *dependency_p;

	dependencies.CreateEntry(transaction, dependency_name, std::move(dependency_p));
	return dependency;
}

DependencyCatalogEntry &DependencySetCatalogEntry::AddDependency(CatalogTransaction transaction,
                                                                 DependencySetCatalogEntry &to_add,
                                                                 DependencyType type) {
	auto &mangled_name = to_add.MangledName();
	auto entry_type = to_add.EntryType();
	auto &entry_schema = to_add.EntrySchema();
	auto &entry_name = to_add.EntryName();
	CatalogEntryInfo info {entry_type, entry_schema, entry_name};
	return AddDependency(transaction, mangled_name, info, type);
}

DependencyCatalogEntry &DependencySetCatalogEntry::AddDependency(CatalogTransaction transaction, CatalogEntry &to_add,
                                                                 DependencyType type) {
	auto mangled_name = DependencyManager::MangleName(to_add);

	auto info = DependencyManager::GetLookupProperties(to_add);
	return AddDependency(transaction, mangled_name, info, type);
}

DependencyCatalogEntry &DependencySetCatalogEntry::AddDependent(CatalogTransaction transaction,
                                                                const MangledEntryName &mangled_name,
                                                                CatalogEntryInfo info, DependencyType type) {
	{
		auto dep = dependents.GetEntry(transaction, mangled_name);
		if (dep) {
			return dep->Cast<DependencyCatalogEntry>();
		}
	}
	auto dependent_p =
	    make_uniq<DependencyCatalogEntry>(DependencyLinkSide::DEPENDENT, catalog, dependency_manager, info, type);
	auto &dependent_name = dependent_p->MangledName();
	D_ASSERT(!StringUtil::CIEquals(dependent_name.name, MangledName().name));
	if (catalog.IsTemporaryCatalog()) {
		dependent_p->temporary = true;
	}
	auto &dependent = *dependent_p;
	dependents.CreateEntry(transaction, dependent_name, std::move(dependent_p));
	return dependent;
}

DependencyCatalogEntry &DependencySetCatalogEntry::AddDependent(CatalogTransaction transaction,
                                                                DependencySetCatalogEntry &to_add,
                                                                DependencyType type) {
	auto &mangled_name = to_add.MangledName();
	auto entry_type = to_add.EntryType();
	auto &entry_schema = to_add.EntrySchema();
	auto &entry_name = to_add.EntryName();

	CatalogEntryInfo info {entry_type, entry_schema, entry_name};
	return AddDependency(transaction, mangled_name, info, type);
}

DependencyCatalogEntry &DependencySetCatalogEntry::AddDependent(CatalogTransaction transaction, CatalogEntry &to_add,
                                                                DependencyType type) {
	auto mangled_name = DependencyManager::MangleName(to_add);

	auto info = DependencyManager::GetLookupProperties(to_add);
	return AddDependent(transaction, mangled_name, info, type);
}

// Add from a Dependency
DependencyCatalogEntry &DependencySetCatalogEntry::AddDependency(CatalogTransaction transaction, Dependency to_add) {
	return AddDependency(transaction, to_add.entry, to_add.dependency_type);
}
DependencyCatalogEntry &DependencySetCatalogEntry::AddDependent(CatalogTransaction transaction, Dependency to_add) {
	return AddDependent(transaction, to_add.entry, to_add.dependency_type);
}

// Remove dependency from a DependencyEntry
void DependencySetCatalogEntry::RemoveDependency(CatalogTransaction transaction, const MangledEntryName &mangled_name) {
	auto entry = dependencies.GetEntry(transaction, mangled_name);
	if (!entry) {
		// Already deleted
		return;
	}
	dependencies.DropEntry(transaction, mangled_name, false);
}

// Remove dependent from a DependencyEntry
void DependencySetCatalogEntry::RemoveDependent(CatalogTransaction transaction, const MangledEntryName &mangled_name) {
	auto entry = dependents.GetEntry(transaction, mangled_name);
	if (!entry) {
		// Already deleted
		return;
	}
	dependents.DropEntry(transaction, mangled_name, false);
}

// Remove dependency from a DependencyEntry
void DependencySetCatalogEntry::RemoveDependency(CatalogTransaction transaction,
                                                 DependencySetCatalogEntry &dependency) {
	auto &mangled_name = dependency.MangledName();
	RemoveDependency(transaction, mangled_name);
}

// Remove dependent from a DependencyEntry
void DependencySetCatalogEntry::RemoveDependent(CatalogTransaction transaction, DependencySetCatalogEntry &dependent) {
	auto &mangled_name = dependent.MangledName();
	RemoveDependent(transaction, mangled_name);
}

// Remove dependency from a DependencyEntry
void DependencySetCatalogEntry::RemoveDependency(CatalogTransaction transaction, CatalogEntry &dependency) {
	D_ASSERT(dependency.type == CatalogType::DEPENDENCY_ENTRY);
	auto mangled_name = DependencyManager::MangleName(dependency);
	RemoveDependency(transaction, mangled_name);
}

// Remove dependent from a DependencyEntry
void DependencySetCatalogEntry::RemoveDependent(CatalogTransaction transaction, CatalogEntry &dependent) {
	D_ASSERT(dependent.type == CatalogType::DEPENDENCY_ENTRY);
	auto mangled_name = DependencyManager::MangleName(dependent);
	RemoveDependent(transaction, mangled_name);
}

DependencyCatalogEntry &DependencySetCatalogEntry::GetDependency(CatalogTransaction &transaction,
                                                                 CatalogEntry &object) {
	auto mangled_name = DependencyManager::MangleName(object);
	auto dependency = dependencies.GetEntry(transaction, mangled_name);
	return dependency->Cast<DependencyCatalogEntry>();
}

DependencyCatalogEntry &DependencySetCatalogEntry::GetDependent(CatalogTransaction &transaction, CatalogEntry &object) {
	auto mangled_name = DependencyManager::MangleName(object);
	auto dependent = dependents.GetEntry(transaction, mangled_name);
	return dependent->Cast<DependencyCatalogEntry>();
}

bool DependencySetCatalogEntry::IsDependencyOf(CatalogTransaction transaction, const MangledEntryName &mangled_name) {
	auto dependent = dependents.GetEntryDetailed(transaction, mangled_name);

	// It's fine if the entry is already deleted
	return dependent.reason != CatalogSet::EntryLookup::FailureReason::NOT_PRESENT;
}

bool DependencySetCatalogEntry::IsDependencyOf(CatalogTransaction transaction, CatalogEntry &entry) {
	auto mangled_name = DependencyManager::MangleName(entry);
	return IsDependencyOf(transaction, mangled_name);
}

bool DependencySetCatalogEntry::IsDependencyOf(CatalogTransaction transaction, DependencySetCatalogEntry &other) {
	auto &mangled_name = other.MangledName();
	return IsDependencyOf(transaction, mangled_name);
}

bool DependencySetCatalogEntry::HasDependencyOn(CatalogTransaction transaction, const MangledEntryName &mangled_name) {
	auto dependency = dependencies.GetEntryDetailed(transaction, mangled_name);
	return dependency.reason != CatalogSet::EntryLookup::FailureReason::NOT_PRESENT;
}

bool DependencySetCatalogEntry::HasDependencyOn(CatalogTransaction transaction, DependencySetCatalogEntry &other) {
	auto &mangled_name = other.MangledName();
	return HasDependencyOn(transaction, mangled_name);
}

bool DependencySetCatalogEntry::HasDependencyOn(CatalogTransaction transaction, CatalogEntry &entry) {
	auto mangled_name = DependencyManager::MangleName(entry);
	return HasDependencyOn(transaction, mangled_name);
}

static string FormatString(string input) {
	for (size_t i = 0; i < input.size(); i++) {
		if (input[i] == '\0') {
			input[i] = '_';
		}
	}
	return input;
}

void DependencySetCatalogEntry::PrintDependencies(CatalogTransaction transaction) {
	Printer::Print(StringUtil::Format("Dependencies of %s", FormatString(mangled_name.name)));
	dependencies.Scan(transaction, [&](CatalogEntry &dependency) {
		auto &dep = dependency.Cast<DependencyCatalogEntry>();
		auto &name = dep.EntryName();
		auto &schema = dep.EntrySchema();
		auto type = dep.EntryType();
		Printer::Print(StringUtil::Format("Schema: %s | Name: %s | Type: %s | DependencyType: %s", schema, name,
		                                  CatalogTypeToString(type), EnumUtil::ToString(dep.Type())));
	});
}
void DependencySetCatalogEntry::PrintDependents(CatalogTransaction transaction) {
	Printer::Print(StringUtil::Format("Dependents of %s", FormatString(mangled_name.name)));
	dependents.Scan(transaction, [&](CatalogEntry &dependent) {
		auto &dep = dependent.Cast<DependencyCatalogEntry>();
		auto &name = dep.EntryName();
		auto &schema = dep.EntrySchema();
		auto type = dep.EntryType();
		Printer::Print(StringUtil::Format("Schema: %s | Name: %s | Type: %s | DependencyType: %s", schema, name,
		                                  CatalogTypeToString(type), EnumUtil::ToString(dep.Type())));
	});
}

} // namespace duckdb
