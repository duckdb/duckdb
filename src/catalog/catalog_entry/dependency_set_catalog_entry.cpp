#include "duckdb/catalog/catalog_entry/dependency_set_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/dependency_catalog_entry.hpp"
#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/catalog/mapping_value.hpp"
#include "duckdb/catalog/dependency_manager.hpp"
#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

DependencySetCatalogEntry::DependencySetCatalogEntry(Catalog &catalog, DependencyManager &dependency_manager,
                                                     CatalogType entry_type, const string &entry_schema,
                                                     const string &entry_name)
    : InCatalogEntry(CatalogType::DEPENDENCY_SET, catalog,
                     DependencyManager::MangleName(entry_type, entry_schema, entry_name)),
      entry_name(entry_name), schema(entry_schema), entry_type(entry_type), dependencies(catalog), dependents(catalog),
      dependency_manager(dependency_manager) {
}

CatalogSet &DependencySetCatalogEntry::Dependencies() {
	return dependencies;
}

CatalogSet &DependencySetCatalogEntry::Dependents() {
	return dependents;
}

DependencyManager &DependencySetCatalogEntry::Manager() {
	return dependency_manager;
}

void DependencySetCatalogEntry::ScanSetInternal(CatalogTransaction transaction, bool dependencies,
                                                dependency_callback_t &callback) {
	auto cb = [&](CatalogEntry &other) {
		D_ASSERT(other.type == CatalogType::DEPENDENCY_ENTRY);
		auto &other_entry = other.Cast<DependencyCatalogEntry>();
		auto other_connections_p = dependency_manager.GetDependencySet(transaction, other);
		if (!other_connections_p) {
			// Already deleted
			return;
		}
		auto &other_connections = *other_connections_p;
		(void)other_connections;

		// Assert some invariants of the connections
		if (dependencies) {
			D_ASSERT(other_connections.IsDependencyOf(transaction, *this));
		} else {
			D_ASSERT(other_connections.HasDependencyOn(transaction, *this, other_entry.Type()));
		}
		callback(other_entry);
	};

	if (dependencies) {
		this->dependencies.Scan(transaction, cb);
	} else {
		this->dependents.Scan(transaction, cb);
	}
}

void DependencySetCatalogEntry::ScanDependents(CatalogTransaction transaction, dependency_callback_t &callback) {
	ScanSetInternal(transaction, false, callback);
}

void DependencySetCatalogEntry::ScanDependencies(CatalogTransaction transaction, dependency_callback_t &callback) {
	ScanSetInternal(transaction, true, callback);
}

DependencySetCatalogEntry::~DependencySetCatalogEntry() {
}

const string &DependencySetCatalogEntry::MangledName() const {
	return name;
}

CatalogType DependencySetCatalogEntry::EntryType() const {
	return entry_type;
}

const string &DependencySetCatalogEntry::EntrySchema() const {
	return schema;
}

const string &DependencySetCatalogEntry::EntryName() const {
	return entry_name;
}

// Add from a Dependency Set
void DependencySetCatalogEntry::AddDependencies(CatalogTransaction transaction, const dependency_set_t &to_add) {
	for (auto &dep : to_add) {
		auto &entry = dep.entry;
		AddDependency(transaction, entry, dep.dependency_type);
	}
}
void DependencySetCatalogEntry::AddDependents(CatalogTransaction transaction, const dependency_set_t &to_add) {
	for (auto &dep : to_add) {
		auto &entry = dep.entry;
		AddDependent(transaction, entry, dep.dependency_type);
	}
}

// Add from a DependencyList
void DependencySetCatalogEntry::AddDependencies(CatalogTransaction transaction, const DependencyList &to_add) {
	for (auto &entry : to_add.set) {
		AddDependency(transaction, entry);
	}
}
void DependencySetCatalogEntry::AddDependents(CatalogTransaction transaction, const DependencyList &to_add) {
	for (auto &entry : to_add.set) {
		AddDependent(transaction, entry);
	}
}

// Add from a single CatalogEntry
DependencyCatalogEntry &DependencySetCatalogEntry::AddDependency(CatalogTransaction transaction, CatalogEntry &to_add,
                                                                 DependencyType type) {
	static const DependencyList EMPTY_DEPENDENCIES;

	{
		auto mangled_name = DependencyManager::MangleName(to_add);
		auto dep = dependencies.GetEntry(transaction, mangled_name);
		if (dep) {
			return dep->Cast<DependencyCatalogEntry>();
		}
	}

	string entry_schema;
	string entry_name;
	CatalogType entry_type;

	DependencyManager::GetLookupProperties(to_add, entry_schema, entry_name, entry_type);
	auto dependency_p = make_uniq<DependencyCatalogEntry>(DependencyLinkSide::DEPENDENCY, catalog, *this, entry_type,
	                                                      entry_schema, entry_name, type);
	auto dependency_name = dependency_p->name;
	D_ASSERT(dependency_name != name);
	if (catalog.IsTemporaryCatalog()) {
		dependency_p->temporary = true;
	}
	auto &dependency = *dependency_p;
	dependencies.CreateEntry(transaction, dependency_name, std::move(dependency_p), EMPTY_DEPENDENCIES);
	return dependency;
}

DependencyCatalogEntry &DependencySetCatalogEntry::AddDependent(CatalogTransaction transaction, CatalogEntry &to_add,
                                                                DependencyType type) {
	static const DependencyList EMPTY_DEPENDENCIES;

	{
		auto mangled_name = DependencyManager::MangleName(to_add);
		auto dep = dependents.GetEntry(transaction, mangled_name);
		if (dep) {
			return dep->Cast<DependencyCatalogEntry>();
		}
	}

	string entry_schema;
	string entry_name;
	CatalogType entry_type;

	DependencyManager::GetLookupProperties(to_add, entry_schema, entry_name, entry_type);
	auto dependent_p = make_uniq<DependencyCatalogEntry>(DependencyLinkSide::DEPENDENT, catalog, *this, entry_type,
	                                                     entry_schema, entry_name, type);
	auto dependent_name = dependent_p->name;
	D_ASSERT(dependent_name != name);
	if (catalog.IsTemporaryCatalog()) {
		dependent_p->temporary = true;
	}
	auto &dependent = *dependent_p;
	dependents.CreateEntry(transaction, dependent_name, std::move(dependent_p), EMPTY_DEPENDENCIES);
	return dependent;
}

// Add from a Dependency
DependencyCatalogEntry &DependencySetCatalogEntry::AddDependency(CatalogTransaction transaction, Dependency to_add) {
	return AddDependency(transaction, to_add.entry, to_add.dependency_type);
}
DependencyCatalogEntry &DependencySetCatalogEntry::AddDependent(CatalogTransaction transaction, Dependency to_add) {
	return AddDependent(transaction, to_add.entry, to_add.dependency_type);
}

// Remove dependency from a DependencyEntry
void DependencySetCatalogEntry::RemoveDependency(CatalogTransaction transaction, CatalogEntry &dependency) {
	D_ASSERT(dependency.type == CatalogType::DEPENDENCY_ENTRY || dependency.type == CatalogType::DEPENDENCY_SET);
	auto &name = dependency.name;
	dependencies.DropEntry(transaction, name, false);
}

// Remove dependent from a DependencyEntry
void DependencySetCatalogEntry::RemoveDependent(CatalogTransaction transaction, CatalogEntry &dependent) {
	D_ASSERT(dependent.type == CatalogType::DEPENDENCY_ENTRY || dependent.type == CatalogType::DEPENDENCY_SET);
	auto &name = dependent.name;
	dependents.DropEntry(transaction, name, false);
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

bool DependencySetCatalogEntry::IsDependencyOf(CatalogTransaction transaction, CatalogEntry &entry) {
	auto mangled_name = DependencyManager::MangleName(entry);
	auto dependent = dependents.GetEntry(transaction, mangled_name);
	return dependent != nullptr;
}

bool DependencySetCatalogEntry::HasDependencyOn(CatalogTransaction transaction, CatalogEntry &entry,
                                                DependencyType dependent_type) {
	auto mangled_name = DependencyManager::MangleName(entry);
	auto dependency = dependencies.GetEntry(transaction, mangled_name);
	return dependency != nullptr;
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
	Printer::Print(StringUtil::Format("Dependencies of %s", FormatString(name)));
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
	Printer::Print(StringUtil::Format("Dependents of %s", FormatString(name)));
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
