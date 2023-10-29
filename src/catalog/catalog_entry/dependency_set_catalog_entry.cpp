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
                                                     CatalogEntry &object)
    : InCatalogEntry(CatalogType::DEPENDENCY_SET, catalog, DependencyManager::MangleName(object)),
      entry_name(object.name), schema(DependencyManager::GetSchema(object)), entry_type(object.type),
      dependencies(catalog), dependents(catalog), dependency_manager(dependency_manager) {
}

CatalogSet &DependencySetCatalogEntry::Dependencies() {
	return dependencies;
}

CatalogSet &DependencySetCatalogEntry::Dependents() {
	return dependents;
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
void DependencySetCatalogEntry::AddDependency(CatalogTransaction transaction, CatalogEntry &to_add,
                                              DependencyType type) {
	static const DependencyList EMPTY_DEPENDENCIES;
	auto dependency = make_uniq<DependencyCatalogEntry>(catalog, to_add, type);
	auto dependency_name = dependency->name;
	D_ASSERT(dependency_name != name);
	if (catalog.IsTemporaryCatalog()) {
		dependency->temporary = true;
	}
	dependencies.CreateEntry(transaction, dependency_name, std::move(dependency), EMPTY_DEPENDENCIES);
}
void DependencySetCatalogEntry::AddDependent(CatalogTransaction transaction, CatalogEntry &to_add,
                                             DependencyType type) {
	static const DependencyList EMPTY_DEPENDENCIES;
	auto dependent = make_uniq<DependencyCatalogEntry>(catalog, to_add, type);
	auto dependent_name = dependent->name;
	D_ASSERT(dependent_name != name);
	if (catalog.IsTemporaryCatalog()) {
		dependent->temporary = true;
	}
	dependents.CreateEntry(transaction, dependent_name, std::move(dependent), EMPTY_DEPENDENCIES);
}

// Add from a Dependency
void DependencySetCatalogEntry::AddDependency(CatalogTransaction transaction, Dependency to_add) {
	AddDependency(transaction, to_add.entry, to_add.dependency_type);
}
void DependencySetCatalogEntry::AddDependent(CatalogTransaction transaction, Dependency to_add) {
	AddDependent(transaction, to_add.entry, to_add.dependency_type);
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
