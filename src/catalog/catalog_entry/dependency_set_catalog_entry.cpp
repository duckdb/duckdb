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
                                                     const string &name)
    : InCatalogEntry(CatalogType::DEPENDENCY_SET, catalog, name), name(name), dependencies(catalog),
      dependents(catalog), dependency_manager(dependency_manager) {
}

CatalogSet &DependencySetCatalogEntry::Dependencies() {
	return dependencies;
}

CatalogSet &DependencySetCatalogEntry::Dependents() {
	return dependents;
}

void DependencySetCatalogEntry::ScanDependents(
    CatalogTransaction transaction,
    const std::function<void(DependencyCatalogEntry &, DependencySetCatalogEntry &)> &callback) {
	dependents.Scan(transaction, [&](CatalogEntry &other) {
		D_ASSERT(other.type == CatalogType::DEPENDENCY_ENTRY);
		auto &other_entry = other.Cast<DependencyCatalogEntry>();
		auto other_connections_p = dependency_manager.GetDependencySet(transaction, other);
		if (!other_connections_p) {
			// Already deleted
			return;
		}
		auto &other_connections = *other_connections_p;
		callback(other_entry, other_connections);
	});
}

void DependencySetCatalogEntry::ScanDependencies(
    CatalogTransaction transaction,
    const std::function<void(DependencyCatalogEntry &, DependencySetCatalogEntry &)> &callback) {
	dependencies.Scan(transaction, [&](CatalogEntry &other) {
		D_ASSERT(other.type == CatalogType::DEPENDENCY_ENTRY);
		auto &other_entry = other.Cast<DependencyCatalogEntry>();
		auto other_connections_p = dependency_manager.GetDependencySet(transaction, other);
		if (!other_connections_p) {
			// Already deleted
			return;
		}
		auto &other_connections = *other_connections_p;
		callback(other_entry, other_connections);
	});
}

DependencySetCatalogEntry::~DependencySetCatalogEntry() {
}

const string &DependencySetCatalogEntry::MangledName() const {
	return name;
}

// Add from a Dependency Set
void DependencySetCatalogEntry::AddDependencies(CatalogTransaction transaction, dependency_set_t &to_add) {
	DependencyList empty_dependencies;
	for (auto &dep : to_add) {
		auto &entry = dep.entry;
		AddDependency(transaction, entry, dep.dependency_type);
	}
}
void DependencySetCatalogEntry::AddDependents(CatalogTransaction transaction, dependency_set_t &to_add) {
	DependencyList empty_dependencies;
	for (auto &dep : to_add) {
		auto &entry = dep.entry;
		AddDependent(transaction, entry, dep.dependency_type);
	}
}

// Add from a DependencyList
void DependencySetCatalogEntry::AddDependencies(CatalogTransaction transaction, DependencyList &to_add) {
	for (auto &entry : to_add.set) {
		AddDependency(transaction, entry);
	}
}
void DependencySetCatalogEntry::AddDependents(CatalogTransaction transaction, DependencyList &to_add) {
	for (auto &entry : to_add.set) {
		AddDependent(transaction, entry);
	}
}

// Add from a single CatalogEntry
void DependencySetCatalogEntry::AddDependency(CatalogTransaction transaction, CatalogEntry &to_add,
                                              DependencyType type) {
	static DependencyList empty_dependencies;
	auto dependency = make_uniq<DependencyCatalogEntry>(DependencyConnectionType::DEPENDENCY, catalog, to_add, type);
	auto dependency_name = dependency->name;
	D_ASSERT(dependency_name != name);
	if (catalog.IsTemporaryCatalog()) {
		dependency->temporary = true;
	}
	dependencies.CreateEntry(transaction, dependency_name, std::move(dependency), empty_dependencies);
}
void DependencySetCatalogEntry::AddDependent(CatalogTransaction transaction, CatalogEntry &to_add,
                                             DependencyType type) {
	static DependencyList empty_dependencies;
	auto dependent = make_uniq<DependencyCatalogEntry>(DependencyConnectionType::DEPENDENT, catalog, to_add, type);
	auto dependent_name = dependent->name;
	D_ASSERT(dependent_name != name);
	if (catalog.IsTemporaryCatalog()) {
		dependent->temporary = true;
	}
	dependents.CreateEntry(transaction, dependent_name, std::move(dependent), empty_dependencies);
}

// Add from a Dependency
void DependencySetCatalogEntry::AddDependency(CatalogTransaction transaction, Dependency to_add) {
	AddDependency(transaction, to_add.entry, to_add.dependency_type);
}
void DependencySetCatalogEntry::AddDependent(CatalogTransaction transaction, Dependency to_add) {
	AddDependent(transaction, to_add.entry, to_add.dependency_type);
}

static bool SkipDependencyRemoval(CatalogEntry &dependency) {
	if (dependency.type != CatalogType::DEPENDENCY_ENTRY) {
		return false;
	}
	auto &dep = dependency.Cast<DependencyCatalogEntry>();

	// This link is not completed, so there is no dependency to remove
	return dep.Type() == DependencyType::DEPENDENCY_OWNS;
}

// Remove dependency from a DependencyEntry
void DependencySetCatalogEntry::RemoveDependency(CatalogTransaction transaction, CatalogEntry &dependency) {
	D_ASSERT(dependency.type == CatalogType::DEPENDENCY_ENTRY || dependency.type == CatalogType::DEPENDENCY_SET);
	if (SkipDependencyRemoval(dependency)) {
		D_ASSERT(!HasDependencyOnInternal(dependency));
		return;
	}
	auto &name = dependency.name;
	dependencies.DropEntry(transaction, name, false);
}

// Remove dependent from a DependencyEntry
void DependencySetCatalogEntry::RemoveDependent(CatalogTransaction transaction, CatalogEntry &dependent) {
	D_ASSERT(dependent.type == CatalogType::DEPENDENCY_ENTRY || dependent.type == CatalogType::DEPENDENCY_SET);
	auto &name = dependent.name;
	dependents.DropEntry(transaction, name, false);
}

bool DependencySetCatalogEntry::IsDependencyOf(CatalogEntry &entry) {
	bool is_dependency_of = false;
	dependents.Scan([&](CatalogEntry &dependent) {
		auto &dependent_entry = dependent.Cast<DependencyCatalogEntry>();
		if (dependent_entry.MangledName() != DependencyManager::MangleName(entry)) {
			return;
		}
		// 'entry' is a dependency of this object
		is_dependency_of = true;
	});
	return is_dependency_of;
}

bool DependencySetCatalogEntry::HasDependencyOnInternal(CatalogEntry &entry) {
	bool has_dependency_on = false;
	dependencies.Scan([&](CatalogEntry &dependency) {
		auto &dependency_entry = dependency.Cast<DependencyCatalogEntry>();
		if (dependency_entry.MangledName() != DependencyManager::MangleName(entry)) {
			return;
		}
		// this object has a dependency on 'entry'
		has_dependency_on = true;
	});
	return has_dependency_on;
}

bool DependencySetCatalogEntry::HasDependencyOn(CatalogEntry &entry, DependencyType dependent_type) {
	if (dependent_type == DependencyType::DEPENDENCY_OWNS) {
		// This link is deliberately left uncompleted
		return true;
	}

	return HasDependencyOnInternal(entry);
}

static string FormatString(string input) {
	std::replace(input.begin(), input.end(), '\0', '_');
	return input;
}

void DependencySetCatalogEntry::PrintDependencies() {
	Printer::Print(StringUtil::Format("Dependencies of %s", FormatString(name)));
	dependencies.Scan([&](CatalogEntry &dependency) {
		auto &dep = dependency.Cast<DependencyCatalogEntry>();
		auto &name = dep.EntryName();
		auto &schema = dep.EntrySchema();
		auto type = dep.EntryType();
		Printer::Print(StringUtil::Format("Schema: %s | Name: %s | Type: %s | DependencyType: %s", schema, name,
		                                  CatalogTypeToString(type), EnumUtil::ToString(dep.Type())));
	});
}
void DependencySetCatalogEntry::PrintDependents() {
	Printer::Print(StringUtil::Format("Dependents of %s", FormatString(name)));
	dependents.Scan([&](CatalogEntry &dependent) {
		auto &dep = dependent.Cast<DependencyCatalogEntry>();
		auto &name = dep.EntryName();
		auto &schema = dep.EntrySchema();
		auto type = dep.EntryType();
		Printer::Print(StringUtil::Format("Schema: %s | Name: %s | Type: %s | DependencyType: %s", schema, name,
		                                  CatalogTypeToString(type), EnumUtil::ToString(dep.Type())));
	});
}

} // namespace duckdb
