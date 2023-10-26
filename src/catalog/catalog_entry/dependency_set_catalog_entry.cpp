#include "duckdb/catalog/catalog_entry/dependency_set_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/dependency_catalog_entry.hpp"
#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"

namespace duckdb {

DependencySetCatalogEntry::DependencySetCatalogEntry(Catalog &catalog, const string &name)
    : InCatalogEntry(CatalogType::DEPENDENCY_SET, catalog, name), dependencies(catalog), dependents(catalog) {
}

CatalogSet &DependencySetCatalogEntry::Dependencies() {
	return dependencies;
}

CatalogSet &DependencySetCatalogEntry::Dependents() {
	return dependents;
}

DependencySetCatalogEntry::~DependencySetCatalogEntry() {
}

// From Dependency Set
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

// From DependencyList
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

// From a single CatalogEntry
void DependencySetCatalogEntry::AddDependency(CatalogTransaction transaction, CatalogEntry &to_add,
                                              DependencyType type) {
	static DependencyList empty_dependencies;
	auto dependency = make_uniq<DependencyCatalogEntry>(DependencyConnectionType::DEPENDENCY, catalog, to_add, type);
	dependencies.CreateEntry(transaction, to_add.name, std::move(dependency), empty_dependencies);
}
void DependencySetCatalogEntry::AddDependent(CatalogTransaction transaction, CatalogEntry &to_add,
                                             DependencyType type) {
	static DependencyList empty_dependencies;
	auto dependent = make_uniq<DependencyCatalogEntry>(DependencyConnectionType::DEPENDENT, catalog, to_add, type);
	dependents.CreateEntry(transaction, to_add.name, std::move(dependent), empty_dependencies);
}

// From a Dependency
void DependencySetCatalogEntry::AddDependency(CatalogTransaction transaction, Dependency to_add) {
	AddDependency(transaction, to_add.entry, to_add.dependency_type);
}
void DependencySetCatalogEntry::AddDependent(CatalogTransaction transaction, Dependency to_add) {
	AddDependent(transaction, to_add.entry, to_add.dependency_type);
}

bool DependencySetCatalogEntry::HasDependencyOn(CatalogTransaction transaction, CatalogEntry &entry) {
	bool has_dependency_on = false;
	dependencies.Scan([&](CatalogEntry &dependency) {
		auto &dependency_entry = dependency.Cast<DependencyCatalogEntry>();
		auto schema = entry.type == CatalogType::SCHEMA_ENTRY ? entry.name : entry.ParentSchema().name;
		if (dependency_entry.schema != schema) {
			return;
		}
		if (dependency_entry.entry_type != entry.type) {
			return;
		}
		if (dependency_entry.name != entry.name) {
			// FIXME: this could change I think??
			return;
		}
		// this object has a dependency on 'entry'
		has_dependency_on = true;
	});
	return has_dependency_on;
}

} // namespace duckdb
