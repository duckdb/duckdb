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

void DependencySetCatalogEntry::AddDependencies(CatalogTransaction transaction, DependencyList &to_add) {
	DependencyList empty_dependencies;
	for (auto &entry : to_add.set) {
		auto dependency = make_uniq<DependencyCatalogEntry>(catalog, entry.get().name);
		dependencies.CreateEntry(transaction, entry.get().name, std::move(dependency), empty_dependencies);
	}
}
void DependencySetCatalogEntry::AddDependents(CatalogTransaction transaction, DependencyList &to_add, DependencyType type) {
	DependencyList empty_dependencies;
	for (auto &entry : to_add.set) {
		auto dependency = make_uniq<DependencyCatalogEntry>(catalog, entry);
		dependents.CreateEntry(transaction, entry.get().name, std::move(dependency), empty_dependencies);
	}
}

void DependencySetCatalogEntry::AddDependency(CatalogTransaction transaction, CatalogEntry &dependency) {
	DependencyList single_dependency;
	single_dependency.AddDependency(dependency);
	AddDependencies(transaction, single_dependency);
}
void DependencySetCatalogEntry::AddDependent(CatalogTransaction transaction, CatalogEntry &dependend, DependencyType type) {
	DependencyList single_dependend;
	single_dependend.AddDependency(dependend);
	AddDependents(transaction, single_dependend, type);
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
