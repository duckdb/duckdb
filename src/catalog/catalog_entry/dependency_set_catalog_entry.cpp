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
    : catalog(catalog), mangled_name(DependencyManager::MangleName(info)), info(info),
      dependencies(dependency_manager.dependencies, info), dependents(dependency_manager.dependents, info),
      dependency_manager(dependency_manager) {
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

const CatalogEntryInfo &DependencySetCatalogEntry::EntryInfo() const {
	return info;
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
