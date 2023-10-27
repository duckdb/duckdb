#include "duckdb/catalog/catalog_entry/dependency_set_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/dependency_catalog_entry.hpp"
#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/catalog/mapping_value.hpp"

namespace duckdb {

DependencySetCatalogEntry::DependencySetCatalogEntry(Catalog &catalog, const string &name)
    : InCatalogEntry(CatalogType::DEPENDENCY_SET, catalog, name), name(name), dependencies(catalog),
      dependents(catalog) {
}

CatalogSet &DependencySetCatalogEntry::Dependencies() {
	return dependencies;
}

CatalogSet &DependencySetCatalogEntry::Dependents() {
	return dependents;
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
	D_ASSERT(dependency->name != name);
	dependencies.CreateEntryInternal(transaction, std::move(dependency));
}
void DependencySetCatalogEntry::AddDependent(CatalogTransaction transaction, CatalogEntry &to_add,
                                             DependencyType type) {
	static DependencyList empty_dependencies;
	auto dependent = make_uniq<DependencyCatalogEntry>(DependencyConnectionType::DEPENDENT, catalog, to_add, type);
	D_ASSERT(dependent->name != name);
	dependents.CreateEntryInternal(transaction, std::move(dependent));
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
	EntryIndex index;
	auto entry = dependencies.GetEntryInternal(transaction, name, &index);
	if (!entry) {
		// Already removed
		return;
	}
	D_ASSERT(index.IsValid());
	dependencies.DropEntryInternal(transaction, std::move(index), *entry, false);
}

// Remove dependent from a DependencyEntry
void DependencySetCatalogEntry::RemoveDependent(CatalogTransaction transaction, CatalogEntry &dependent) {
	D_ASSERT(dependent.type == CatalogType::DEPENDENCY_ENTRY || dependent.type == CatalogType::DEPENDENCY_SET);
	auto &name = dependent.name;
	EntryIndex index;
	auto entry = dependents.GetEntryInternal(transaction, name, &index);
	if (!entry) {
		// Already removed
		return;
	}
	D_ASSERT(index.IsValid());
	dependents.DropEntryInternal(transaction, std::move(index), *entry, false);
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
