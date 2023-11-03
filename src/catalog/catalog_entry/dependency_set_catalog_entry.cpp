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
                                                     LogicalDependency internal)
    : InCatalogEntry(CatalogType::DEPENDENCY_SET, catalog, DependencyManager::MangleName(internal)), internal(internal),
      dependencies(catalog), dependents(catalog), dependency_manager(dependency_manager) {
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
		auto other_dependency_set_p = dependency_manager.GetDependencySet(&transaction, other);
		if (!other_dependency_set_p) {
			// Already deleted
			return;
		}
		auto &other_dependency_set = *other_dependency_set_p;
		(void)other_dependency_set;

		// Assert some invariants of the dependency_set
		if (dependencies) {
			D_ASSERT(other_dependency_set.IsDependencyOf(transaction, *this));
		} else {
			D_ASSERT(other_dependency_set.HasDependencyOn(transaction, *this, other_entry.Type()));
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
	return internal.type;
}

const string &DependencySetCatalogEntry::EntrySchema() const {
	return internal.schema;
}

const string &DependencySetCatalogEntry::EntryName() const {
	return internal.name;
}

const LogicalDependency &DependencySetCatalogEntry::Internal() const {
	return internal;
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
void DependencySetCatalogEntry::AddDependencies(CatalogTransaction transaction, const LogicalDependencyList &to_add) {
	for (auto &entry : to_add.Set()) {
		AddDependency(transaction, entry);
	}
}
void DependencySetCatalogEntry::AddDependents(CatalogTransaction transaction, const LogicalDependencyList &to_add) {
	for (auto &entry : to_add.Set()) {
		AddDependent(transaction, entry);
	}
}

// Add from a single CatalogEntry
DependencyCatalogEntry &DependencySetCatalogEntry::AddDependency(CatalogTransaction transaction,
                                                                 LogicalDependency to_add, DependencyType type) {
	static const LogicalDependencyList EMPTY_DEPENDENCIES;

	{
		auto mangled_name = DependencyManager::MangleName(to_add);
		auto dep = dependencies.GetEntry(transaction, mangled_name);
		if (dep) {
			return dep->Cast<DependencyCatalogEntry>();
		}
	}

	auto dependency_p = make_uniq<DependencyCatalogEntry>(DependencyLinkSide::DEPENDENCY, catalog, *this, to_add, type);
	auto dependency_name = dependency_p->name;
	D_ASSERT(dependency_name != name);
	if (catalog.IsTemporaryCatalog()) {
		dependency_p->temporary = true;
	}
	auto &dependency = *dependency_p;
	dependencies.CreateEntry(transaction, dependency_name, std::move(dependency_p), EMPTY_DEPENDENCIES);
	return dependency;
}

DependencyCatalogEntry &DependencySetCatalogEntry::AddDependency(CatalogTransaction transaction, CatalogEntry &to_add,
                                                                 DependencyType type) {
	return AddDependency(transaction, LogicalDependency(to_add), type);
}

DependencyCatalogEntry &DependencySetCatalogEntry::AddDependent(CatalogTransaction transaction,
                                                                LogicalDependency to_add, DependencyType type) {
	static const LogicalDependencyList EMPTY_DEPENDENCIES;

	{
		auto mangled_name = DependencyManager::MangleName(to_add);
		auto dep = dependents.GetEntry(transaction, mangled_name);
		if (dep) {
			return dep->Cast<DependencyCatalogEntry>();
		}
	}

	auto dependent_p = make_uniq<DependencyCatalogEntry>(DependencyLinkSide::DEPENDENT, catalog, *this, to_add, type);
	auto dependent_name = dependent_p->name;
	D_ASSERT(dependent_name != name);
	if (catalog.IsTemporaryCatalog()) {
		dependent_p->temporary = true;
	}
	auto &dependent = *dependent_p;
	dependents.CreateEntry(transaction, dependent_name, std::move(dependent_p), EMPTY_DEPENDENCIES);
	return dependent;
}

DependencyCatalogEntry &DependencySetCatalogEntry::AddDependent(CatalogTransaction transaction, CatalogEntry &to_add,
                                                                DependencyType type) {
	return AddDependent(transaction, LogicalDependency(to_add), type);
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

DependencyCatalogEntry &DependencySetCatalogEntry::GetDependency(optional_ptr<CatalogTransaction> transaction,
                                                                 CatalogEntry &object) {
	auto mangled_name = DependencyManager::MangleName(object);
	optional_ptr<CatalogEntry> dependency;
	if (transaction) {
		dependency = dependencies.GetEntry(*transaction, mangled_name);
	} else {
		dependency = dependencies.GetEntry(mangled_name);
	}
	return dependency->Cast<DependencyCatalogEntry>();
}

DependencyCatalogEntry &DependencySetCatalogEntry::GetDependent(optional_ptr<CatalogTransaction> transaction,
                                                                CatalogEntry &object) {
	auto mangled_name = DependencyManager::MangleName(object);
	optional_ptr<CatalogEntry> dependent;
	if (transaction) {
		dependent = dependents.GetEntry(*transaction, mangled_name);
	} else {
		dependent = dependents.GetEntry(mangled_name);
	}
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

catalog_entry_vector_t
DependencySetCatalogEntry::GetEntriesThatDependOnUs(optional_ptr<CatalogTransaction> transaction) {
	catalog_entry_vector_t entries;
	if (transaction) {
		dependents.Scan(*transaction, [&](CatalogEntry &entry) { entries.push_back(entry); });
	} else {
		dependents.Scan([&](CatalogEntry &entry) { entries.push_back(entry); });
	}
	return entries;
}

catalog_entry_vector_t
DependencySetCatalogEntry::GetEntriesThatWeDependOn(optional_ptr<CatalogTransaction> transaction) {
	catalog_entry_vector_t entries;
	if (transaction) {
		dependencies.Scan(*transaction, [&](CatalogEntry &entry) { entries.push_back(entry); });
	} else {
		dependencies.Scan([&](CatalogEntry &entry) { entries.push_back(entry); });
	}
	return entries;
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
