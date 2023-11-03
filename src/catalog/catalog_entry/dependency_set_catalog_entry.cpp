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
                                                     const LogicalDependency &internal)
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
			D_ASSERT(other_dependency_set.HasDependencyOn(transaction, *this));
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
		AddDependency(transaction, entry, dep.flags);
	}
}
void DependencySetCatalogEntry::AddDependents(CatalogTransaction transaction, const dependency_set_t &to_add) {
	for (auto &dep : to_add) {
		auto &entry = dep.entry;
		AddDependent(transaction, entry, dep.flags);
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
                                                                 const LogicalDependency &to_add,
                                                                 DependencyFlags flags) {
	static const LogicalDependencyList EMPTY_DEPENDENCIES;

	{
		auto mangled_name = DependencyManager::MangleName(to_add);
		auto dep = dependencies.GetEntry(transaction, mangled_name);
		if (dep) {
			auto &dependency = dep->Cast<DependencyCatalogEntry>();
			if (dependency.Flags() == flags) {
				// Only return the entry if no changes need to be made
				return dependency;
			}
		}
	}

	auto dependency_p =
	    make_uniq<DependencyCatalogEntry>(DependencyLinkSide::DEPENDENCY, catalog, *this, to_add, flags);
	auto dependency_name = dependency_p->name;
	D_ASSERT(dependency_name != name);
	if (catalog.IsTemporaryCatalog()) {
		dependency_p->temporary = true;
	}
	auto &dependency = *dependency_p;
	const bool inserted =
	    dependencies.CreateEntry(transaction, dependency_name, std::move(dependency_p), EMPTY_DEPENDENCIES);
	(void)inserted;
	D_ASSERT(inserted);
	return dependency;
}

DependencyCatalogEntry &DependencySetCatalogEntry::AddDependency(CatalogTransaction transaction, CatalogEntry &to_add,
                                                                 DependencyFlags flags) {
	return AddDependency(transaction, LogicalDependency(to_add), flags);
}

DependencyCatalogEntry &DependencySetCatalogEntry::AddDependent(CatalogTransaction transaction,
                                                                const LogicalDependency &to_add,
                                                                DependencyFlags flags) {
	static const LogicalDependencyList EMPTY_DEPENDENCIES;

	{
		// Check if it already exists
		auto mangled_name = DependencyManager::MangleName(to_add);
		auto dep = dependents.GetEntry(transaction, mangled_name);
		if (dep) {
			auto &dependent = dep->Cast<DependencyCatalogEntry>();
			if (dependent.Flags() == flags) {
				// Only return the entry if no changes need to be made
				return dependent;
			}
			flags.Apply(dependent.Flags());
			// It has to be dropped because the type needs to be updated
			dependents.DropEntry(transaction, mangled_name, false, false);
		}
	}

	auto dependent_p = make_uniq<DependencyCatalogEntry>(DependencyLinkSide::DEPENDENT, catalog, *this, to_add, flags);
	auto dependent_name = dependent_p->name;
	D_ASSERT(dependent_name != name);
	if (catalog.IsTemporaryCatalog()) {
		dependent_p->temporary = true;
	}
	auto &dependent = *dependent_p;
	const bool inserted =
	    dependents.CreateEntry(transaction, dependent_name, std::move(dependent_p), EMPTY_DEPENDENCIES);
	(void)inserted;
	D_ASSERT(inserted);
	return dependent;
}

DependencyCatalogEntry &DependencySetCatalogEntry::AddDependent(CatalogTransaction transaction, CatalogEntry &to_add,
                                                                DependencyFlags flags) {
	return AddDependent(transaction, LogicalDependency(to_add), flags);
}

// Add from a Dependency
DependencyCatalogEntry &DependencySetCatalogEntry::AddDependency(CatalogTransaction transaction, Dependency to_add) {
	return AddDependency(transaction, to_add.entry, to_add.flags);
}
DependencyCatalogEntry &DependencySetCatalogEntry::AddDependent(CatalogTransaction transaction, Dependency to_add) {
	return AddDependent(transaction, to_add.entry, to_add.flags);
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

bool DependencySetCatalogEntry::HasDependencyOn(CatalogTransaction transaction, CatalogEntry &entry) {
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

void DependencySetCatalogEntry::PrintDependencies(optional_ptr<CatalogTransaction> transaction) {
	Printer::Print(StringUtil::Format("Dependencies of %s", FormatString(name)));
	auto cb = [&](CatalogEntry &dependency) {
		auto &dep = dependency.Cast<DependencyCatalogEntry>();
		auto &name = dep.EntryName();
		auto &schema = dep.EntrySchema();
		auto type = dep.EntryType();
		Printer::Print(StringUtil::Format("Schema: %s | Name: %s | Type: %s | DependencyFlags: %s", schema, name,
		                                  CatalogTypeToString(type), dep.Flags().ToString()));
	};
	if (transaction) {
		dependencies.Scan(*transaction, cb);
	} else {
		dependencies.Scan(cb);
	}
}
void DependencySetCatalogEntry::PrintDependents(optional_ptr<CatalogTransaction> transaction) {
	Printer::Print(StringUtil::Format("Dependents of %s", FormatString(name)));
	auto cb = [&](CatalogEntry &dependent) {
		auto &dep = dependent.Cast<DependencyCatalogEntry>();
		auto &name = dep.EntryName();
		auto &schema = dep.EntrySchema();
		auto type = dep.EntryType();
		Printer::Print(StringUtil::Format("Schema: %s | Name: %s | Type: %s | DependencyFlags: %s", schema, name,
		                                  CatalogTypeToString(type), dep.Flags().ToString()));
	};
	if (transaction) {
		dependencies.Scan(*transaction, cb);
	} else {
		dependencies.Scan(cb);
	}
}

} // namespace duckdb
