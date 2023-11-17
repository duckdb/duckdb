#include "duckdb/catalog/catalog_entry/dependency_set_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/dependency_catalog_entry.hpp"
#include "duckdb/catalog/dependency/dependency_list.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/catalog/dependency/dependency_manager.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/duck_catalog.hpp"

namespace duckdb {

DependencySetCatalogEntry::DependencySetCatalogEntry(DuckCatalog &catalog, DependencyManager &dependency_manager,
                                                     CatalogType entry_type, const string &entry_schema,
                                                     const string &entry_name)
    : catalog(catalog), name(DependencyManager::MangleName(entry_type, entry_schema, entry_name)),
      entry_name(entry_name), schema(entry_schema), entry_type(entry_type),
      dependencies(dependency_manager.dependencies, name, entry_type, schema, entry_name),
      dependents(dependency_manager.dependents, name, entry_type, schema, entry_name),
      dependency_manager(dependency_manager) {
}

ProxyCatalogSet &DependencySetCatalogEntry::Dependencies() {
	return dependencies;
}

ProxyCatalogSet &DependencySetCatalogEntry::Dependents() {
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

//// Add from a Dependency Set
// void DependencySetCatalogEntry::AddDependencies(CatalogTransaction transaction, const dependency_set_t &to_add) {
//	for (auto &dep : to_add) {
//		auto &entry = dep.entry;
//		AddDependency(transaction, entry, dep.dependency_type);
//	}
//}
// void DependencySetCatalogEntry::AddDependents(CatalogTransaction transaction, const dependency_set_t &to_add) {
//	for (auto &dep : to_add) {
//		auto &entry = dep.entry;
//		AddDependent(transaction, entry, dep.dependency_type);
//	}
//}

//// Add from a DependencyList
// void DependencySetCatalogEntry::AddDependencies(CatalogTransaction transaction, const DependencyList &to_add) {
//	for (auto &entry : to_add.set) {
//		AddDependency(transaction, entry);
//	}
//}
// void DependencySetCatalogEntry::AddDependents(CatalogTransaction transaction, const DependencyList &to_add) {
//	for (auto &entry : to_add.set) {
//		AddDependent(transaction, entry);
//	}
//}

void DependencySetCatalogEntry::VerifyDependencyName(const string &name) {
#ifdef DEBUG
	idx_t pos = 0;
	vector<string> parts;
	while (pos != std::string::npos) {
		auto new_pos = name.find('\0', pos);
		auto part = name.substr(pos, new_pos == std::string::npos ? new_pos : new_pos - pos);
		pos = new_pos == std::string::npos ? new_pos : new_pos + 1;
		parts.push_back(part);
	}
	D_ASSERT(parts.size() == 3);

	// D_ASSERT(!StringUtil::CIEquals(parts[0], CatalogTypeToString(entry_type)));
	// D_ASSERT(!StringUtil::CIEquals(parts[1], schema));
	// D_ASSERT(!StringUtil::CIEquals(parts[2], entry_name));
	D_ASSERT(!StringUtil::CIEquals(name, this->name));
#endif
}

// Add from a single CatalogEntry
DependencyCatalogEntry &DependencySetCatalogEntry::AddDependency(CatalogTransaction transaction,
                                                                 const string &mangled_name, CatalogType entry_type,
                                                                 const string &entry_schema, const string &entry_name,
                                                                 DependencyType type) {
	static const DependencyList EMPTY_DEPENDENCIES;

	VerifyDependencyName(mangled_name);
	{
		auto dep = dependencies.GetEntry(transaction, mangled_name);
		if (dep) {
			return dep->Cast<DependencyCatalogEntry>();
		}
	}

	auto dependency_p = make_uniq<DependencyCatalogEntry>(DependencyLinkSide::DEPENDENCY, catalog, dependency_manager,
	                                                      entry_type, entry_schema, entry_name, type);
	auto dependency_name = dependency_p->name;
	D_ASSERT(dependency_name != name);
	if (catalog.IsTemporaryCatalog()) {
		dependency_p->temporary = true;
	}
	auto &dependency = *dependency_p;

	dependencies.CreateEntry(transaction, dependency_name, std::move(dependency_p), EMPTY_DEPENDENCIES);
	return dependency;
}

DependencyCatalogEntry &DependencySetCatalogEntry::AddDependency(CatalogTransaction transaction,
                                                                 DependencySetCatalogEntry &to_add,
                                                                 DependencyType type) {
	auto &mangled_name = to_add.MangledName();
	auto entry_type = to_add.EntryType();
	auto &entry_schema = to_add.EntrySchema();
	auto &entry_name = to_add.EntryName();
	return AddDependency(transaction, mangled_name, entry_type, entry_schema, entry_name, type);
}

DependencyCatalogEntry &DependencySetCatalogEntry::AddDependency(CatalogTransaction transaction, CatalogEntry &to_add,
                                                                 DependencyType type) {
	auto mangled_name = DependencyManager::MangleName(to_add);
	string entry_schema;
	string entry_name;
	CatalogType entry_type;

	DependencyManager::GetLookupProperties(to_add, entry_schema, entry_name, entry_type);
	return AddDependency(transaction, mangled_name, entry_type, entry_schema, entry_name, type);
}

DependencyCatalogEntry &DependencySetCatalogEntry::AddDependent(CatalogTransaction transaction,
                                                                const string &mangled_name, CatalogType entry_type,
                                                                const string &entry_schema, const string &entry_name,
                                                                DependencyType type) {
	static const DependencyList EMPTY_DEPENDENCIES;

	VerifyDependencyName(mangled_name);
	{
		auto dep = dependents.GetEntry(transaction, mangled_name);
		if (dep) {
			return dep->Cast<DependencyCatalogEntry>();
		}
	}
	auto dependent_p = make_uniq<DependencyCatalogEntry>(DependencyLinkSide::DEPENDENT, catalog, dependency_manager,
	                                                     entry_type, entry_schema, entry_name, type);
	auto dependent_name = dependent_p->name;
	D_ASSERT(dependent_name != name);
	if (catalog.IsTemporaryCatalog()) {
		dependent_p->temporary = true;
	}
	auto &dependent = *dependent_p;
	dependents.CreateEntry(transaction, dependent_name, std::move(dependent_p), EMPTY_DEPENDENCIES);
	return dependent;
}

DependencyCatalogEntry &DependencySetCatalogEntry::AddDependent(CatalogTransaction transaction,
                                                                DependencySetCatalogEntry &to_add,
                                                                DependencyType type) {
	auto &mangled_name = to_add.MangledName();
	auto entry_type = to_add.EntryType();
	auto &entry_schema = to_add.EntrySchema();
	auto &entry_name = to_add.EntryName();
	return AddDependency(transaction, mangled_name, entry_type, entry_schema, entry_name, type);
}

DependencyCatalogEntry &DependencySetCatalogEntry::AddDependent(CatalogTransaction transaction, CatalogEntry &to_add,
                                                                DependencyType type) {
	auto mangled_name = DependencyManager::MangleName(to_add);
	string entry_schema;
	string entry_name;
	CatalogType entry_type;

	DependencyManager::GetLookupProperties(to_add, entry_schema, entry_name, entry_type);
	return AddDependent(transaction, mangled_name, entry_type, entry_schema, entry_name, type);
}

// Add from a Dependency
DependencyCatalogEntry &DependencySetCatalogEntry::AddDependency(CatalogTransaction transaction, Dependency to_add) {
	return AddDependency(transaction, to_add.entry, to_add.dependency_type);
}
DependencyCatalogEntry &DependencySetCatalogEntry::AddDependent(CatalogTransaction transaction, Dependency to_add) {
	return AddDependent(transaction, to_add.entry, to_add.dependency_type);
}

// Remove dependency from a DependencyEntry
void DependencySetCatalogEntry::RemoveDependency(CatalogTransaction transaction, const string &mangled_name) {
	auto entry = dependencies.GetEntry(transaction, mangled_name);
	if (!entry) {
		// Already deleted
		return;
	}
	dependencies.DropEntry(transaction, mangled_name, false);
}

// Remove dependent from a DependencyEntry
void DependencySetCatalogEntry::RemoveDependent(CatalogTransaction transaction, const string &mangled_name) {
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
	auto mangled_name = dependency.MangledName();
	RemoveDependency(transaction, mangled_name);
}

// Remove dependent from a DependencyEntry
void DependencySetCatalogEntry::RemoveDependent(CatalogTransaction transaction, DependencySetCatalogEntry &dependent) {
	auto mangled_name = dependent.MangledName();
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

bool DependencySetCatalogEntry::IsDependencyOf(CatalogTransaction transaction, const string &mangled_name) {
	auto dependent = dependents.GetEntryDetailed(transaction, mangled_name);

	// It's fine if the entry is already deleted
	return dependent.reason != CatalogSet::EntryLookup::FailureReason::NOT_PRESENT;
}

bool DependencySetCatalogEntry::IsDependencyOf(CatalogTransaction transaction, CatalogEntry &entry) {
	auto mangled_name = DependencyManager::MangleName(entry);
	return IsDependencyOf(transaction, mangled_name);
}

bool DependencySetCatalogEntry::IsDependencyOf(CatalogTransaction transaction, DependencySetCatalogEntry &other) {
	auto mangled_name = other.MangledName();
	return IsDependencyOf(transaction, mangled_name);
}

bool DependencySetCatalogEntry::HasDependencyOn(CatalogTransaction transaction, const string &mangled_name) {
	auto dependency = dependencies.GetEntryDetailed(transaction, mangled_name);
	return dependency.reason != CatalogSet::EntryLookup::FailureReason::NOT_PRESENT;
}

bool DependencySetCatalogEntry::HasDependencyOn(CatalogTransaction transaction, DependencySetCatalogEntry &other) {
	auto mangled_name = other.MangledName();
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
