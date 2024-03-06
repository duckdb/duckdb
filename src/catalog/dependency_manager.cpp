#include "duckdb/catalog/dependency_manager.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/catalog/catalog_entry/dependency/dependency_entry.hpp"
#include "duckdb/catalog/catalog_entry/dependency/dependency_subject_entry.hpp"
#include "duckdb/catalog/catalog_entry/dependency/dependency_dependent_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_schema_entry.hpp"
#include "duckdb/catalog/dependency_catalog_set.hpp"

namespace duckdb {

static void AssertMangledName(const string &mangled_name, idx_t expected_null_bytes) {
#ifdef DEBUG
	idx_t nullbyte_count = 0;
	for (auto &ch : mangled_name) {
		nullbyte_count += ch == '\0';
	}
	D_ASSERT(nullbyte_count == expected_null_bytes);
#endif
}

MangledEntryName::MangledEntryName(const CatalogEntryInfo &info) {
	auto &type = info.type;
	auto &schema = info.schema;
	auto &name = info.name;

	this->name = CatalogTypeToString(type) + '\0' + schema + '\0' + name;
	AssertMangledName(this->name, 2);
}

MangledDependencyName::MangledDependencyName(const MangledEntryName &from, const MangledEntryName &to) {
	this->name = from.name + '\0' + to.name;
	AssertMangledName(this->name, 5);
}

DependencyManager::DependencyManager(DuckCatalog &catalog) : catalog(catalog), subjects(catalog), dependents(catalog) {
}

string DependencyManager::GetSchema(CatalogEntry &entry) {
	if (entry.type == CatalogType::SCHEMA_ENTRY) {
		return entry.name;
	}
	return entry.ParentSchema().name;
}

MangledEntryName DependencyManager::MangleName(const CatalogEntryInfo &info) {
	return MangledEntryName(info);
}

MangledEntryName DependencyManager::MangleName(CatalogEntry &entry) {
	if (entry.type == CatalogType::DEPENDENCY_ENTRY) {
		auto &dependency_entry = entry.Cast<DependencyEntry>();
		return dependency_entry.EntryMangledName();
	}
	auto type = entry.type;
	auto schema = GetSchema(entry);
	auto name = entry.name;
	CatalogEntryInfo info {type, schema, name};

	return MangleName(info);
}

DependencyInfo DependencyInfo::FromSubject(DependencyEntry &dep) {
	return DependencyInfo {/*dependent = */ dep.Dependent(),
	                       /*subject = */ dep.Subject()};
}

DependencyInfo DependencyInfo::FromDependent(DependencyEntry &dep) {
	return DependencyInfo {/*dependent = */ dep.Dependent(),
	                       /*subject = */ dep.Subject()};
}

// ----------- DEPENDENCY_MANAGER -----------

bool DependencyManager::IsSystemEntry(CatalogEntry &entry) const {
	if (entry.internal) {
		return true;
	}

	switch (entry.type) {
	case CatalogType::DEPENDENCY_ENTRY:
	case CatalogType::DATABASE_ENTRY:
	case CatalogType::RENAMED_ENTRY:
		return true;
	default:
		return false;
	}
}

CatalogSet &DependencyManager::Dependents() {
	return dependents;
}

CatalogSet &DependencyManager::Subjects() {
	return subjects;
}

void DependencyManager::ScanSetInternal(CatalogTransaction transaction, const CatalogEntryInfo &info,
                                        bool scan_subjects, dependency_callback_t &callback) {
	catalog_entry_set_t other_entries;

	auto cb = [&](CatalogEntry &other) {
		D_ASSERT(other.type == CatalogType::DEPENDENCY_ENTRY);
		auto &other_entry = other.Cast<DependencyEntry>();
#ifdef DEBUG
		auto side = other_entry.Side();
		if (scan_subjects) {
			D_ASSERT(side == DependencyEntryType::SUBJECT);
		} else {
			D_ASSERT(side == DependencyEntryType::DEPENDENT);
		}

#endif

		other_entries.insert(other_entry);
		callback(other_entry);
	};

	if (scan_subjects) {
		DependencyCatalogSet subjects(Subjects(), info);
		subjects.Scan(transaction, cb);
	} else {
		DependencyCatalogSet dependents(Dependents(), info);
		dependents.Scan(transaction, cb);
	}

#ifdef DEBUG
	// Verify some invariants
	// Every dependency should have a matching dependent in the other set
	// And vice versa
	auto mangled_name = MangleName(info);

	if (scan_subjects) {
		for (auto &entry : other_entries) {
			auto other_info = GetLookupProperties(entry);
			DependencyCatalogSet other_dependents(Dependents(), other_info);

			// Verify that the other half of the dependency also exists
			auto dependent = other_dependents.GetEntryDetailed(transaction, mangled_name);
			D_ASSERT(dependent.reason != CatalogSet::EntryLookup::FailureReason::NOT_PRESENT);
		}
	} else {
		for (auto &entry : other_entries) {
			auto other_info = GetLookupProperties(entry);
			DependencyCatalogSet other_subjects(Subjects(), other_info);

			// Verify that the other half of the dependent also exists
			auto subject = other_subjects.GetEntryDetailed(transaction, mangled_name);
			D_ASSERT(subject.reason != CatalogSet::EntryLookup::FailureReason::NOT_PRESENT);
		}
	}
#endif
}

void DependencyManager::ScanDependents(CatalogTransaction transaction, const CatalogEntryInfo &info,
                                       dependency_callback_t &callback) {
	ScanSetInternal(transaction, info, false, callback);
}

void DependencyManager::ScanSubjects(CatalogTransaction transaction, const CatalogEntryInfo &info,
                                     dependency_callback_t &callback) {
	ScanSetInternal(transaction, info, true, callback);
}

void DependencyManager::RemoveDependency(CatalogTransaction transaction, const DependencyInfo &info) {
	auto &dependent = info.dependent;
	auto &subject = info.subject;

	// The dependents of the dependency (target)
	DependencyCatalogSet dependents(Dependents(), subject.entry);
	// The subjects of the dependencies of the dependent
	DependencyCatalogSet subjects(Subjects(), dependent.entry);

	auto dependent_mangled = MangledEntryName(dependent.entry);
	auto subject_mangled = MangledEntryName(subject.entry);

	auto dependent_p = dependents.GetEntry(transaction, dependent_mangled);
	if (dependent_p) {
		// 'dependent' is no longer inhibiting the deletion of 'dependency'
		dependents.DropEntry(transaction, dependent_mangled, false);
	}
	auto subject_p = subjects.GetEntry(transaction, subject_mangled);
	if (subject_p) {
		// 'dependency' is no longer required by 'dependent'
		subjects.DropEntry(transaction, subject_mangled, false);
	}
}

void DependencyManager::CreateSubject(CatalogTransaction transaction, const DependencyInfo &info) {
	auto &from = info.dependent.entry;

	DependencyCatalogSet set(Subjects(), from);
	auto dep = make_uniq_base<DependencyEntry, DependencySubjectEntry>(catalog, info);
	auto entry_name = dep->EntryMangledName();

	//! Add to the list of objects that 'dependent' has a dependency on
	set.CreateEntry(transaction, entry_name, std::move(dep));
}

void DependencyManager::CreateDependent(CatalogTransaction transaction, const DependencyInfo &info) {
	auto &from = info.subject.entry;

	DependencyCatalogSet set(Dependents(), from);
	auto dep = make_uniq_base<DependencyEntry, DependencyDependentEntry>(catalog, info);
	auto entry_name = dep->EntryMangledName();

	//! Add to the list of object that depend on 'subject'
	set.CreateEntry(transaction, entry_name, std::move(dep));
}

void DependencyManager::CreateDependency(CatalogTransaction transaction, DependencyInfo &info) {
	DependencyCatalogSet subjects(Subjects(), info.dependent.entry);
	DependencyCatalogSet dependents(Dependents(), info.subject.entry);

	auto subject_mangled = MangleName(info.subject.entry);
	auto dependent_mangled = MangleName(info.dependent.entry);

	auto &dependent_flags = info.dependent.flags;
	auto &subject_flags = info.subject.flags;

	auto existing_subject = subjects.GetEntry(transaction, subject_mangled);
	auto existing_dependent = dependents.GetEntry(transaction, dependent_mangled);

	// Inherit the existing flags and drop the existing entry if present
	if (existing_subject) {
		auto &existing = existing_subject->Cast<DependencyEntry>();
		auto existing_flags = existing.Subject().flags;
		if (existing_flags != subject_flags) {
			subject_flags.Apply(existing_flags);
		}
		subjects.DropEntry(transaction, subject_mangled, false, false);
	}
	if (existing_dependent) {
		auto &existing = existing_dependent->Cast<DependencyEntry>();
		auto existing_flags = existing.Dependent().flags;
		if (existing_flags != dependent_flags) {
			dependent_flags.Apply(existing_flags);
		}
		dependents.DropEntry(transaction, dependent_mangled, false, false);
	}

	// Create an entry in the dependents map of the object that is the target of the dependency
	CreateDependent(transaction, info);
	// Create an entry in the subjects map of the object that is targeting another entry
	CreateSubject(transaction, info);
}

void DependencyManager::AddObject(CatalogTransaction transaction, CatalogEntry &object,
                                  const DependencyList &dependencies) {
	if (IsSystemEntry(object)) {
		// Don't do anything for this
		return;
	}

	// check for each object in the sources if they were not deleted yet
	for (auto &dep : dependencies.set) {
		auto &dependency = dep.get();
		if (&dependency.ParentCatalog() != &object.ParentCatalog()) {
			throw DependencyException(
			    "Error adding dependency for object \"%s\" - dependency \"%s\" is in catalog "
			    "\"%s\", which does not match the catalog \"%s\".\nCross catalog dependencies are not supported.",
			    object.name, dependency.name, dependency.ParentCatalog().GetName(), object.ParentCatalog().GetName());
		}
		if (!dependency.set) {
			throw InternalException("Dependency has no set");
		}
		auto catalog_entry = dependency.set->GetEntry(transaction, dependency.name);
		if (!catalog_entry) {
			throw InternalException("Dependency has already been deleted?");
		}
	}

	// indexes do not require CASCADE to be dropped, they are simply always dropped along with the table
	DependencyDependentFlags dependency_flags;
	if (object.type != CatalogType::INDEX_ENTRY) {
		// indexes do not require CASCADE to be dropped, they are simply always dropped along with the table
		dependency_flags.SetBlocking();
	}

	// add the object to the dependents_map of each object that it depends on
	for (auto &dependency : dependencies.set) {
		DependencyInfo info {
		    /*dependent = */ DependencyDependent {GetLookupProperties(object), dependency_flags},
		    /*subject = */ DependencySubject {GetLookupProperties(dependency), DependencySubjectFlags()}};
		CreateDependency(transaction, info);
	}
}

static bool CascadeDrop(bool cascade, const DependencyDependentFlags &flags) {
	if (cascade) {
		return true;
	}
	if (flags.IsOwnedBy()) {
		// We are owned by this object, while it exists we can not be dropped without cascade.
		return false;
	}
	return !flags.IsBlocking();
}

CatalogEntryInfo DependencyManager::GetLookupProperties(CatalogEntry &entry) {
	if (entry.type == CatalogType::DEPENDENCY_ENTRY) {
		auto &dependency_entry = entry.Cast<DependencyEntry>();
		return dependency_entry.EntryInfo();
	} else {
		auto schema = DependencyManager::GetSchema(entry);
		auto &name = entry.name;
		auto &type = entry.type;
		return CatalogEntryInfo {type, schema, name};
	}
}

optional_ptr<CatalogEntry> DependencyManager::LookupEntry(CatalogTransaction transaction, CatalogEntry &dependency) {
	auto info = GetLookupProperties(dependency);

	auto &type = info.type;
	auto &schema = info.schema;
	auto &name = info.name;

	// Lookup the schema
	auto schema_entry = catalog.GetSchema(transaction, schema, OnEntryNotFound::RETURN_NULL);
	if (type == CatalogType::SCHEMA_ENTRY || !schema_entry) {
		// This is a schema entry, perform the callback only providing the schema
		return reinterpret_cast<CatalogEntry *>(schema_entry.get());
	}
	auto entry = schema_entry->GetEntry(transaction, type, name);
	return entry;
}

void DependencyManager::CleanupDependencies(CatalogTransaction transaction, CatalogEntry &object) {
	// Collect the dependencies
	vector<DependencyInfo> to_remove;

	auto info = GetLookupProperties(object);
	ScanSubjects(transaction, info,
	             [&](DependencyEntry &dep) { to_remove.push_back(DependencyInfo::FromSubject(dep)); });
	ScanDependents(transaction, info,
	               [&](DependencyEntry &dep) { to_remove.push_back(DependencyInfo::FromDependent(dep)); });

	// Remove the dependency entries
	for (auto &dep : to_remove) {
		RemoveDependency(transaction, dep);
	}
}

void DependencyManager::DropObject(CatalogTransaction transaction, CatalogEntry &object, bool cascade) {
	if (IsSystemEntry(object)) {
		// Don't do anything for this
		return;
	}

	auto info = GetLookupProperties(object);
	// Check if there are any entries that block the DROP because they still depend on the object
	catalog_entry_set_t to_drop;
	ScanDependents(transaction, info, [&](DependencyEntry &dep) {
		// It makes no sense to have a schema depend on anything
		D_ASSERT(dep.EntryInfo().type != CatalogType::SCHEMA_ENTRY);
		auto entry = LookupEntry(transaction, dep);
		if (!entry) {
			return;
		}

		if (!CascadeDrop(cascade, dep.Dependent().flags)) {
			// no cascade and there are objects that depend on this object: throw error
			throw DependencyException("Cannot drop entry \"%s\" because there are entries that "
			                          "depend on it. Use DROP...CASCADE to drop all dependents.",
			                          object.name);
		}
		to_drop.insert(*entry);
	});
	ScanSubjects(transaction, info, [&](DependencyEntry &dep) {
		auto flags = dep.Subject().flags;
		if (flags.IsOwnership()) {
			// We own this object, it should be dropped along with the table
			auto entry = LookupEntry(transaction, dep);
			to_drop.insert(*entry);
		}
	});

	CleanupDependencies(transaction, object);

	for (auto &entry : to_drop) {
		auto set = entry.get().set;
		D_ASSERT(set);
		set->DropEntry(transaction, entry.get().name, cascade);
	}
}

void DependencyManager::AlterObject(CatalogTransaction transaction, CatalogEntry &old_obj, CatalogEntry &new_obj) {
	if (IsSystemEntry(new_obj)) {
		D_ASSERT(IsSystemEntry(old_obj));
		// Don't do anything for this
		return;
	}

	auto info = GetLookupProperties(old_obj);
	dependency_set_t owned_objects;
	ScanDependents(transaction, info, [&](DependencyEntry &dep) {
		// It makes no sense to have a schema depend on anything
		D_ASSERT(dep.EntryInfo().type != CatalogType::SCHEMA_ENTRY);

		auto entry = LookupEntry(transaction, dep);
		if (!entry) {
			return;
		}
		// conflict: attempting to alter this object but the dependent object still exists
		// no cascade and there are objects that depend on this object: throw error
		throw DependencyException("Cannot alter entry \"%s\" because there are entries that "
		                          "depend on it.",
		                          old_obj.name);
	});

	// Keep old dependencies
	dependency_set_t dependents;
	ScanSubjects(transaction, info, [&](DependencyEntry &dep) {
		auto entry = LookupEntry(transaction, dep);
		if (!entry) {
			return;
		}
		if (dep.Subject().flags.IsOwnership()) {
			owned_objects.insert(Dependency(*entry, dep.Dependent().flags));
			return;
		}
		dependents.insert(Dependency(*entry, dep.Dependent().flags));
	});

	// FIXME: we should update dependencies in the future
	// some alters could cause dependencies to change (imagine types of table columns)
	// or DEFAULT depending on a sequence
	if (StringUtil::CIEquals(old_obj.name, new_obj.name)) {
		// The name was not changed, we do not need to recreate the dependency links
		return;
	}
	CleanupDependencies(transaction, old_obj);

	for (auto &dep : dependents) {
		auto &other = dep.entry.get();
		DependencyInfo info {/*dependent = */ DependencyDependent {GetLookupProperties(new_obj), dep.flags},
		                     /*subject = */ DependencySubject {GetLookupProperties(other), DependencySubjectFlags()}};
		CreateDependency(transaction, info);
	}

	// For all the objects we own, re establish the dependency of the owner on the object
	for (auto &object : owned_objects) {
		auto &entry = object.entry.get();
		{
			DependencyInfo info {
			    /*dependent = */ DependencyDependent {GetLookupProperties(new_obj),
			                                          DependencyDependentFlags().SetOwnedBy()},
			    /*subject = */ DependencySubject {GetLookupProperties(entry), DependencySubjectFlags().SetOwnership()}};
			CreateDependency(transaction, info);
		}
	}
}

void DependencyManager::Scan(
    ClientContext &context,
    const std::function<void(CatalogEntry &, CatalogEntry &, const DependencyDependentFlags &)> &callback) {
	lock_guard<mutex> write_lock(catalog.GetWriteLock());
	auto transaction = catalog.GetCatalogTransaction(context);

	// All the objects registered in the dependency manager
	catalog_entry_set_t entries;
	dependents.Scan(transaction, [&](CatalogEntry &set) {
		auto entry = LookupEntry(transaction, set);
		entries.insert(*entry);
	});

	// For every registered entry, get the dependents
	for (auto &entry : entries) {
		auto entry_info = GetLookupProperties(entry);
		// Scan all the dependents of the entry
		ScanDependents(transaction, entry_info, [&](DependencyEntry &dependent) {
			auto dep = LookupEntry(transaction, dependent);
			if (!dep) {
				return;
			}
			auto &dependent_entry = *dep;
			callback(entry, dependent_entry, dependent.Dependent().flags);
		});
	}
}

void DependencyManager::AddOwnership(CatalogTransaction transaction, CatalogEntry &owner, CatalogEntry &entry) {
	if (IsSystemEntry(entry) || IsSystemEntry(owner)) {
		return;
	}

	// If the owner is already owned by something else, throw an error
	auto owner_info = GetLookupProperties(owner);
	ScanDependents(transaction, owner_info, [&](DependencyEntry &dep) {
		if (dep.Dependent().flags.IsOwnedBy()) {
			throw DependencyException("%s can not become the owner, it is already owned by %s", owner.name,
			                          dep.EntryInfo().name);
		}
	});

	// If the entry is the owner of another entry, throw an error
	auto entry_info = GetLookupProperties(entry);
	ScanSubjects(transaction, entry_info, [&](DependencyEntry &other) {
		auto dependent_entry = LookupEntry(transaction, other);
		if (!dependent_entry) {
			return;
		}
		auto &dep = *dependent_entry;

		auto flags = other.Dependent().flags;
		if (!flags.IsOwnedBy()) {
			return;
		}
		throw DependencyException("%s already owns %s. Cannot have circular dependencies", entry.name, dep.name);
	});

	// If the entry is already owned, throw an error
	ScanDependents(transaction, entry_info, [&](DependencyEntry &other) {
		auto dependent_entry = LookupEntry(transaction, other);
		if (!dependent_entry) {
			return;
		}

		auto &dep = *dependent_entry;
		auto flags = other.Subject().flags;
		if (!flags.IsOwnership()) {
			return;
		}
		if (&dep != &owner) {
			throw DependencyException("%s is already owned by %s", entry.name, dep.name);
		}
	});

	DependencyInfo info {
	    /*dependent = */ DependencyDependent {GetLookupProperties(owner), DependencyDependentFlags().SetOwnedBy()},
	    /*subject = */ DependencySubject {GetLookupProperties(entry), DependencySubjectFlags().SetOwnership()}};
	CreateDependency(transaction, info);
}

static string FormatString(const MangledEntryName &mangled) {
	auto input = mangled.name;
	for (size_t i = 0; i < input.size(); i++) {
		if (input[i] == '\0') {
			input[i] = '_';
		}
	}
	return input;
}

void DependencyManager::PrintSubjects(CatalogTransaction transaction, const CatalogEntryInfo &info) {
	auto name = MangleName(info);
	Printer::Print(StringUtil::Format("Subjects of %s", FormatString(name)));
	auto subjects = DependencyCatalogSet(Subjects(), info);
	subjects.Scan(transaction, [&](CatalogEntry &dependency) {
		auto &dep = dependency.Cast<DependencyEntry>();
		auto &entry_info = dep.EntryInfo();
		auto type = entry_info.type;
		auto schema = entry_info.schema;
		auto name = entry_info.name;
		Printer::Print(StringUtil::Format("Schema: %s | Name: %s | Type: %s | Dependent type: %s | Subject type: %s",
		                                  schema, name, CatalogTypeToString(type), dep.Dependent().flags.ToString(),
		                                  dep.Subject().flags.ToString()));
	});
}

void DependencyManager::PrintDependents(CatalogTransaction transaction, const CatalogEntryInfo &info) {
	auto name = MangleName(info);
	Printer::Print(StringUtil::Format("Dependents of %s", FormatString(name)));
	auto dependents = DependencyCatalogSet(Dependents(), info);
	dependents.Scan(transaction, [&](CatalogEntry &dependent) {
		auto &dep = dependent.Cast<DependencyEntry>();
		auto &entry_info = dep.EntryInfo();
		auto type = entry_info.type;
		auto schema = entry_info.schema;
		auto name = entry_info.name;
		Printer::Print(StringUtil::Format("Schema: %s | Name: %s | Type: %s | Dependent type: %s | Subject type: %s",
		                                  schema, name, CatalogTypeToString(type), dep.Dependent().flags.ToString(),
		                                  dep.Subject().flags.ToString()));
	});
}

} // namespace duckdb
