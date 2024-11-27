#include "duckdb/catalog/dependency_manager.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/catalog/catalog_entry/dependency/dependency_entry.hpp"
#include "duckdb/catalog/catalog_entry/dependency/dependency_subject_entry.hpp"
#include "duckdb/catalog/catalog_entry/dependency/dependency_dependent_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_schema_entry.hpp"
#include "duckdb/common/queue.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/constraints/foreign_key_constraint.hpp"
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

string DependencyManager::GetSchema(const CatalogEntry &entry) {
	if (entry.type == CatalogType::SCHEMA_ENTRY) {
		return entry.name;
	}
	return entry.ParentSchema().name;
}

MangledEntryName DependencyManager::MangleName(const CatalogEntryInfo &info) {
	return MangledEntryName(info);
}

MangledEntryName DependencyManager::MangleName(const CatalogEntry &entry) {
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

void DependencyManager::CreateDependencies(CatalogTransaction transaction, const CatalogEntry &object,
                                           const LogicalDependencyList &dependencies) {
	DependencyDependentFlags dependency_flags;
	if (object.type != CatalogType::INDEX_ENTRY) {
		// indexes do not require CASCADE to be dropped, they are simply always dropped along with the table
		dependency_flags.SetBlocking();
	}

	const auto object_info = GetLookupProperties(object);
	// check for each object in the sources if they were not deleted yet
	for (auto &dependency : dependencies.Set()) {
		if (dependency.catalog != object.ParentCatalog().GetName()) {
			throw DependencyException(
			    "Error adding dependency for object \"%s\" - dependency \"%s\" is in catalog "
			    "\"%s\", which does not match the catalog \"%s\".\nCross catalog dependencies are not supported.",
			    object.name, dependency.entry.name, dependency.catalog, object.ParentCatalog().GetName());
		}
	}

	// add the object to the dependents_map of each object that it depends on
	for (auto &dependency : dependencies.Set()) {
		DependencyInfo info {/*dependent = */ DependencyDependent {GetLookupProperties(object), dependency_flags},
		                     /*subject = */ DependencySubject {dependency.entry, DependencySubjectFlags()}};
		CreateDependency(transaction, info);
	}
}

void DependencyManager::AddObject(CatalogTransaction transaction, CatalogEntry &object,
                                  const LogicalDependencyList &dependencies) {
	if (IsSystemEntry(object)) {
		// Don't do anything for this
		return;
	}
	CreateDependencies(transaction, object, dependencies);
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

CatalogEntryInfo DependencyManager::GetLookupProperties(const CatalogEntry &entry) {
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
	if (dependency.type != CatalogType::DEPENDENCY_ENTRY) {
		return &dependency;
	}
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

static string EntryToString(CatalogEntryInfo &info) {
	auto type = info.type;
	switch (type) {
	case CatalogType::TABLE_ENTRY: {
		return StringUtil::Format("table \"%s\"", info.name);
	}
	case CatalogType::SCHEMA_ENTRY: {
		return StringUtil::Format("schema \"%s\"", info.name);
	}
	case CatalogType::VIEW_ENTRY: {
		return StringUtil::Format("view \"%s\"", info.name);
	}
	case CatalogType::INDEX_ENTRY: {
		return StringUtil::Format("index \"%s\"", info.name);
	}
	case CatalogType::SEQUENCE_ENTRY: {
		return StringUtil::Format("index \"%s\"", info.name);
	}
	case CatalogType::COLLATION_ENTRY: {
		return StringUtil::Format("collation \"%s\"", info.name);
	}
	case CatalogType::TYPE_ENTRY: {
		return StringUtil::Format("type \"%s\"", info.name);
	}
	case CatalogType::TABLE_FUNCTION_ENTRY: {
		return StringUtil::Format("table function \"%s\"", info.name);
	}
	case CatalogType::SCALAR_FUNCTION_ENTRY: {
		return StringUtil::Format("scalar function \"%s\"", info.name);
	}
	case CatalogType::AGGREGATE_FUNCTION_ENTRY: {
		return StringUtil::Format("aggregate function \"%s\"", info.name);
	}
	case CatalogType::PRAGMA_FUNCTION_ENTRY: {
		return StringUtil::Format("pragma function \"%s\"", info.name);
	}
	case CatalogType::COPY_FUNCTION_ENTRY: {
		return StringUtil::Format("copy function \"%s\"", info.name);
	}
	case CatalogType::MACRO_ENTRY: {
		return StringUtil::Format("macro function \"%s\"", info.name);
	}
	case CatalogType::TABLE_MACRO_ENTRY: {
		return StringUtil::Format("table macro function \"%s\"", info.name);
	}
	case CatalogType::SECRET_ENTRY: {
		return StringUtil::Format("secret \"%s\"", info.name);
	}
	case CatalogType::SECRET_TYPE_ENTRY: {
		return StringUtil::Format("secret type \"%s\"", info.name);
	}
	case CatalogType::SECRET_FUNCTION_ENTRY: {
		return StringUtil::Format("secret function \"%s\"", info.name);
	}
	default:
		throw InternalException("CatalogType not handled in EntryToString (DependencyManager) for %s",
		                        CatalogTypeToString(type));
	};
}

string DependencyManager::CollectDependents(CatalogTransaction transaction, catalog_entry_set_t &entries,
                                            CatalogEntryInfo &info) {
	string result;
	for (auto &entry : entries) {
		D_ASSERT(!IsSystemEntry(entry.get()));
		auto other_info = GetLookupProperties(entry);
		result += StringUtil::Format("%s depends on %s.\n", EntryToString(other_info), EntryToString(info));
		catalog_entry_set_t entry_dependents;
		ScanDependents(transaction, other_info, [&](DependencyEntry &dep) {
			auto child = LookupEntry(transaction, dep);
			if (!child) {
				return;
			}
			if (!CascadeDrop(false, dep.Dependent().flags)) {
				entry_dependents.insert(*child);
			}
		});
		if (!entry_dependents.empty()) {
			result += CollectDependents(transaction, entry_dependents, other_info);
		}
	}
	return result;
}

void DependencyManager::VerifyExistence(CatalogTransaction transaction, DependencyEntry &object) {
	auto &subject = object.Subject();

	CatalogEntryInfo info;
	if (subject.flags.IsOwnership()) {
		info = object.SourceInfo();
	} else {
		info = object.EntryInfo();
	}

	auto &type = info.type;
	auto &schema = info.schema;
	auto &name = info.name;

	auto &duck_catalog = catalog.Cast<DuckCatalog>();
	auto &schema_catalog_set = duck_catalog.GetSchemaCatalogSet();

	CatalogSet::EntryLookup lookup_result;
	lookup_result = schema_catalog_set.GetEntryDetailed(transaction, schema);

	if (type != CatalogType::SCHEMA_ENTRY && lookup_result.result) {
		auto &schema_entry = lookup_result.result->Cast<SchemaCatalogEntry>();
		lookup_result = schema_entry.GetEntryDetailed(transaction, type, name);
	}

	if (lookup_result.reason == CatalogSet::EntryLookup::FailureReason::DELETED) {
		throw DependencyException("Could not commit creation of dependency, subject \"%s\" has been deleted",
		                          object.SourceInfo().name);
	}
}

void DependencyManager::VerifyCommitDrop(CatalogTransaction transaction, transaction_t start_time,
                                         CatalogEntry &object) {
	if (IsSystemEntry(object)) {
		return;
	}
	auto info = GetLookupProperties(object);
	ScanDependents(transaction, info, [&](DependencyEntry &dep) {
		auto dep_committed_at = dep.timestamp.load();
		if (dep_committed_at > start_time) {
			// In the event of a CASCADE, the dependency drop has not committed yet
			// so we would be halted by the existence of a dependency we are already dropping unless we check the
			// timestamp
			//
			// Which differentiates between objects that we were already aware of (and will subsequently be dropped) and
			// objects that were introduced inbetween, which should cause this error:
			throw DependencyException(
			    "Could not commit DROP of \"%s\" because a dependency was created after the transaction started",
			    object.name);
		}
	});
	ScanSubjects(transaction, info, [&](DependencyEntry &dep) {
		auto dep_committed_at = dep.timestamp.load();
		if (!dep.Dependent().flags.IsOwnedBy()) {
			return;
		}
		D_ASSERT(dep.Subject().flags.IsOwnership());
		if (dep_committed_at > start_time) {
			// Same as above, objects that are owned by the object that is being dropped will be dropped as part of this
			// transaction. Only objects that were introduced by other transactions, that this transaction could not
			// see, should cause this error:
			throw DependencyException(
			    "Could not commit DROP of \"%s\" because a dependency was created after the transaction started",
			    object.name);
		}
	});
}

catalog_entry_set_t DependencyManager::CheckDropDependencies(CatalogTransaction transaction, CatalogEntry &object,
                                                             bool cascade) {
	if (IsSystemEntry(object)) {
		// Don't do anything for this
		return catalog_entry_set_t();
	}

	catalog_entry_set_t to_drop;
	catalog_entry_set_t blocking_dependents;

	auto info = GetLookupProperties(object);
	// Look through all the objects that depend on the 'object'
	ScanDependents(transaction, info, [&](DependencyEntry &dep) {
		// It makes no sense to have a schema depend on anything
		D_ASSERT(dep.EntryInfo().type != CatalogType::SCHEMA_ENTRY);
		auto entry = LookupEntry(transaction, dep);
		if (!entry) {
			return;
		}

		if (!CascadeDrop(cascade, dep.Dependent().flags)) {
			// no cascade and there are objects that depend on this object: throw error
			blocking_dependents.insert(*entry);
		} else {
			to_drop.insert(*entry);
		}
	});
	if (!blocking_dependents.empty()) {
		string error_string =
		    StringUtil::Format("Cannot drop entry \"%s\" because there are entries that depend on it.\n", object.name);
		error_string += CollectDependents(transaction, blocking_dependents, info);
		error_string += "Use DROP...CASCADE to drop all dependents.";
		throw DependencyException(error_string);
	}

	// Look through all the entries that 'object' depends on
	ScanSubjects(transaction, info, [&](DependencyEntry &dep) {
		auto flags = dep.Subject().flags;
		if (flags.IsOwnership()) {
			// We own this object, it should be dropped along with the table
			auto entry = LookupEntry(transaction, dep);
			to_drop.insert(*entry);
		}
	});
	return to_drop;
}

void DependencyManager::DropObject(CatalogTransaction transaction, CatalogEntry &object, bool cascade) {
	if (IsSystemEntry(object)) {
		// Don't do anything for this
		return;
	}

	// Check if there are any entries that block the DROP because they still depend on the object
	auto to_drop = CheckDropDependencies(transaction, object, cascade);
	CleanupDependencies(transaction, object);

	for (auto &entry : to_drop) {
		auto set = entry.get().set;
		D_ASSERT(set);
		set->DropEntry(transaction, entry.get().name, cascade);
	}
}

void DependencyManager::ReorderEntries(catalog_entry_vector_t &entries, ClientContext &context) {
	auto transaction = catalog.GetCatalogTransaction(context);
	// Read all the entries visible to this snapshot
	ReorderEntries(entries, transaction);
}

void DependencyManager::ReorderEntries(catalog_entry_vector_t &entries) {
	// Read all committed entries
	CatalogTransaction transaction(catalog.GetDatabase(), TRANSACTION_ID_START - 1, TRANSACTION_ID_START - 1);
	ReorderEntries(entries, transaction);
}

void DependencyManager::ReorderEntry(CatalogTransaction transaction, CatalogEntry &entry, catalog_entry_set_t &visited,
                                     catalog_entry_vector_t &order) {
	auto &catalog_entry = *LookupEntry(transaction, entry);
	// We use this in CheckpointManager, it has the highest commit ID, allowing us to read any committed data
	bool allow_internal = transaction.start_time == TRANSACTION_ID_START - 1;
	if (visited.count(catalog_entry) || (!allow_internal && catalog_entry.internal)) {
		// Already seen and ordered appropriately
		return;
	}

	// Check if there are any entries that this entry depends on, those are written first
	catalog_entry_vector_t dependents;
	auto info = GetLookupProperties(entry);
	ScanSubjects(transaction, info, [&](DependencyEntry &dep) { dependents.push_back(dep); });
	for (auto &dep : dependents) {
		ReorderEntry(transaction, dep, visited, order);
	}

	// Then write the entry
	visited.insert(catalog_entry);
	order.push_back(catalog_entry);
}

void DependencyManager::ReorderEntries(catalog_entry_vector_t &entries, CatalogTransaction transaction) {
	catalog_entry_vector_t reordered;
	catalog_entry_set_t visited;
	for (auto &entry : entries) {
		ReorderEntry(transaction, entry, visited, reordered);
	}
	// If this would fail, that means there are more entries that we somehow reached through the dependency manager
	// but those entries should not actually be visible to this transaction
	D_ASSERT(entries.size() == reordered.size());
	entries.clear();
	entries = reordered;
}

void DependencyManager::AlterObject(CatalogTransaction transaction, CatalogEntry &old_obj, CatalogEntry &new_obj,
                                    AlterInfo &alter_info) {
	if (IsSystemEntry(new_obj)) {
		D_ASSERT(IsSystemEntry(old_obj));
		// Don't do anything for this
		return;
	}

	const auto old_info = GetLookupProperties(old_obj);
	const auto new_info = GetLookupProperties(new_obj);

	vector<DependencyInfo> dependencies;
	// Other entries that depend on us
	ScanDependents(transaction, old_info, [&](DependencyEntry &dep) {
		// It makes no sense to have a schema depend on anything
		D_ASSERT(dep.EntryInfo().type != CatalogType::SCHEMA_ENTRY);

		bool disallow_alter = true;
		switch (alter_info.type) {
		case AlterType::ALTER_TABLE: {
			auto &alter_table = alter_info.Cast<AlterTableInfo>();
			switch (alter_table.alter_table_type) {
			case AlterTableType::FOREIGN_KEY_CONSTRAINT: {
				// These alters are made as part of a CREATE or DROP table statement when a foreign key column is
				// present either adding or removing a reference to the referenced primary key table
				disallow_alter = false;
				break;
			}
			case AlterTableType::ADD_COLUMN: {
				disallow_alter = false;
				break;
			}
			default:
				break;
			}
			break;
		}
		case AlterType::SET_COLUMN_COMMENT:
		case AlterType::SET_COMMENT: {
			disallow_alter = false;
			break;
		}
		default:
			break;
		}
		if (disallow_alter) {
			throw DependencyException("Cannot alter entry \"%s\" because there are entries that "
			                          "depend on it.",
			                          old_obj.name);
		}

		auto dep_info = DependencyInfo::FromDependent(dep);
		dep_info.subject.entry = new_info;
		dependencies.emplace_back(dep_info);
	});

	// Keep old dependencies
	dependency_set_t dependents;
	ScanSubjects(transaction, old_info, [&](DependencyEntry &dep) {
		auto entry = LookupEntry(transaction, dep);
		if (!entry) {
			return;
		}

		auto dep_info = DependencyInfo::FromSubject(dep);
		dep_info.dependent.entry = new_info;
		dependencies.emplace_back(dep_info);
	});

	// FIXME: we should update dependencies in the future
	// some alters could cause dependencies to change (imagine types of table columns)
	// or DEFAULT depending on a sequence
	if (!StringUtil::CIEquals(old_obj.name, new_obj.name)) {
		// The name has been changed, we need to recreate the dependency links
		CleanupDependencies(transaction, old_obj);
	}

	// Reinstate the old dependencies
	for (auto &dep : dependencies) {
		CreateDependency(transaction, dep);
	}
}

void DependencyManager::Scan(
    ClientContext &context,
    const std::function<void(CatalogEntry &, CatalogEntry &, const DependencyDependentFlags &)> &callback) {
	auto transaction = catalog.GetCatalogTransaction(context);
	lock_guard<mutex> write_lock(catalog.GetWriteLock());

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
	const auto owner_info = GetLookupProperties(owner);
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
