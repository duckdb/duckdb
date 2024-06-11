#include "duckdb/catalog/catalog_set.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/catalog/dependency_manager.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"

namespace duckdb {

void CatalogEntryMap::AddEntry(unique_ptr<CatalogEntry> entry) {
	auto name = entry->name;

	if (entries.find(name) != entries.end()) {
		throw InternalException("Entry with name \"%s\" already exists", name);
	}
	entries.insert(make_pair(name, std::move(entry)));
}

void CatalogEntryMap::UpdateEntry(unique_ptr<CatalogEntry> catalog_entry) {
	auto name = catalog_entry->name;

	auto entry = entries.find(name);
	if (entry == entries.end()) {
		throw InternalException("Entry with name \"%s\" does not exist", name);
	}

	auto existing = std::move(entry->second);
	entry->second = std::move(catalog_entry);
	entry->second->SetChild(std::move(existing));
}

case_insensitive_tree_t<unique_ptr<CatalogEntry>> &CatalogEntryMap::Entries() {
	return entries;
}

void CatalogEntryMap::DropEntry(CatalogEntry &entry) {
	auto &name = entry.name;
	auto chain = GetEntry(name);
	if (!chain) {
		throw InternalException("Attempting to drop entry with name \"%s\" but no chain with that name exists", name);
	}
	auto child = entry.TakeChild();
	if (!entry.HasParent()) {
		// This is the top of the chain
		D_ASSERT(chain.get() == &entry);
		auto it = entries.find(name);
		D_ASSERT(it != entries.end());

		// Remove the entry
		it->second.reset();
		if (child) {
			// Replace it with its child
			it->second = std::move(child);
		} else {
			entries.erase(it);
		}
	} else {
		// Just replace the entry with its child
		auto &parent = entry.Parent();
		parent.SetChild(std::move(child));
	}
}

optional_ptr<CatalogEntry> CatalogEntryMap::GetEntry(const string &name) {
	auto entry = entries.find(name);
	if (entry == entries.end()) {
		return nullptr;
	}
	return entry->second.get();
}

CatalogSet::CatalogSet(Catalog &catalog_p, unique_ptr<DefaultGenerator> defaults)
    : catalog(catalog_p.Cast<DuckCatalog>()), defaults(std::move(defaults)) {
	D_ASSERT(catalog_p.IsDuckCatalog());
}
CatalogSet::~CatalogSet() {
}

bool IsDependencyEntry(CatalogEntry &entry) {
	return entry.type == CatalogType::DEPENDENCY_ENTRY;
}

bool CatalogSet::StartChain(CatalogTransaction transaction, const string &name, unique_lock<mutex> &read_lock) {
	D_ASSERT(!map.GetEntry(name));

	// check if there is a default entry
	auto entry = CreateDefaultEntry(transaction, name, read_lock);
	if (entry) {
		return false;
	}

	// first create a dummy deleted entry for this entry
	// so transactions started before the commit of this transaction don't
	// see it yet
	auto dummy_node = make_uniq<InCatalogEntry>(CatalogType::INVALID, catalog, name);
	dummy_node->timestamp = 0;
	dummy_node->deleted = true;
	dummy_node->set = this;

	map.AddEntry(std::move(dummy_node));
	return true;
}

bool CatalogSet::VerifyVacancy(CatalogTransaction transaction, CatalogEntry &entry) {
	// if it does, we have to check version numbers
	if (HasConflict(transaction, entry.timestamp)) {
		// current version has been written to by a currently active
		// transaction
		throw TransactionException("Catalog write-write conflict on create with \"%s\"", entry.name);
	}
	// there is a current version that has been committed
	// if it has not been deleted there is a conflict
	if (!entry.deleted) {
		return false;
	}
	return true;
}

void CatalogSet::CheckCatalogEntryInvariants(CatalogEntry &value, const string &name) {
	if (value.internal && !catalog.IsSystemCatalog() && name != DEFAULT_SCHEMA) {
		throw InternalException("Attempting to create internal entry \"%s\" in non-system catalog - internal entries "
		                        "can only be created in the system catalog",
		                        name);
	}
	if (!value.internal) {
		if (!value.temporary && catalog.IsSystemCatalog() && !IsDependencyEntry(value)) {
			throw InternalException(
			    "Attempting to create non-internal entry \"%s\" in system catalog - the system catalog "
			    "can only contain internal entries",
			    name);
		}
		if (value.temporary && !catalog.IsTemporaryCatalog()) {
			throw InternalException("Attempting to create temporary entry \"%s\" in non-temporary catalog", name);
		}
		if (!value.temporary && catalog.IsTemporaryCatalog() && name != DEFAULT_SCHEMA) {
			throw InvalidInputException("Cannot create non-temporary entry \"%s\" in temporary catalog", name);
		}
	}
}

optional_ptr<CatalogEntry> CatalogSet::CreateCommittedEntry(unique_ptr<CatalogEntry> entry) {
	auto existing_entry = map.GetEntry(entry->name);
	if (existing_entry) {
		// Return null if an entry by that name already exists
		return nullptr;
	}

	auto catalog_entry = entry.get();

	entry->set = this;
	// Set the timestamp to the first committed transaction
	entry->timestamp = 0;
	map.AddEntry(std::move(entry));

	return catalog_entry;
}

bool CatalogSet::CreateEntryInternal(CatalogTransaction transaction, const string &name, unique_ptr<CatalogEntry> value,
                                     unique_lock<mutex> &read_lock, bool should_be_empty) {
	auto entry_value = map.GetEntry(name);
	if (!entry_value) {
		// Add a dummy node to start the chain
		if (!StartChain(transaction, name, read_lock)) {
			return false;
		}
	} else if (should_be_empty) {
		// Verify that the chain is deleted, not altered by another transaction
		if (!VerifyVacancy(transaction, *entry_value)) {
			return false;
		}
	}

	// Finally add the new entry to the chain
	auto value_ptr = value.get();
	map.UpdateEntry(std::move(value));
	// push the old entry in the undo buffer for this transaction
	if (transaction.transaction) {
		auto &dtransaction = transaction.transaction->Cast<DuckTransaction>();
		dtransaction.PushCatalogEntry(value_ptr->Child());
	}
	return true;
}

bool CatalogSet::CreateEntry(CatalogTransaction transaction, const string &name, unique_ptr<CatalogEntry> value,
                             const LogicalDependencyList &dependencies) {
	CheckCatalogEntryInvariants(*value, name);

	// Set the timestamp to the timestamp of the current transaction
	value->timestamp = transaction.transaction_id;
	value->set = this;
	// now add the dependency set of this object to the dependency manager
	catalog.GetDependencyManager().AddObject(transaction, *value, dependencies);

	// lock the catalog for writing
	lock_guard<mutex> write_lock(catalog.GetWriteLock());
	// lock this catalog set to disallow reading
	unique_lock<mutex> read_lock(catalog_lock);

	return CreateEntryInternal(transaction, name, std::move(value), read_lock);
}

bool CatalogSet::CreateEntry(ClientContext &context, const string &name, unique_ptr<CatalogEntry> value,
                             const LogicalDependencyList &dependencies) {
	return CreateEntry(catalog.GetCatalogTransaction(context), name, std::move(value), dependencies);
}

optional_ptr<CatalogEntry> CatalogSet::GetEntryInternal(CatalogTransaction transaction, const string &name) {
	auto entry_value = map.GetEntry(name);
	if (!entry_value) {
		// the entry does not exist, check if we can create a default entry
		return nullptr;
	}
	auto &catalog_entry = *entry_value;

	// if it does: we have to retrieve the entry and to check version numbers
	if (HasConflict(transaction, catalog_entry.timestamp)) {
		// current version has been written to by a currently active
		// transaction
		throw TransactionException("Catalog write-write conflict on alter with \"%s\"", catalog_entry.name);
	}
	// there is a current version that has been committed by this transaction
	if (catalog_entry.deleted) {
		// if the entry was already deleted, it now does not exist anymore
		// so we return that we could not find it
		return nullptr;
	}
	return &catalog_entry;
}

bool CatalogSet::AlterOwnership(CatalogTransaction transaction, ChangeOwnershipInfo &info) {
	// lock the catalog for writing
	unique_lock<mutex> write_lock(catalog.GetWriteLock());

	auto entry = GetEntryInternal(transaction, info.name);
	if (!entry) {
		return false;
	}
	optional_ptr<CatalogEntry> owner_entry;
	auto schema = catalog.GetSchema(transaction, info.owner_schema, OnEntryNotFound::RETURN_NULL);
	if (schema) {
		vector<CatalogType> entry_types {CatalogType::TABLE_ENTRY, CatalogType::SEQUENCE_ENTRY};
		for (auto entry_type : entry_types) {
			owner_entry = schema->GetEntry(transaction, entry_type, info.owner_name);
			if (owner_entry) {
				break;
			}
		}
	}
	if (!owner_entry) {
		throw CatalogException("CatalogElement \"%s.%s\" does not exist!", info.owner_schema, info.owner_name);
	}
	write_lock.unlock();
	catalog.GetDependencyManager().AddOwnership(transaction, *owner_entry, *entry);
	return true;
}

bool CatalogSet::RenameEntryInternal(CatalogTransaction transaction, CatalogEntry &old, const string &new_name,
                                     AlterInfo &alter_info, unique_lock<mutex> &read_lock) {
	auto &original_name = old.name;

	auto &context = *transaction.context;
	auto entry_value = map.GetEntry(new_name);
	if (entry_value) {
		auto &existing_entry = GetEntryForTransaction(transaction, *entry_value);
		if (!existing_entry.deleted) {
			// There exists an entry by this name that is not deleted
			old.UndoAlter(context, alter_info);
			throw CatalogException("Could not rename \"%s\" to \"%s\": another entry with this name already exists!",
			                       original_name, new_name);
		}
	}

	// Add a RENAMED_ENTRY before adding a DELETED_ENTRY, this makes it so that when this is committed
	// we know that this was not a DROP statement.
	auto renamed_tombstone = make_uniq<InCatalogEntry>(CatalogType::RENAMED_ENTRY, old.ParentCatalog(), original_name);
	renamed_tombstone->timestamp = transaction.transaction_id;
	renamed_tombstone->deleted = false;
	renamed_tombstone->set = this;
	if (!CreateEntryInternal(transaction, original_name, std::move(renamed_tombstone), read_lock,
	                         /*should_be_empty = */ false)) {
		return false;
	}
	if (!DropEntryInternal(transaction, original_name, false)) {
		return false;
	}

	// Add the renamed entry
	// Start this off with a RENAMED_ENTRY node, for commit/cleanup/rollback purposes
	auto renamed_node = make_uniq<InCatalogEntry>(CatalogType::RENAMED_ENTRY, catalog, new_name);
	renamed_node->timestamp = transaction.transaction_id;
	renamed_node->deleted = false;
	renamed_node->set = this;
	return CreateEntryInternal(transaction, new_name, std::move(renamed_node), read_lock);
}

bool CatalogSet::AlterEntry(CatalogTransaction transaction, const string &name, AlterInfo &alter_info) {
	// If the entry does not exist, we error
	auto entry = GetEntry(transaction, name);
	if (!entry) {
		return false;
	}
	if (!alter_info.allow_internal && entry->internal) {
		throw CatalogException("Cannot alter entry \"%s\" because it is an internal system entry", entry->name);
	}

	unique_ptr<CatalogEntry> value;
	if (alter_info.type == AlterType::SET_COMMENT) {
		// Copy the existing entry; we are only changing metadata here
		if (!transaction.context) {
			throw InternalException("Cannot AlterEntry::SET_COMMENT without client context");
		}
		value = entry->Copy(*transaction.context);
		value->comment = alter_info.Cast<SetCommentInfo>().comment_value;
	} else {
		// Use the existing entry to create the altered entry
		value = entry->AlterEntry(transaction, alter_info);
		if (!value) {
			// alter failed, but did not result in an error
			return true;
		}
	}

	// lock the catalog for writing
	unique_lock<mutex> write_lock(catalog.GetWriteLock());
	// lock this catalog set to disallow reading
	unique_lock<mutex> read_lock(catalog_lock);

	// fetch the entry again before doing the modification
	// this will catch any write-write conflicts between transactions
	entry = GetEntryInternal(transaction, name);

	// Mark this entry as being created by this transaction
	value->timestamp = transaction.transaction_id;
	value->set = this;

	if (!StringUtil::CIEquals(value->name, entry->name)) {
		if (!RenameEntryInternal(transaction, *entry, value->name, alter_info, read_lock)) {
			return false;
		}
	}
	auto new_entry = value.get();
	map.UpdateEntry(std::move(value));

	// push the old entry in the undo buffer for this transaction
	if (transaction.transaction) {
		// serialize the AlterInfo into a temporary buffer
		MemoryStream stream;
		BinarySerializer serializer(stream);
		serializer.Begin();
		serializer.WriteProperty(100, "column_name", alter_info.GetColumnName());
		serializer.WriteProperty(101, "alter_info", &alter_info);
		serializer.End();

		auto &dtransaction = transaction.transaction->Cast<DuckTransaction>();
		dtransaction.PushCatalogEntry(new_entry->Child(), stream.GetData(), stream.GetPosition());
	}

	read_lock.unlock();
	write_lock.unlock();

	// Check the dependency manager to verify that there are no conflicting dependencies with this alter
	catalog.GetDependencyManager().AlterObject(transaction, *entry, *new_entry, alter_info);

	return true;
}

bool CatalogSet::DropDependencies(CatalogTransaction transaction, const string &name, bool cascade,
                                  bool allow_drop_internal) {
	auto entry = GetEntry(transaction, name);
	if (!entry) {
		return false;
	}
	if (entry->internal && !allow_drop_internal) {
		throw CatalogException("Cannot drop entry \"%s\" because it is an internal system entry", entry->name);
	}
	// check any dependencies of this object
	D_ASSERT(entry->ParentCatalog().IsDuckCatalog());
	auto &duck_catalog = entry->ParentCatalog().Cast<DuckCatalog>();
	duck_catalog.GetDependencyManager().DropObject(transaction, *entry, cascade);
	return true;
}

bool CatalogSet::DropEntryInternal(CatalogTransaction transaction, const string &name, bool allow_drop_internal) {
	// lock the catalog for writing
	// we can only delete an entry that exists
	auto entry = GetEntryInternal(transaction, name);
	if (!entry) {
		return false;
	}
	if (entry->internal && !allow_drop_internal) {
		throw CatalogException("Cannot drop entry \"%s\" because it is an internal system entry", entry->name);
	}

	// create a new tombstone entry and replace the currently stored one
	// set the timestamp to the timestamp of the current transaction
	// and point it at the tombstone node
	auto value = make_uniq<InCatalogEntry>(CatalogType::DELETED_ENTRY, entry->ParentCatalog(), entry->name);
	value->timestamp = transaction.transaction_id;
	value->set = this;
	value->deleted = true;
	auto value_ptr = value.get();
	map.UpdateEntry(std::move(value));

	// push the old entry in the undo buffer for this transaction
	if (transaction.transaction) {
		auto &dtransaction = transaction.transaction->Cast<DuckTransaction>();
		dtransaction.PushCatalogEntry(value_ptr->Child());
	}
	return true;
}

bool CatalogSet::DropEntry(CatalogTransaction transaction, const string &name, bool cascade, bool allow_drop_internal) {
	if (!DropDependencies(transaction, name, cascade, allow_drop_internal)) {
		return false;
	}
	lock_guard<mutex> write_lock(catalog.GetWriteLock());
	lock_guard<mutex> read_lock(catalog_lock);
	return DropEntryInternal(transaction, name, allow_drop_internal);
}

bool CatalogSet::DropEntry(ClientContext &context, const string &name, bool cascade, bool allow_drop_internal) {
	return DropEntry(catalog.GetCatalogTransaction(context), name, cascade, allow_drop_internal);
}

DuckCatalog &CatalogSet::GetCatalog() {
	return catalog;
}

void CatalogSet::CleanupEntry(CatalogEntry &catalog_entry) {
	// destroy the backed up entry: it is no longer required
	lock_guard<mutex> write_lock(catalog.GetWriteLock());
	lock_guard<mutex> lock(catalog_lock);
	auto &parent = catalog_entry.Parent();
	map.DropEntry(catalog_entry);
	if (parent.deleted && !parent.HasChild() && !parent.HasParent()) {
		// The entry's parent is a tombstone and the entry had no child
		// clean up the mapping and the tombstone entry as well
		D_ASSERT(map.GetEntry(parent.name).get() == &parent);
		map.DropEntry(parent);
	}
}

bool CatalogSet::CreatedByOtherActiveTransaction(CatalogTransaction transaction, transaction_t timestamp) {
	// True if this transaction is not committed yet and the entry was made by another active (not committed)
	// transaction
	return (timestamp >= TRANSACTION_ID_START && timestamp != transaction.transaction_id);
}

bool CatalogSet::CommittedAfterStarting(CatalogTransaction transaction, transaction_t timestamp) {
	// The entry has been committed after this transaction started, this is not our source of truth.
	return (timestamp < TRANSACTION_ID_START && timestamp > transaction.start_time);
}

bool CatalogSet::HasConflict(CatalogTransaction transaction, transaction_t timestamp) {
	return CreatedByOtherActiveTransaction(transaction, timestamp) || CommittedAfterStarting(transaction, timestamp);
}

bool CatalogSet::UseTimestamp(CatalogTransaction transaction, transaction_t timestamp) {
	if (timestamp == transaction.transaction_id) {
		// we created this version
		return true;
	}
	if (timestamp < transaction.start_time) {
		// this version was commited before we started the transaction
		return true;
	}
	return false;
}

CatalogEntry &CatalogSet::GetEntryForTransaction(CatalogTransaction transaction, CatalogEntry &current) {
	reference<CatalogEntry> entry(current);
	while (entry.get().HasChild()) {
		if (UseTimestamp(transaction, entry.get().timestamp)) {
			break;
		}
		entry = entry.get().Child();
	}
	return entry.get();
}

CatalogEntry &CatalogSet::GetCommittedEntry(CatalogEntry &current) {
	reference<CatalogEntry> entry(current);
	while (entry.get().HasChild()) {
		if (entry.get().timestamp < TRANSACTION_ID_START) {
			// this entry is committed: use it
			break;
		}
		entry = entry.get().Child();
	}
	return entry.get();
}

SimilarCatalogEntry CatalogSet::SimilarEntry(CatalogTransaction transaction, const string &name) {
	unique_lock<mutex> lock(catalog_lock);
	CreateDefaultEntries(transaction, lock);

	SimilarCatalogEntry result;
	for (auto &kv : map.Entries()) {
		auto ldist = StringUtil::SimilarityScore(kv.first, name);
		if (ldist < result.distance) {
			result.distance = ldist;
			result.name = kv.first;
		}
	}
	return result;
}

optional_ptr<CatalogEntry> CatalogSet::CreateDefaultEntry(CatalogTransaction transaction, const string &name,
                                                          unique_lock<mutex> &read_lock) {
	// no entry found with this name, check for defaults
	if (!defaults || defaults->created_all_entries) {
		// no defaults either: return null
		return nullptr;
	}
	read_lock.unlock();
	// this catalog set has a default map defined
	// check if there is a default entry that we can create with this name
	auto entry = defaults->CreateDefaultEntry(transaction, name);

	read_lock.lock();
	if (!entry) {
		// no default entry
		return nullptr;
	}
	// there is a default entry! create it
	auto result = CreateCommittedEntry(std::move(entry));
	if (result) {
		return result;
	}
	// we found a default entry, but failed
	// this means somebody else created the entry first
	// just retry?
	read_lock.unlock();
	return GetEntry(transaction, name);
}

CatalogSet::EntryLookup CatalogSet::GetEntryDetailed(CatalogTransaction transaction, const string &name) {
	unique_lock<mutex> read_lock(catalog_lock);
	auto entry_value = map.GetEntry(name);
	if (entry_value) {
		// we found an entry for this name
		// check the version numbers

		auto &catalog_entry = *entry_value;
		auto &current = GetEntryForTransaction(transaction, catalog_entry);
		if (current.deleted) {
			return EntryLookup {nullptr, EntryLookup::FailureReason::DELETED};
		}
		D_ASSERT(StringUtil::CIEquals(name, current.name));
		return EntryLookup {&current, EntryLookup::FailureReason::SUCCESS};
	}
	auto default_entry = CreateDefaultEntry(transaction, name, read_lock);
	if (!default_entry) {
		return EntryLookup {default_entry, EntryLookup::FailureReason::NOT_PRESENT};
	}
	return EntryLookup {default_entry, EntryLookup::FailureReason::SUCCESS};
}

optional_ptr<CatalogEntry> CatalogSet::GetEntry(CatalogTransaction transaction, const string &name) {
	auto lookup = GetEntryDetailed(transaction, name);
	return lookup.result;
}

optional_ptr<CatalogEntry> CatalogSet::GetEntry(ClientContext &context, const string &name) {
	return GetEntry(catalog.GetCatalogTransaction(context), name);
}

void CatalogSet::UpdateTimestamp(CatalogEntry &entry, transaction_t timestamp) {
	entry.timestamp = timestamp;
}

void CatalogSet::Undo(CatalogEntry &entry) {
	lock_guard<mutex> write_lock(catalog.GetWriteLock());
	lock_guard<mutex> lock(catalog_lock);

	// entry has to be restored
	// and entry->parent has to be removed ("rolled back")

	// i.e. we have to place (entry) as (entry->parent) again
	auto &to_be_removed_node = entry.Parent();

	D_ASSERT(StringUtil::CIEquals(entry.name, to_be_removed_node.name));
	if (!to_be_removed_node.HasParent()) {
		to_be_removed_node.Child().SetAsRoot();
	}
	map.DropEntry(to_be_removed_node);

	if (entry.type == CatalogType::INVALID) {
		// This was the root of the entry chain
		map.DropEntry(entry);
	}
	// we mark the catalog as being modified, since this action can lead to e.g. tables being dropped
	catalog.ModifyCatalog();
}

void CatalogSet::CreateDefaultEntries(CatalogTransaction transaction, unique_lock<mutex> &read_lock) {
	if (!defaults || defaults->created_all_entries) {
		return;
	}
	// this catalog set has a default set defined:
	auto default_entries = defaults->GetDefaultEntries();
	for (auto &default_entry : default_entries) {
		auto entry_value = map.GetEntry(default_entry);
		if (!entry_value) {
			// we unlock during the CreateEntry, since it might reference other catalog sets...
			// specifically for views this can happen since the view will be bound
			read_lock.unlock();
			auto entry = defaults->CreateDefaultEntry(transaction, default_entry);
			if (!entry) {
				throw InternalException("Failed to create default entry for %s", default_entry);
			}

			read_lock.lock();
			CreateCommittedEntry(std::move(entry));
		}
	}
	defaults->created_all_entries = true;
}

void CatalogSet::Scan(CatalogTransaction transaction, const std::function<void(CatalogEntry &)> &callback) {
	// lock the catalog set
	unique_lock<mutex> lock(catalog_lock);
	CreateDefaultEntries(transaction, lock);

	for (auto &kv : map.Entries()) {
		auto &entry = *kv.second;
		auto &entry_for_transaction = GetEntryForTransaction(transaction, entry);
		if (!entry_for_transaction.deleted) {
			callback(entry_for_transaction);
		}
	}
}

void CatalogSet::Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback) {
	Scan(catalog.GetCatalogTransaction(context), callback);
}

void CatalogSet::ScanWithPrefix(CatalogTransaction transaction, const std::function<void(CatalogEntry &)> &callback,
                                const string &prefix) {
	// lock the catalog set
	unique_lock<mutex> lock(catalog_lock);
	CreateDefaultEntries(transaction, lock);

	auto &entries = map.Entries();
	auto it = entries.lower_bound(prefix);
	auto end = entries.upper_bound(prefix + char(255));
	for (; it != end; it++) {
		auto &entry = *it->second;
		auto &entry_for_transaction = GetEntryForTransaction(transaction, entry);
		if (!entry_for_transaction.deleted) {
			callback(entry_for_transaction);
		}
	}
}

void CatalogSet::Scan(const std::function<void(CatalogEntry &)> &callback) {
	// lock the catalog set
	lock_guard<mutex> lock(catalog_lock);
	for (auto &kv : map.Entries()) {
		auto &entry = *kv.second;
		auto &commited_entry = GetCommittedEntry(entry);
		if (!commited_entry.deleted) {
			callback(commited_entry);
		}
	}
}

void CatalogSet::Verify(Catalog &catalog_p) {
	D_ASSERT(&catalog_p == &catalog);
	vector<reference<CatalogEntry>> entries;
	Scan([&](CatalogEntry &entry) { entries.push_back(entry); });
	for (auto &entry : entries) {
		entry.get().Verify(catalog_p);
	}
}

} // namespace duckdb
