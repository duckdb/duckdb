#include "duckdb/catalog/catalog_set.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/common/serializer/buffered_serializer.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/catalog/dependency_manager.hpp"

namespace duckdb {
using namespace std;

CatalogSet::CatalogSet(Catalog &catalog) : catalog(catalog) {
}

bool CatalogSet::CreateEntry(Transaction &transaction, const string &name, unique_ptr<CatalogEntry> value,
                             unordered_set<CatalogEntry *> &dependencies) {
	// lock the catalog for writing
	lock_guard<mutex> write_lock(catalog.write_lock);
	// lock this catalog set to disallow reading
	lock_guard<mutex> read_lock(catalog_lock);

	// first check if the entry exists in the unordered set
	idx_t entry_index;
	auto entry = mapping.find(name);
	if (entry == mapping.end()) {
		// if it does not: entry has never been created

		// first create a dummy deleted entry for this entry
		// so transactions started before the commit of this transaction don't
		// see it yet
		entry_index = current_entry++;
		auto dummy_node = make_unique<CatalogEntry>(CatalogType::INVALID, value->catalog, name);
		dummy_node->timestamp = 0;
		dummy_node->deleted = true;
		dummy_node->set = this;

		entries[entry_index] = move(dummy_node);
		mapping[name] = entry_index;
	} else {
		entry_index = entry->second;
		auto &current = *entries[entry_index];
		// if it does, we have to check version numbers
		if (HasConflict(transaction, current)) {
			// current version has been written to by a currently active
			// transaction
			throw TransactionException("Catalog write-write conflict on create with \"%s\"", current.name);
		}
		// there is a current version that has been committed
		// if it has not been deleted there is a conflict
		if (!current.deleted) {
			return false;
		}
	}
	// create a new entry and replace the currently stored one
	// set the timestamp to the timestamp of the current transaction
	// and point it at the dummy node
	value->timestamp = transaction.transaction_id;
	value->set = this;

	// now add the dependency set of this object to the dependency manager
	catalog.dependency_manager->AddObject(transaction, value.get(), dependencies);

	value->child = move(entries[entry_index]);
	value->child->parent = value.get();
	// push the old entry in the undo buffer for this transaction
	transaction.PushCatalogEntry(value->child.get());
	entries[entry_index] = move(value);
	return true;
}

bool CatalogSet::GetEntryInternal(Transaction &transaction, idx_t entry_index, CatalogEntry *&catalog_entry) {
	catalog_entry = entries[entry_index].get();
	// if it does: we have to retrieve the entry and to check version numbers
	if (HasConflict(transaction, *catalog_entry)) {
		// current version has been written to by a currently active
		// transaction
		throw TransactionException("Catalog write-write conflict on alter with \"%s\"", catalog_entry->name);
	}
	// there is a current version that has been committed by this transaction
	if (catalog_entry->deleted) {
		// if the entry was already deleted, it now does not exist anymore
		// so we return that we could not find it
		return false;
	}
	return true;
}

bool CatalogSet::GetEntryInternal(Transaction &transaction, const string &name, idx_t &entry_index,
                                  CatalogEntry *&catalog_entry) {
	auto entry = mapping.find(name);
	if (entry == mapping.end()) {
		// if it does not: entry has never been created and cannot be altered
		return false;
	}
	entry_index = entry->second;
	return GetEntryInternal(transaction, entry_index, catalog_entry);
}

void CatalogSet::ClearEntryName(string name) {
	auto entry = mapping.find(name);
	assert(entry != mapping.end());
	mapping.erase(entry);
}

bool CatalogSet::AlterEntry(ClientContext &context, const string &name, AlterInfo *alter_info) {
	auto &transaction = Transaction::GetTransaction(context);
	// lock the catalog for writing
	lock_guard<mutex> write_lock(catalog.write_lock);

	// first check if the entry exists in the unordered set
	idx_t entry_index;
	CatalogEntry *entry;
	if (!GetEntryInternal(transaction, name, entry_index, entry)) {
		return false;
	}

	// lock this catalog set to disallow reading
	lock_guard<mutex> read_lock(catalog_lock);

	// create a new entry and replace the currently stored one
	// set the timestamp to the timestamp of the current transaction
	// and point it to the updated table node
	auto value = entry->AlterEntry(context, alter_info);
	if (!value) {
		// alter failed, but did not result in an error
		return true;
	}
	if (value->name != name) {
		if (mapping.find(value->name) != mapping.end()) {
			throw CatalogException("Could not rename \"%s\" to \"%s\": another entry with this name already exists!",
			                       name, value->name);
		}
		mapping[value->name] = entry_index;
	}
	//! Check the dependency manager to verify that there are no conflicting dependencies with this alter
	catalog.dependency_manager->AlterObject(transaction, entry);

	value->timestamp = transaction.transaction_id;
	value->child = move(entries[entry_index]);
	value->child->parent = value.get();
	value->set = this;

	// serialize the AlterInfo into a temporary buffer
	BufferedSerializer serializer;
	alter_info->Serialize(serializer);
	BinaryData serialized_alter = serializer.GetData();

	// push the old entry in the undo buffer for this transaction
	transaction.PushCatalogEntry(value->child.get(), serialized_alter.data.get(), serialized_alter.size);
	entries[entry_index] = move(value);

	return true;
}

void CatalogSet::DropEntryInternal(Transaction &transaction, idx_t entry_index, CatalogEntry &entry, bool cascade,
                                   set_lock_map_t &lock_set) {
	// check any dependencies of this object
	entry.catalog->dependency_manager->DropObject(transaction, &entry, cascade, lock_set);

	// add this catalog to the lock set, if it is not there yet
	if (lock_set.find(this) == lock_set.end()) {
		lock_set.insert(make_pair(this, unique_lock<mutex>(catalog_lock)));
	}

	// create a new entry and replace the currently stored one
	// set the timestamp to the timestamp of the current transaction
	// and point it at the dummy node
	auto value = make_unique<CatalogEntry>(CatalogType::DELETED_ENTRY, entry.catalog, entry.name);
	value->timestamp = transaction.transaction_id;
	value->child = move(entries[entry_index]);
	value->child->parent = value.get();
	value->set = this;
	value->deleted = true;

	// push the old entry in the undo buffer for this transaction
	transaction.PushCatalogEntry(value->child.get());

	entries[entry_index] = move(value);
}

bool CatalogSet::DropEntry(Transaction &transaction, const string &name, bool cascade) {
	// lock the catalog for writing
	lock_guard<mutex> write_lock(catalog.write_lock);
	// we can only delete an entry that exists
	idx_t entry_index;
	CatalogEntry *entry;
	if (!GetEntryInternal(transaction, name, entry_index, entry)) {
		return false;
	}

	// create the lock set for this delete operation
	set_lock_map_t lock_set;
	DropEntryInternal(transaction, entry_index, *entry, cascade, lock_set);
	return true;
}

idx_t CatalogSet::GetEntryIndex(CatalogEntry *entry) {
	assert(mapping.find(entry->name) != mapping.end());
	idx_t entry_index = mapping[entry->name];
	return entry_index;
}

bool CatalogSet::HasConflict(Transaction &transaction, CatalogEntry &current) {
	return (current.timestamp >= TRANSACTION_ID_START && current.timestamp != transaction.transaction_id) ||
	       (current.timestamp < TRANSACTION_ID_START && current.timestamp > transaction.start_time);
}

CatalogEntry *CatalogSet::GetEntryForTransaction(Transaction &transaction, CatalogEntry *current) {
	while (current->child) {
		if (current->timestamp == transaction.transaction_id) {
			// we created this version
			break;
		}
		if (current->timestamp < transaction.start_time) {
			// this version was commited before we started the transaction
			break;
		}
		current = current->child.get();
		assert(current);
	}
	return current;
}

CatalogEntry *CatalogSet::GetEntry(Transaction &transaction, const string &name) {
	lock_guard<mutex> lock(catalog_lock);

	auto entry = mapping.find(name);
	if (entry == mapping.end()) {
		return nullptr;
	}
	auto catalog_entry = entries[entry->second].get();
	// if it does, we have to check version numbers
	CatalogEntry *current = GetEntryForTransaction(transaction, catalog_entry);
	if (current->deleted || current->name != name) {
		return nullptr;
	}
	return current;
}

CatalogEntry *CatalogSet::GetRootEntry(const string &name) {
	lock_guard<mutex> lock(catalog_lock);
	auto entry = mapping.find(name);
	return entry == mapping.end() ? nullptr : entries[entry->second].get();
}

void CatalogSet::Undo(CatalogEntry *entry) {
	lock_guard<mutex> lock(catalog_lock);

	// entry has to be restored
	// and entry->parent has to be removed ("rolled back")

	// i.e. we have to place (entry) as (entry->parent) again
	auto &to_be_removed_node = entry->parent;
	if (!to_be_removed_node->deleted) {
		// delete the entry from the dependency manager as well
		catalog.dependency_manager->EraseObject(to_be_removed_node);
	}
	if (entry->name != to_be_removed_node->name) {
		// rename: clean up the new name when the rename is rolled back
		mapping.erase(mapping.find(to_be_removed_node->name));
	}
	if (to_be_removed_node->parent) {
		// if the to be removed node has a parent, set the child pointer to the
		// to be restored node
		to_be_removed_node->parent->child = move(to_be_removed_node->child);
		entry->parent = to_be_removed_node->parent;
	} else {
		// otherwise we need to update the base entry tables
		auto &name = entry->name;
		to_be_removed_node->child->SetAsRoot();
		entries[mapping[name]] = move(to_be_removed_node->child);
		entry->parent = nullptr;
	}
}

} // namespace duckdb
