#include "duckdb/catalog/catalog_set.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/common/serializer/buffered_serializer.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"

using namespace duckdb;
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
	auto entry = data.find(name);
	if (entry == data.end()) {
		// if it does not: entry has never been created

		// first create a dummy deleted entry for this entry
		// so transactions started before the commit of this transaction don't
		// see it yet
		auto dummy_node = make_unique<CatalogEntry>(CatalogType::INVALID, value->catalog, name);
		dummy_node->timestamp = 0;
		dummy_node->deleted = true;
		dummy_node->set = this;
		data[name] = move(dummy_node);
	} else {
		// if it does, we have to check version numbers
		CatalogEntry &current = *entry->second;
		if (HasConflict(transaction, current)) {
			// current version has been written to by a currently active
			// transaction
			throw TransactionException("Catalog write-write conflict on create with \"%s\"", current.name.c_str());
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
	catalog.dependency_manager.AddObject(transaction, value.get(), dependencies);

	value->child = move(data[name]);
	value->child->parent = value.get();
	// push the old entry in the undo buffer for this transaction
	transaction.PushCatalogEntry(value->child.get());
	data[name] = move(value);
	return true;
}

bool CatalogSet::AlterEntry(ClientContext &context, const string &name, AlterInfo *alter_info) {
	auto &transaction = Transaction::GetTransaction(context);
	// lock the catalog for writing
	lock_guard<mutex> write_lock(catalog.write_lock);

	// first check if the entry exists in the unordered set
	auto entry = data.find(name);
	if (entry == data.end()) {
		// if it does not: entry has never been created and cannot be altered
		return false;
	}
	// if it does: we have to retrieve the entry and to check version numbers
	CatalogEntry &current = *entry->second;
	if (HasConflict(transaction, current)) {
		// current version has been written to by a currently active
		// transaction
		throw TransactionException("Catalog write-write conflict on alter with \"%s\"", current.name.c_str());
	}

	// lock this catalog set to disallow reading
	lock_guard<mutex> read_lock(catalog_lock);

	// create a new entry and replace the currently stored one
	// set the timestamp to the timestamp of the current transaction
	// and point it to the updated table node
	auto value = current.AlterEntry(context, alter_info);
	if (!value) {
		// alter failed, but did not result in an error
		return true;
	}

	// now transfer all dependencies from the old table to the new table
	catalog.dependency_manager.AlterObject(transaction, data[name].get(), value.get());

	value->timestamp = transaction.transaction_id;
	value->child = move(data[name]);
	value->child->parent = value.get();
	value->set = this;

	// serialize the AlterInfo into a temporary buffer
	BufferedSerializer serializer;
	alter_info->Serialize(serializer);
	BinaryData serialized_alter = serializer.GetData();

	// push the old entry in the undo buffer for this transaction
	transaction.PushCatalogEntry(value->child.get(), serialized_alter.data.get(), serialized_alter.size);
	data[name] = move(value);

	return true;
}

bool CatalogSet::DropEntry(Transaction &transaction, const string &name, bool cascade) {
	// lock the catalog for writing
	lock_guard<mutex> write_lock(catalog.write_lock);
	// we can only delete an entry that exists
	auto entry = data.find(name);
	if (entry == data.end()) {
		return false;
	}
	if (HasConflict(transaction, *entry->second)) {
		// current version has been written to by a currently active transaction
		throw TransactionException("Catalog write-write conflict on drop with \"%s\"", name.c_str());
	}
	// there is a current version that has been committed by this transaction
	if (entry->second->deleted) {
		// if the entry was already deleted, it now does not exist anymore
		// so we return that we could not find it
		return false;
	}

	// lock this catalog for reading
	// create the lock set for this delete operation
	set_lock_map_t lock_set;
	// now drop the entry
	DropEntryInternal(transaction, *entry->second, cascade, lock_set);

	return true;
}

void CatalogSet::DropEntryInternal(Transaction &transaction, CatalogEntry &current, bool cascade,
                                   set_lock_map_t &lock_set) {
	assert(data.find(current.name) != data.end());
	// first check any dependencies of this object
	current.catalog->dependency_manager.DropObject(transaction, &current, cascade, lock_set);

	// add this catalog to the lock set, if it is not there yet
	if (lock_set.find(this) == lock_set.end()) {
		lock_set.insert(make_pair(this, unique_lock<mutex>(catalog_lock)));
	}

	// create a new entry and replace the currently stored one
	// set the timestamp to the timestamp of the current transaction
	// and point it at the dummy node
	auto value = make_unique<CatalogEntry>(CatalogType::DELETED_ENTRY, current.catalog, current.name);
	value->timestamp = transaction.transaction_id;
	value->child = move(data[current.name]);
	value->child->parent = value.get();
	value->set = this;
	value->deleted = true;

	// push the old entry in the undo buffer for this transaction
	transaction.PushCatalogEntry(value->child.get());

	data[current.name] = move(value);
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

	auto entry = data.find(name);
	if (entry == data.end()) {
		return nullptr;
	}
	// if it does, we have to check version numbers
	CatalogEntry *current = GetEntryForTransaction(transaction, entry->second.get());
	if (current->deleted) {
		return nullptr;
	}
	return current;
}

CatalogEntry *CatalogSet::GetRootEntry(const string &name) {
	lock_guard<mutex> lock(catalog_lock);
	auto entry = data.find(name);
	return entry == data.end() ? nullptr : entry->second.get();
}

void CatalogSet::Undo(CatalogEntry *entry) {
	lock_guard<mutex> lock(catalog_lock);

	// entry has to be restored
	// and entry->parent has to be removed ("rolled back")

	// i.e. we have to place (entry) as (entry->parent) again
	auto &to_be_removed_node = entry->parent;
	if (!to_be_removed_node->deleted) {
		// delete the entry from the dependency manager as well
		catalog.dependency_manager.EraseObject(to_be_removed_node);
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
		data[name] = move(to_be_removed_node->child);
		entry->parent = nullptr;
	}
}
