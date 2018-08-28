
#include "catalog/catalog_set.hpp"

#include "common/exception.hpp"

#include "transaction/transaction_manager.hpp"

using namespace duckdb;
using namespace std;

bool CatalogSet::CreateEntry(Transaction &transaction, const string &name,
                             unique_ptr<AbstractCatalogEntry> value) {
	lock_guard<mutex> lock(catalog_lock);

	// first check if the entry exists in the unordered set
	auto entry = data.find(name);
	if (entry == data.end()) {
		// if it does not: entry has never been created

		// first create a dummy deleted entry for this entry
		// so transactions started before the commit of this transaction don't
		// see it yet
		auto dummy_node =
		    make_unique<AbstractCatalogEntry>(value->catalog, name);
		dummy_node->timestamp = 0;
		dummy_node->deleted = true;
		dummy_node->set = this;
		data[name] = move(dummy_node);
	} else {
		// if it does, we have to check version numbers
		AbstractCatalogEntry &current = *entry->second;
		if (current.timestamp >= TRANSACTION_ID_START &&
		    current.timestamp != transaction.transaction_id) {
			// current version has been written to by a currently active
			// transaction
			throw TransactionException("Catalog write-write conflict!");
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
	value->child = move(data[name]);
	value->child->parent = value.get();
	value->set = this;
	// push the old entry in the undo buffer for this transaction
	transaction.PushCatalogEntry(value->child.get());
	data[name] = move(value);
	return true;
}

bool CatalogSet::DropEntry(Transaction &transaction, const string &name) {
	lock_guard<mutex> lock(catalog_lock);
	// we can only delete a table that exists
	auto entry = data.find(name);
	assert(entry != data.end());

	AbstractCatalogEntry &current = *entry->second;
	if (current.timestamp >= TRANSACTION_ID_START &&
	    current.timestamp != transaction.transaction_id) {
		// current version has been written to by a currently active
		// transaction
		throw TransactionException("Catalog write-write conflict!");
	}
	// there is a current version that has been committed
	// this is equivalent to what we are trying to do, so fine
	if (current.deleted) {
		return true;
	}

	auto value = make_unique<AbstractCatalogEntry>(current.catalog, name);

	// create a new entry and replace the currently stored one
	// set the timestamp to the timestamp of the current transaction
	// and point it at the dummy node
	value->timestamp = transaction.transaction_id;
	value->child = move(data[name]);
	value->child->parent = value.get();
	value->set = this;
	value->deleted = true;

	// push the old entry in the undo buffer for this transaction
	transaction.PushCatalogEntry(value->child.get());
	data[name] = move(value);
	return true;
}

bool CatalogSet::EntryExists(Transaction &transaction, const string &name) {
	lock_guard<mutex> lock(catalog_lock);

	// first check if the entry exists in the unordered set
	auto entry = data.find(name);
	if (entry == data.end()) {
		// entry has never been created
		return false;
	}
	// if it does, we have to check version numbers
	AbstractCatalogEntry *current = data[name].get();
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
	return !current->deleted;
}

AbstractCatalogEntry *CatalogSet::GetEntry(Transaction &transaction,
                                           const string &name) {
	lock_guard<mutex> lock(catalog_lock);

	auto entry = data.find(name);
	if (entry == data.end()) {
		return nullptr;
	}
	// if it does, we have to check version numbers
	AbstractCatalogEntry *current = data[name].get();
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
	if (current->deleted) {
		return nullptr;
	}
	return current;
}

void CatalogSet::Undo(AbstractCatalogEntry *entry) {
	lock_guard<mutex> lock(catalog_lock);

	// entry has to be restored
	// and entry->parent has to be removed ("rolled back")

	// i.e. we have to place (entry) as (entry->parent) again
	auto &to_be_removed_node = entry->parent;
	if (to_be_removed_node->parent) {
		// if the to be removed node has a parent, set the child pointer to the to be restored node
		to_be_removed_node->parent->child = move(to_be_removed_node->child);
		entry->parent = to_be_removed_node->parent;
	} else {
		// otherwise we need to update the base entry tables
		auto &name = entry->name;
		data[name] = move(to_be_removed_node->child);
		entry->parent = nullptr;
	}
}
