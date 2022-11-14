#include "duckdb/catalog/catalog_set.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/common/serializer/buffered_serializer.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/catalog/dependency_manager.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"

namespace duckdb {

//! Class responsible to keep track of state when removing entries from the catalog.
//! When deleting, many types of errors can be thrown, since we want to avoid try/catch blocks
//! this class makes sure that whatever elements were modified are returned to a correct state
//! when exceptions are thrown.
//! The idea here is to use RAII (Resource acquisition is initialization) to mimic a try/catch/finally block.
//! If any exception is raised when this object exists, then its destructor will be called
//! and the entry will return to its previous state during deconstruction.
class EntryDropper {
public:
	//! Both constructor and destructor are privates because they should only be called by DropEntryDependencies
	explicit EntryDropper(CatalogSet &catalog_set, idx_t entry_index)
	    : catalog_set(catalog_set), entry_index(entry_index) {
		old_deleted = catalog_set.entries[entry_index].get()->deleted;
	}

	~EntryDropper() {
		catalog_set.entries[entry_index].get()->deleted = old_deleted;
	}

private:
	//! The current catalog_set
	CatalogSet &catalog_set;
	//! Keeps track of the state of the entry before starting the delete
	bool old_deleted;
	//! Index of entry to be deleted
	idx_t entry_index;
};

CatalogSet::CatalogSet(Catalog &catalog, unique_ptr<DefaultGenerator> defaults)
    : catalog(catalog), defaults(move(defaults)) {
}

bool CatalogSet::CreateEntry(ClientContext &context, const string &name, unique_ptr<CatalogEntry> value,
                             unordered_set<CatalogEntry *> &dependencies) {
	auto &transaction = Transaction::GetTransaction(context);
	// lock the catalog for writing
	lock_guard<mutex> write_lock(catalog.write_lock);
	// lock this catalog set to disallow reading
	unique_lock<mutex> read_lock(catalog_lock);

	// first check if the entry exists in the unordered set
	idx_t entry_index;
	auto mapping_value = GetMapping(context, name);
	if (mapping_value == nullptr || mapping_value->deleted) {
		// if it does not: entry has never been created

		// check if there is a default entry
		auto entry = CreateDefaultEntry(context, name, read_lock);
		if (entry) {
			return false;
		}

		// first create a dummy deleted entry for this entry
		// so transactions started before the commit of this transaction don't
		// see it yet
		entry_index = current_entry++;
		auto dummy_node = make_unique<CatalogEntry>(CatalogType::INVALID, value->catalog, name);
		dummy_node->timestamp = 0;
		dummy_node->deleted = true;
		dummy_node->set = this;

		entries[entry_index] = move(dummy_node);
		PutMapping(context, name, entry_index);
	} else {
		entry_index = mapping_value->index;
		auto &current = *entries[entry_index];
		// if it does, we have to check version numbers
		if (HasConflict(context, current.timestamp)) {
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
	catalog.dependency_manager->AddObject(context, value.get(), dependencies);

	value->child = move(entries[entry_index]);
	value->child->parent = value.get();
	// push the old entry in the undo buffer for this transaction
	transaction.PushCatalogEntry(value->child.get());
	entries[entry_index] = move(value);
	return true;
}

bool CatalogSet::GetEntryInternal(ClientContext &context, idx_t entry_index, CatalogEntry *&catalog_entry) {
	catalog_entry = entries[entry_index].get();
	// if it does: we have to retrieve the entry and to check version numbers
	if (HasConflict(context, catalog_entry->timestamp)) {
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

bool CatalogSet::GetEntryInternal(ClientContext &context, const string &name, idx_t &entry_index,
                                  CatalogEntry *&catalog_entry) {
	auto mapping_value = GetMapping(context, name);
	if (mapping_value == nullptr || mapping_value->deleted) {
		// the entry does not exist, check if we can create a default entry
		return false;
	}
	entry_index = mapping_value->index;
	return GetEntryInternal(context, entry_index, catalog_entry);
}

bool CatalogSet::AlterOwnership(ClientContext &context, ChangeOwnershipInfo *info) {
	idx_t entry_index;
	CatalogEntry *entry;
	if (!GetEntryInternal(context, info->name, entry_index, entry)) {
		return false;
	}

	auto owner_entry = catalog.GetEntry(context, info->owner_schema, info->owner_name);
	if (!owner_entry) {
		return false;
	}

	catalog.dependency_manager->AddOwnership(context, owner_entry, entry);

	return true;
}

bool CatalogSet::AlterEntry(ClientContext &context, const string &name, AlterInfo *alter_info) {
	auto &transaction = Transaction::GetTransaction(context);
	// lock the catalog for writing
	lock_guard<mutex> write_lock(catalog.write_lock);

	// first check if the entry exists in the unordered set
	idx_t entry_index;
	CatalogEntry *entry;
	if (!GetEntryInternal(context, name, entry_index, entry)) {
		return false;
	}
	if (entry->internal) {
		throw CatalogException("Cannot alter entry \"%s\" because it is an internal system entry", entry->name);
	}

	// lock this catalog set to disallow reading
	lock_guard<mutex> read_lock(catalog_lock);

	// create a new entry and replace the currently stored one
	// set the timestamp to the timestamp of the current transaction
	// and point it to the updated table node
	string original_name = entry->name;
	auto value = entry->AlterEntry(context, alter_info);
	if (!value) {
		// alter failed, but did not result in an error
		return true;
	}

	if (value->name != original_name) {
		auto mapping_value = GetMapping(context, value->name);
		if (mapping_value && !mapping_value->deleted) {
			auto entry = GetEntryForTransaction(context, entries[mapping_value->index].get());
			if (!entry->deleted) {
				string rename_err_msg =
				    "Could not rename \"%s\" to \"%s\": another entry with this name already exists!";
				throw CatalogException(rename_err_msg, original_name, value->name);
			}
		}
	}

	if (value->name != original_name) {
		// Do PutMapping and DeleteMapping after dependency check
		PutMapping(context, value->name, entry_index);
		DeleteMapping(context, original_name);
	}

	value->timestamp = transaction.transaction_id;
	value->child = move(entries[entry_index]);
	value->child->parent = value.get();
	value->set = this;

	// serialize the AlterInfo into a temporary buffer
	BufferedSerializer serializer;
	alter_info->Serialize(serializer);
	BinaryData serialized_alter = serializer.GetData();

	auto new_entry = value.get();

	// push the old entry in the undo buffer for this transaction
	transaction.PushCatalogEntry(value->child.get(), serialized_alter.data.get(), serialized_alter.size);
	entries[entry_index] = move(value);

	// Check the dependency manager to verify that there are no conflicting dependencies with this alter
	// Note that we do this AFTER the new entry has been entirely set up in the catalog set
	// that is because in case the alter fails because of a dependency conflict, we need to be able to cleanly roll back
	// to the old entry.
	catalog.dependency_manager->AlterObject(context, entry, new_entry);

	return true;
}

void CatalogSet::DropEntryDependencies(ClientContext &context, idx_t entry_index, CatalogEntry &entry, bool cascade) {

	// Stores the deleted value of the entry before starting the process
	EntryDropper dropper(*this, entry_index);

	// To correctly delete the object and its dependencies, it temporarily is set to deleted.
	entries[entry_index].get()->deleted = true;

	// check any dependencies of this object
	entry.catalog->dependency_manager->DropObject(context, &entry, cascade);

	// dropper destructor is called here
	// the destructor makes sure to return the value to the previous state
	// dropper.~EntryDropper()
}

void CatalogSet::DropEntryInternal(ClientContext &context, idx_t entry_index, CatalogEntry &entry, bool cascade) {
	auto &transaction = Transaction::GetTransaction(context);

	DropEntryDependencies(context, entry_index, entry, cascade);

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

bool CatalogSet::DropEntry(ClientContext &context, const string &name, bool cascade) {
	// lock the catalog for writing
	lock_guard<mutex> write_lock(catalog.write_lock);
	// we can only delete an entry that exists
	idx_t entry_index;
	CatalogEntry *entry;
	if (!GetEntryInternal(context, name, entry_index, entry)) {
		return false;
	}
	if (entry->internal) {
		throw CatalogException("Cannot drop entry \"%s\" because it is an internal system entry", entry->name);
	}

	DropEntryInternal(context, entry_index, *entry, cascade);
	return true;
}

void CatalogSet::CleanupEntry(CatalogEntry *catalog_entry) {
	// destroy the backed up entry: it is no longer required
	D_ASSERT(catalog_entry->parent);
	if (catalog_entry->parent->type != CatalogType::UPDATED_ENTRY) {
		lock_guard<mutex> lock(catalog_lock);
		if (!catalog_entry->deleted) {
			// delete the entry from the dependency manager, if it is not deleted yet
			catalog_entry->catalog->dependency_manager->EraseObject(catalog_entry);
		}
		auto parent = catalog_entry->parent;
		parent->child = move(catalog_entry->child);
		if (parent->deleted && !parent->child && !parent->parent) {
			auto mapping_entry = mapping.find(parent->name);
			D_ASSERT(mapping_entry != mapping.end());
			auto index = mapping_entry->second->index;
			auto entry = entries.find(index);
			D_ASSERT(entry != entries.end());
			if (entry->second.get() == parent) {
				mapping.erase(mapping_entry);
				entries.erase(entry);
			}
		}
	}
}

bool CatalogSet::HasConflict(ClientContext &context, transaction_t timestamp) {
	auto &transaction = Transaction::GetTransaction(context);
	return (timestamp >= TRANSACTION_ID_START && timestamp != transaction.transaction_id) ||
	       (timestamp < TRANSACTION_ID_START && timestamp > transaction.start_time);
}

MappingValue *CatalogSet::GetMapping(ClientContext &context, const string &name, bool get_latest) {
	MappingValue *mapping_value;
	auto entry = mapping.find(name);
	if (entry != mapping.end()) {
		mapping_value = entry->second.get();
	} else {

		return nullptr;
	}
	if (get_latest) {
		return mapping_value;
	}
	while (mapping_value->child) {
		if (UseTimestamp(context, mapping_value->timestamp)) {
			break;
		}
		mapping_value = mapping_value->child.get();
		D_ASSERT(mapping_value);
	}
	return mapping_value;
}

void CatalogSet::PutMapping(ClientContext &context, const string &name, idx_t entry_index) {
	auto entry = mapping.find(name);
	auto new_value = make_unique<MappingValue>(entry_index);
	new_value->timestamp = Transaction::GetTransaction(context).transaction_id;
	if (entry != mapping.end()) {
		if (HasConflict(context, entry->second->timestamp)) {
			throw TransactionException("Catalog write-write conflict on name \"%s\"", name);
		}
		new_value->child = move(entry->second);
		new_value->child->parent = new_value.get();
	}
	mapping[name] = move(new_value);
}

void CatalogSet::DeleteMapping(ClientContext &context, const string &name) {
	auto entry = mapping.find(name);
	D_ASSERT(entry != mapping.end());
	auto delete_marker = make_unique<MappingValue>(entry->second->index);
	delete_marker->deleted = true;
	delete_marker->timestamp = Transaction::GetTransaction(context).transaction_id;
	delete_marker->child = move(entry->second);
	delete_marker->child->parent = delete_marker.get();
	mapping[name] = move(delete_marker);
}

bool CatalogSet::UseTimestamp(ClientContext &context, transaction_t timestamp) {
	auto &transaction = Transaction::GetTransaction(context);
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

CatalogEntry *CatalogSet::GetEntryForTransaction(ClientContext &context, CatalogEntry *current) {
	while (current->child) {
		if (UseTimestamp(context, current->timestamp)) {
			break;
		}
		current = current->child.get();
		D_ASSERT(current);
	}
	return current;
}

CatalogEntry *CatalogSet::GetCommittedEntry(CatalogEntry *current) {
	while (current->child) {
		if (current->timestamp < TRANSACTION_ID_START) {
			// this entry is committed: use it
			break;
		}
		current = current->child.get();
		D_ASSERT(current);
	}
	return current;
}

pair<string, idx_t> CatalogSet::SimilarEntry(ClientContext &context, const string &name) {
	unique_lock<mutex> lock(catalog_lock);
	CreateDefaultEntries(context, lock);

	string result;
	idx_t current_score = (idx_t)-1;
	for (auto &kv : mapping) {
		auto mapping_value = GetMapping(context, kv.first);
		if (mapping_value && !mapping_value->deleted) {
			auto ldist = StringUtil::LevenshteinDistance(kv.first, name);
			if (ldist < current_score) {
				current_score = ldist;
				result = kv.first;
			}
		}
	}
	return {result, current_score};
}

CatalogEntry *CatalogSet::CreateEntryInternal(ClientContext &context, unique_ptr<CatalogEntry> entry) {
	if (mapping.find(entry->name) != mapping.end()) {
		return nullptr;
	}
	auto &name = entry->name;
	auto entry_index = current_entry++;
	auto catalog_entry = entry.get();

	entry->set = this;
	entry->timestamp = 0;

	PutMapping(context, name, entry_index);
	mapping[name]->timestamp = 0;
	entries[entry_index] = move(entry);
	return catalog_entry;
}

CatalogEntry *CatalogSet::CreateDefaultEntry(ClientContext &context, const string &name, unique_lock<mutex> &lock) {
	// no entry found with this name, check for defaults
	if (!defaults || defaults->created_all_entries) {
		// no defaults either: return null
		return nullptr;
	}
	// this catalog set has a default map defined
	// check if there is a default entry that we can create with this name
	lock.unlock();
	auto entry = defaults->CreateDefaultEntry(context, name);

	lock.lock();
	if (!entry) {
		// no default entry
		return nullptr;
	}
	// there is a default entry! create it
	auto result = CreateEntryInternal(context, move(entry));
	if (result) {
		return result;
	}
	// we found a default entry, but failed
	// this means somebody else created the entry first
	// just retry?
	lock.unlock();
	return GetEntry(context, name);
}

CatalogEntry *CatalogSet::GetEntry(ClientContext &context, const string &name) {
	unique_lock<mutex> lock(catalog_lock);
	auto mapping_value = GetMapping(context, name);
	if (mapping_value != nullptr && !mapping_value->deleted) {
		// we found an entry for this name
		// check the version numbers

		auto catalog_entry = entries[mapping_value->index].get();
		CatalogEntry *current = GetEntryForTransaction(context, catalog_entry);
		if (current->deleted || (current->name != name && !UseTimestamp(context, mapping_value->timestamp))) {
			return nullptr;
		}
		return current;
	}
	return CreateDefaultEntry(context, name, lock);
}

void CatalogSet::UpdateTimestamp(CatalogEntry *entry, transaction_t timestamp) {
	entry->timestamp = timestamp;
	mapping[entry->name]->timestamp = timestamp;
}

void CatalogSet::AdjustUserDependency(CatalogEntry *entry, ColumnDefinition &column, bool remove) {
	CatalogEntry *user_type_catalog = (CatalogEntry *)LogicalType::GetCatalog(column.Type());
	if (user_type_catalog) {
		if (remove) {
			catalog.dependency_manager->dependents_map[user_type_catalog].erase(entry->parent);
			catalog.dependency_manager->dependencies_map[entry->parent].erase(user_type_catalog);
		} else {
			catalog.dependency_manager->dependents_map[user_type_catalog].insert(entry);
			catalog.dependency_manager->dependencies_map[entry].insert(user_type_catalog);
		}
	}
}

void CatalogSet::AdjustDependency(CatalogEntry *entry, TableCatalogEntry *table, ColumnDefinition &column,
                                  bool remove) {
	bool found = false;
	if (column.Type().id() == LogicalTypeId::ENUM) {
		for (auto &old_column : table->columns.Logical()) {
			if (old_column.Name() == column.Name() && old_column.Type().id() != LogicalTypeId::ENUM) {
				AdjustUserDependency(entry, column, remove);
				found = true;
			}
		}
		if (!found) {
			AdjustUserDependency(entry, column, remove);
		}
	} else if (!(column.Type().GetAlias().empty())) {
		auto alias = column.Type().GetAlias();
		for (auto &old_column : table->columns.Logical()) {
			auto old_alias = old_column.Type().GetAlias();
			if (old_column.Name() == column.Name() && old_alias != alias) {
				AdjustUserDependency(entry, column, remove);
				found = true;
			}
		}
		if (!found) {
			AdjustUserDependency(entry, column, remove);
		}
	}
}

void CatalogSet::AdjustTableDependencies(CatalogEntry *entry) {
	if (entry->type == CatalogType::TABLE_ENTRY && entry->parent->type == CatalogType::TABLE_ENTRY) {
		// If it's a table entry we have to check for possibly removing or adding user type dependencies
		auto old_table = (TableCatalogEntry *)entry->parent;
		auto new_table = (TableCatalogEntry *)entry;

		for (idx_t i = 0; i < new_table->columns.LogicalColumnCount(); i++) {
			auto &new_column = new_table->columns.GetColumnMutable(LogicalIndex(i));
			AdjustDependency(entry, old_table, new_column, false);
		}
		for (idx_t i = 0; i < old_table->columns.LogicalColumnCount(); i++) {
			auto &old_column = old_table->columns.GetColumnMutable(LogicalIndex(i));
			AdjustDependency(entry, new_table, old_column, true);
		}
	}
}

void CatalogSet::Undo(CatalogEntry *entry) {
	lock_guard<mutex> write_lock(catalog.write_lock);

	lock_guard<mutex> lock(catalog_lock);

	// entry has to be restored
	// and entry->parent has to be removed ("rolled back")

	// i.e. we have to place (entry) as (entry->parent) again
	auto &to_be_removed_node = entry->parent;

	AdjustTableDependencies(entry);

	if (!to_be_removed_node->deleted) {
		// delete the entry from the dependency manager as well
		catalog.dependency_manager->EraseObject(to_be_removed_node);
	}
	if (entry->name != to_be_removed_node->name) {
		// rename: clean up the new name when the rename is rolled back
		auto removed_entry = mapping.find(to_be_removed_node->name);
		if (removed_entry->second->child) {
			removed_entry->second->child->parent = nullptr;
			mapping[to_be_removed_node->name] = move(removed_entry->second->child);
		} else {
			mapping.erase(removed_entry);
		}
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
		entries[mapping[name]->index] = move(to_be_removed_node->child);
		entry->parent = nullptr;
	}

	// restore the name if it was deleted
	auto restored_entry = mapping.find(entry->name);
	if (restored_entry->second->deleted || entry->type == CatalogType::INVALID) {
		if (restored_entry->second->child) {
			restored_entry->second->child->parent = nullptr;
			mapping[entry->name] = move(restored_entry->second->child);
		} else {
			mapping.erase(restored_entry);
		}
	}
	// we mark the catalog as being modified, since this action can lead to e.g. tables being dropped
	entry->catalog->ModifyCatalog();
}

void CatalogSet::CreateDefaultEntries(ClientContext &context, unique_lock<mutex> &lock) {
	if (!defaults || defaults->created_all_entries) {
		return;
	}
	// this catalog set has a default set defined:
	auto default_entries = defaults->GetDefaultEntries();
	for (auto &default_entry : default_entries) {
		auto map_entry = mapping.find(default_entry);
		if (map_entry == mapping.end()) {
			// we unlock during the CreateEntry, since it might reference other catalog sets...
			// specifically for views this can happen since the view will be bound
			lock.unlock();
			auto entry = defaults->CreateDefaultEntry(context, default_entry);
			if (!entry) {
				throw InternalException("Failed to create default entry for %s", default_entry);
			}

			lock.lock();
			CreateEntryInternal(context, move(entry));
		}
	}
	defaults->created_all_entries = true;
}

void CatalogSet::Scan(ClientContext &context, const std::function<void(CatalogEntry *)> &callback) {
	// lock the catalog set
	unique_lock<mutex> lock(catalog_lock);
	CreateDefaultEntries(context, lock);

	for (auto &kv : entries) {
		auto entry = kv.second.get();
		entry = GetEntryForTransaction(context, entry);
		if (!entry->deleted) {
			callback(entry);
		}
	}
}

void CatalogSet::Scan(const std::function<void(CatalogEntry *)> &callback) {
	// lock the catalog set
	lock_guard<mutex> lock(catalog_lock);
	for (auto &kv : entries) {
		auto entry = kv.second.get();
		entry = GetCommittedEntry(entry);
		if (!entry->deleted) {
			callback(entry);
		}
	}
}
} // namespace duckdb
