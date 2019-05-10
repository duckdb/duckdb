#include "transaction/transaction.hpp"

#include "catalog/catalog_entry/table_catalog_entry.hpp"
#include "common/exception.hpp"
#include "parser/column_definition.hpp"
#include "storage/data_table.hpp"

using namespace duckdb;
using namespace std;

void Transaction::PushCatalogEntry(CatalogEntry *entry) {
	// store only the pointer to the catalog entry
	CatalogEntry **blob = (CatalogEntry **)undo_buffer.CreateEntry(UndoFlags::CATALOG_ENTRY, sizeof(CatalogEntry *));
	*blob = entry;
}

void Transaction::PushDeletedEntries(uint64_t offset, uint64_t count, StorageChunk *storage,
                                     VersionInformation *version_pointers[]) {
	for (uint64_t i = 0; i < count; i++) {
		auto ptr = PushTuple(UndoFlags::INSERT_TUPLE, 0);
		auto meta = (VersionInformation *)ptr;
		meta->table = &storage->table;
		meta->tuple_data = nullptr;
		meta->version_number = transaction_id;
		meta->prev.entry = offset + i;
		meta->chunk = storage;
		meta->next = nullptr;
		version_pointers[i] = meta;
	}
}

void Transaction::PushTuple(UndoFlags flags, uint64_t offset, StorageChunk *storage) {
	// push the tuple into the undo buffer
	auto ptr = PushTuple(flags, storage->table.tuple_size);

	auto meta = (VersionInformation *)ptr;
	auto tuple_data = ptr + sizeof(VersionInformation);

	// fill in the meta data for the tuple
	meta->table = &storage->table;
	meta->tuple_data = tuple_data;
	meta->version_number = transaction_id;
	meta->prev.entry = offset;
	meta->chunk = storage;
	meta->next = storage->version_pointers[offset];
	storage->version_pointers[offset] = meta;

	if (meta->next) {
		meta->next->chunk = nullptr;
		meta->next->prev.pointer = meta;
	}

	// now fill in the tuple data
	storage->table.serializer.Serialize(storage->columns, offset, tuple_data);
}

void Transaction::PushQuery(string query) {
	char *blob = (char *)undo_buffer.CreateEntry(UndoFlags::QUERY, query.size() + 1);
	strcpy(blob, query.c_str());
}

uint8_t *Transaction::PushTuple(UndoFlags flags, uint64_t data_size) {
	return undo_buffer.CreateEntry(flags, sizeof(VersionInformation) + data_size);
}

void Transaction::Commit(WriteAheadLog *log, transaction_t commit_id) {
	this->commit_id = commit_id;
	undo_buffer.Commit(log, commit_id);
}
