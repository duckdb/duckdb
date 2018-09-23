
#include "transaction/undo_buffer.hpp"

#include "common/exception.hpp"

#include "catalog/abstract_catalog.hpp"
#include "catalog/catalog_set.hpp"

#include "storage/storage_chunk.hpp"
#include "storage/write_ahead_log.hpp"

using namespace duckdb;
using namespace std;

uint8_t *UndoBuffer::CreateEntry(UndoFlags type, size_t len) {
	UndoEntry entry;
	entry.type = type;
	entry.length = len;
	auto dataptr = new uint8_t[len];
	entry.data = unique_ptr<uint8_t[]>(dataptr);
	entries.push_back(move(entry));
	return dataptr;
}

void UndoBuffer::Cleanup() {
	// garbage collect everything in the Undo Chunk
	// this should only happen if
	//  (1) the transaction this UndoBuffer belongs to has successfully
	//  committed
	//      (on Rollback the Rollback() function should be called, that clears
	//      the chunks)
	//  (2) there is no active transaction with start_id < commit_id of this
	//  transaction
	for (auto &entry : entries) {
		if (entry.type == UndoFlags::CATALOG_ENTRY) {
			AbstractCatalogEntry *catalog_entry =
			    *((AbstractCatalogEntry **)entry.data.get());
			// destroy the backed up entry: it is no longer required
			assert(catalog_entry->parent);
			catalog_entry->parent->child = move(catalog_entry->child);
		} else if (entry.type == UndoFlags::TUPLE_ENTRY) {
			// undo this entry
			auto info = (VersionInformation *)entry.data.get();
			if (info->chunk) {
				// parent refers to a storage chunk
				info->chunk->Cleanup(info);
			} else {
				// parent refers to another entry in UndoBuffer
				// simply remove this entry from the list
				auto parent = info->prev.pointer;
				parent->next = info->next;
				if (parent->next) {
					parent->next->prev.pointer = parent;
				}
			}
		} else if (entry.type == UndoFlags::EMPTY_ENTRY) {
			// skip
			continue;
		} else {
			throw Exception("UndoBuffer - don't know how to garbage collect "
			                "this type!");
		}
	}
}

static void WriteCatalogEntry(WriteAheadLog *log, AbstractCatalogEntry *entry) {
	if (!log) {
		return;
	}
	// look at the type of the parent entry
	auto parent = entry->parent;
	switch (parent->type) {
	case CatalogType::TABLE: {
		auto table = (TableCatalogEntry *)parent;
		if (parent->deleted) {
			log->WriteDropTable(table);
		} else {
			log->WriteCreateTable(table);
		}
		break;
	}
	case CatalogType::SCHEMA: {
		auto schema = (SchemaCatalogEntry *)parent;
		if (parent->deleted) {
			log->WriteDropSchema(schema);
		} else {
			log->WriteCreateSchema(schema);
		}
		break;
	}
	default:
		throw Exception(
		    "UndoBuffer - don't know how to write this entry to the WAL");
	}
}

static void WriteTuple(WriteAheadLog *log, VersionInformation *entry) {
	if (!log) {
		return;
	}
}

void UndoBuffer::Commit(WriteAheadLog *log, transaction_t commit_id) {
	for (auto &entry : entries) {
		if (entry.type == UndoFlags::CATALOG_ENTRY) {
			// set the commit timestamp of the catalog entry to the given id
			AbstractCatalogEntry *catalog_entry =
			    *((AbstractCatalogEntry **)entry.data.get());
			assert(catalog_entry->parent);
			catalog_entry->parent->timestamp = commit_id;

			// push the catalog update to the WAL
			WriteCatalogEntry(log, catalog_entry);
		} else if (entry.type == UndoFlags::TUPLE_ENTRY) {
			// set the commit timestamp of the entry
			auto info = (VersionInformation *)entry.data.get();
			info->version_number = commit_id;

			// push the tuple update to the WAL
			WriteTuple(log, info);
		} else {
			throw Exception("UndoBuffer - don't know how to commit this type!");
		}
	}
	if (log) {
		// flush the WAL
		log->Flush();
	}
}

void UndoBuffer::Rollback() {
	for (size_t i = entries.size(); i > 0; i--) {
		auto &entry = entries[i - 1];
		if (entry.type == UndoFlags::CATALOG_ENTRY) {
			// undo this catalog entry
			AbstractCatalogEntry *catalog_entry =
			    *((AbstractCatalogEntry **)entry.data.get());
			assert(catalog_entry->set);
			catalog_entry->set->Undo(catalog_entry);
		} else if (entry.type == UndoFlags::TUPLE_ENTRY) {
			// undo this entry
			auto info = (VersionInformation *)entry.data.get();
			if (info->chunk) {
				// parent refers to a storage chunk
				// have to move information back into chunk
				info->chunk->Undo(info);
			} else {
				// parent refers to another entry in UndoBuffer
				// simply remove this entry from the list
				auto parent = info->prev.pointer;
				parent->next = info->next;
				if (parent->next) {
					parent->next->prev.pointer = parent;
				}
			}
		} else {
			throw Exception(
			    "UndoBuffer - don't know how to rollback this type!");
		}
		entry.type = UndoFlags::EMPTY_ENTRY;
	}
}
