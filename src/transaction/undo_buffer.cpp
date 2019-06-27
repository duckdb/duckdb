#include "transaction/undo_buffer.hpp"

#include "catalog/catalog_entry.hpp"
#include "catalog/catalog_entry/list.hpp"
#include "catalog/catalog_set.hpp"
#include "common/exception.hpp"
#include "storage/data_table.hpp"
#include "storage/storage_chunk.hpp"
#include "storage/write_ahead_log.hpp"

#include <unordered_map>
#include <transaction/transaction.hpp>

using namespace duckdb;
using namespace std;

data_ptr_t UndoBuffer::CreateEntry(UndoFlags type, index_t len) {
	UndoEntry entry;
	entry.type = type;
	entry.length = len;
	auto dataptr = new data_t[len];
	entry.data = unique_ptr<data_t[]>(dataptr);
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
			CatalogEntry *catalog_entry = *((CatalogEntry **)entry.data.get());
			// destroy the backed up entry: it is no longer required
			assert(catalog_entry->parent);
			if (catalog_entry->parent->type != CatalogType::UPDATED_ENTRY) {
				if (!catalog_entry->parent->child->deleted) {
					// delete the entry from the dependency manager, if it is not deleted yet
					catalog_entry->catalog->dependency_manager.EraseObject(catalog_entry->parent->child.get());
				}
				catalog_entry->parent->child = move(catalog_entry->child);
			}
		} else if (entry.type == UndoFlags::INSERT_TUPLE || entry.type == UndoFlags::DELETE_TUPLE ||
		           entry.type == UndoFlags::UPDATE_TUPLE) {
			// undo this entry
			auto info = (VersionInformation *)entry.data.get();
			if (entry.type == UndoFlags::DELETE_TUPLE || entry.type == UndoFlags::UPDATE_TUPLE) {
				if (info->table->indexes.size() > 0) {
					// fetch the row identifiers
					row_t row_number;
					VersionInformation *current_info = info;
					while (!current_info->chunk) {
						assert(current_info->prev.pointer);
						current_info = current_info->prev.pointer;
					}
					row_number = current_info->chunk->start + current_info->prev.entry;

					assert(info->tuple_data);
					Value ptr = Value::BIGINT(row_number);
					uint8_t *alternate_version_pointers[1];
					uint64_t alternate_version_index[1];

					alternate_version_pointers[0] = info->tuple_data;
					alternate_version_index[0] = 0;

					DataChunk result;
					result.Initialize(info->table->types);

					vector<column_t> column_ids;
					for (size_t i = 0; i < info->table->types.size(); i++) {
						column_ids.push_back(i);
					}
					Vector row_identifiers(ptr);

					info->table->RetrieveVersionedData(result, column_ids, alternate_version_pointers,
					                                   alternate_version_index, 1);
					for (auto &index : info->table->indexes) {
						index->Delete(result, row_identifiers);
					}
				}
			}
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
		} else {
			assert(entry.type == UndoFlags::EMPTY_ENTRY || entry.type == UndoFlags::QUERY);
		}
	}
}

static void WriteCatalogEntry(WriteAheadLog *log, CatalogEntry *entry) {
	if (!log) {
		return;
	}

	// look at the type of the parent entry
	auto parent = entry->parent;
	switch (parent->type) {
	case CatalogType::TABLE:
		if (entry->type == CatalogType::TABLE) {
			// ALTER TABLE statement, skip it
			return;
		}
		log->WriteCreateTable((TableCatalogEntry *)parent);
		break;
	case CatalogType::SCHEMA:
		if (entry->type == CatalogType::SCHEMA) {
			// ALTER TABLE statement, skip it
			return;
		}
		log->WriteCreateSchema((SchemaCatalogEntry *)parent);
		break;
	case CatalogType::VIEW:
		log->WriteCreateView((ViewCatalogEntry *)parent);
		break;
	case CatalogType::SEQUENCE:
		log->WriteCreateSequence((SequenceCatalogEntry *)parent);
		break;
	case CatalogType::DELETED_ENTRY:
		if (entry->type == CatalogType::TABLE) {
			log->WriteDropTable((TableCatalogEntry *)entry);
		} else if (entry->type == CatalogType::SCHEMA) {
			log->WriteDropSchema((SchemaCatalogEntry *)entry);
		} else if (entry->type == CatalogType::VIEW) {
			log->WriteDropView((ViewCatalogEntry *)entry);
		} else if (entry->type == CatalogType::SEQUENCE) {
			log->WriteDropSequence((SequenceCatalogEntry *)entry);
		} else if (entry->type == CatalogType::PREPARED_STATEMENT) {
			// do nothing, we log the query to drop this
		} else {
			throw NotImplementedException("Don't know how to drop this type!");
		}
		break;

	case CatalogType::PREPARED_STATEMENT:
		// do nothing, we log the query to recreate this
		break;
	default:
		throw NotImplementedException("UndoBuffer - don't know how to write this entry to the WAL");
	}
}

static void FlushAppends(WriteAheadLog *log, unordered_map<DataTable *, unique_ptr<DataChunk>> &appends) {
	// write appends that were not flushed yet to the WAL
	assert(log);
	if (appends.size() == 0) {
		return;
	}
	for (auto &entry : appends) {
		auto dtable = entry.first;
		auto chunk = entry.second.get();
		auto &schema_name = dtable->schema;
		auto &table_name = dtable->table;
		log->WriteInsert(schema_name, table_name, *chunk);
	}
	appends.clear();
}

static void WriteTuple(WriteAheadLog *log, VersionInformation *entry,
                       unordered_map<DataTable *, unique_ptr<DataChunk>> &appends) {
	if (!log) {
		return;
	}
	// we only insert tuples of insertions into the WAL
	// for deletions and updates we instead write the queries
	if (entry->tuple_data) {
		return;
	}
	// get the data for the insertion
	StorageChunk *storage = nullptr;
	DataChunk *chunk = nullptr;
	if (entry->chunk) {
		// versioninfo refers to data inside StorageChunk
		// fetch the data from the base rows
		storage = entry->chunk;
	} else {
		// insertion was updated or deleted after insertion in the same
		// transaction iterate back to the chunk to find the StorageChunk
		auto prev = entry->prev.pointer;
		while (!prev->chunk) {
			assert(entry->prev.pointer);
			prev = prev->prev.pointer;
		}
		storage = prev->chunk;
	}

	DataTable *dtable = &storage->table;
	// first lookup the chunk to which we will append this entry to
	auto append_entry = appends.find(dtable);
	if (append_entry != appends.end()) {
		// entry exists, check if we need to flush it
		chunk = append_entry->second.get();
		if (chunk->size() == STANDARD_VECTOR_SIZE) {
			// entry is full: flush to WAL
			auto &schema_name = dtable->schema;
			auto &table_name = dtable->table;
			log->WriteInsert(schema_name, table_name, *chunk);
			chunk->Reset();
		}
	} else {
		// entry does not exist: need to make a new entry
		auto &types = dtable->types;
		auto new_chunk = make_unique<DataChunk>();
		chunk = new_chunk.get();
		chunk->Initialize(types);
		appends.insert(make_pair(dtable, move(new_chunk)));
	}

	if (entry->chunk) {
		auto id = entry->prev.entry;
		// append the tuple to the current chunk
		index_t current_offset = chunk->size();
		for (index_t i = 0; i < chunk->column_count; i++) {
			index_t value_size = GetTypeIdSize(chunk->data[i].type);
			data_ptr_t storage_pointer = storage->GetPointerToRow(i, storage->start + id);
			memcpy(chunk->data[i].data + value_size * current_offset, storage_pointer, value_size);
			chunk->data[i].count++;
		}
	} else {
		assert(entry->prev.pointer->tuple_data);
		auto tuple_data = entry->prev.pointer->tuple_data;
		// append the tuple to the current chunk
		index_t current_offset = chunk->size();
		for (index_t i = 0; i < chunk->column_count; i++) {
			auto type = chunk->data[i].type;
			index_t value_size = GetTypeIdSize(type);
			memcpy(chunk->data[i].data + value_size * current_offset, tuple_data, value_size);
			tuple_data += value_size;
			chunk->data[i].count++;
		}
	}
}

bool UndoBuffer::ChangesMade() {
	return entries.size() > 0;
}

void UndoBuffer::Commit(WriteAheadLog *log, transaction_t commit_id) {
	// the list of appends committed by this transaction for each table
	unordered_map<DataTable *, unique_ptr<DataChunk>> appends;
	for (auto &entry : entries) {
		if (entry.type == UndoFlags::CATALOG_ENTRY) {
			// set the commit timestamp of the catalog entry to the given id
			CatalogEntry *catalog_entry = *((CatalogEntry **)entry.data.get());
			assert(catalog_entry->parent);
			catalog_entry->parent->timestamp = commit_id;

			// push the catalog update to the WAL
			WriteCatalogEntry(log, catalog_entry);
		} else if (entry.type == UndoFlags::INSERT_TUPLE || entry.type == UndoFlags::DELETE_TUPLE ||
		           entry.type == UndoFlags::UPDATE_TUPLE) {
			// set the commit timestamp of the entry
			auto info = (VersionInformation *)entry.data.get();
			info->version_number = commit_id;

			// update the cardinality of the base table
			if (entry.type == UndoFlags::INSERT_TUPLE) {
				// insertion
				info->table->cardinality++;
			} else if (entry.type == UndoFlags::DELETE_TUPLE) {
				// deletion
				info->table->cardinality--;
			}

			// push the tuple update to the WAL
			WriteTuple(log, info, appends);
		} else if (entry.type == UndoFlags::QUERY) {
			string query = string((char *)entry.data.get());
			if (log) {
				// before we write a query we write any scheduled appends
				// as the queries can reference previously appended data
				FlushAppends(log, appends);
				log->WriteQuery(query);
			}
		} else {
			throw NotImplementedException("UndoBuffer - don't know how to commit this type!");
		}
	}
	if (log) {
		// flush any remaining appends
		FlushAppends(log, appends);
	}
}

void UndoBuffer::Rollback() {
	for (index_t i = entries.size(); i > 0; i--) {
		auto &entry = entries[i - 1];
		if (entry.type == UndoFlags::CATALOG_ENTRY) {
			// undo this catalog entry
			CatalogEntry *catalog_entry = *((CatalogEntry **)entry.data.get());
			assert(catalog_entry->set);
			catalog_entry->set->Undo(catalog_entry);
		} else if (entry.type == UndoFlags::INSERT_TUPLE || entry.type == UndoFlags::DELETE_TUPLE ||
		           entry.type == UndoFlags::UPDATE_TUPLE) {
			// undo this entry
			auto info = (VersionInformation *)entry.data.get();
			if (entry.type == UndoFlags::UPDATE_TUPLE || entry.type == UndoFlags::INSERT_TUPLE) {
				// update or insert rolled back
				// delete base table entry from index
				assert(info->chunk);
				if (info->table->indexes.size() > 0) {
					row_t row_id = info->chunk->start + info->prev.entry;
					Value ptr = Value::BIGINT(row_id);

					DataChunk result;
					result.Initialize(info->table->types);

					vector<column_t> column_ids;
					for (size_t i = 0; i < info->table->types.size(); i++) {
						column_ids.push_back(i);
					}
					Vector row_identifiers(ptr);

					info->chunk->table.RetrieveTupleFromBaseTable(result, info->chunk, column_ids, row_id);
					for (auto &index : info->table->indexes) {
						index->Delete(result, row_identifiers);
					}
				}
			}
			assert(info->chunk);
			// parent needs to refer to a storage chunk because of our transactional model
			// the current entry is still dirty, hence no other transaction can have modified it
			info->chunk->Undo(info);
		} else {
			assert(entry.type == UndoFlags::EMPTY_ENTRY || entry.type == UndoFlags::QUERY);
		}
		entry.type = UndoFlags::EMPTY_ENTRY;
	}
}
