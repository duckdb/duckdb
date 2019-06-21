#include "transaction/undo_buffer.hpp"

#include "catalog/catalog_entry.hpp"
#include "catalog/catalog_entry/list.hpp"
#include "catalog/catalog_set.hpp"
#include "common/exception.hpp"
#include "storage/data_table.hpp"
#include "storage/table/version_chunk.hpp"
#include "storage/write_ahead_log.hpp"
#include "transaction/version_info.hpp"

#include <unordered_map>

using namespace duckdb;
using namespace std;

constexpr uint32_t DEFAULT_UNDO_CHUNK_SIZE = 4096 * 3;
constexpr uint32_t UNDO_ENTRY_HEADER_SIZE = sizeof(UndoFlags) + sizeof(uint32_t);

static void CleanupIndexInsert(VersionInfo *info);
static void RollbackIndexInsert(VersionInfo *info);

UndoBuffer::UndoBuffer() {
	head = make_unique<UndoChunk>(DEFAULT_UNDO_CHUNK_SIZE);
	tail = head.get();
}

UndoChunk::UndoChunk(index_t size) : current_position(0), maximum_size(size), prev(nullptr) {
	data = unique_ptr<data_t[]>(new data_t[maximum_size]);
}
UndoChunk::~UndoChunk() {
	if (next) {
		auto current_next = move(next);
		while (current_next) {
			current_next = move(current_next->next);
		}
	}
}

data_ptr_t UndoChunk::WriteEntry(UndoFlags type, uint32_t len) {
	*((UndoFlags *)(data.get() + current_position)) = type;
	current_position += sizeof(UndoFlags);
	*((uint32_t *)(data.get() + current_position)) = len;
	current_position += sizeof(uint32_t);

	data_ptr_t result = data.get() + current_position;
	current_position += len;
	return result;
}

data_ptr_t UndoBuffer::CreateEntry(UndoFlags type, index_t len) {
	assert(len <= std::numeric_limits<uint32_t>::max());
	index_t needed_space = len + UNDO_ENTRY_HEADER_SIZE;
	if (head->current_position + needed_space >= head->maximum_size) {
		auto new_chunk =
		    make_unique<UndoChunk>(needed_space > DEFAULT_UNDO_CHUNK_SIZE ? needed_space : DEFAULT_UNDO_CHUNK_SIZE);
		head->prev = new_chunk.get();
		new_chunk->next = move(head);
		head = move(new_chunk);
	}
	return head->WriteEntry(type, len);
}

template <class T> void UndoBuffer::IterateEntries(T &&callback) {
	// iterate in insertion order: start with the tail
	auto current = tail;
	while (current) {
		data_ptr_t start = current->data.get();
		data_ptr_t end = start + current->current_position;
		while (start < end) {
			UndoFlags type = *((UndoFlags *)start);
			start += sizeof(UndoFlags);
			uint32_t len = *((uint32_t *)start);
			start += sizeof(uint32_t);
			callback(type, start);
			start += len;
		}
		current = current->prev;
	}
}

template <class T> void UndoBuffer::ReverseIterateEntries(T &&callback) {
	// iterate in reverse insertion order: start with the head
	auto current = head.get();
	while (current) {
		data_ptr_t start = current->data.get();
		data_ptr_t end = start + current->current_position;
		// create a vector with all nodes in this chunk
		vector<pair<UndoFlags, data_ptr_t>> nodes;
		while (start < end) {
			UndoFlags type = *((UndoFlags *)start);
			start += sizeof(UndoFlags);
			uint32_t len = *((uint32_t *)start);
			start += sizeof(uint32_t);
			nodes.push_back(make_pair(type, start));
			start += len;
		}
		// iterate over it in reverse order
		for (index_t i = nodes.size(); i > 0; i--) {
			callback(nodes[i - 1].first, nodes[i - 1].second);
		}
		current = current->next.get();
	}
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

	IterateEntries([&](UndoFlags type, data_ptr_t data) {
		switch (type) {
		case UndoFlags::CATALOG_ENTRY: {
			CatalogEntry *catalog_entry = *((CatalogEntry **)data);
			// destroy the backed up entry: it is no longer required
			assert(catalog_entry->parent);
			if (catalog_entry->parent->type != CatalogType::UPDATED_ENTRY) {
				if (!catalog_entry->parent->child->deleted) {
					// delete the entry from the dependency manager, if it is not deleted yet
					catalog_entry->catalog->dependency_manager.EraseObject(catalog_entry->parent->child.get());
				}
				catalog_entry->parent->child = move(catalog_entry->child);
			}
			break;
		}
		case UndoFlags::DELETE_TUPLE:
		case UndoFlags::UPDATE_TUPLE:
		case UndoFlags::INSERT_TUPLE: {
			// undo this entry
			auto info = (VersionInfo *)data;
			if (type == UndoFlags::DELETE_TUPLE || type == UndoFlags::UPDATE_TUPLE) {
				if (info->table->indexes.size() > 0) {
					CleanupIndexInsert(info);
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
			break;
		}
		case UndoFlags::QUERY:
			break;
		default:
			assert(type == UndoFlags::EMPTY_ENTRY);
			break;
		}
	});
}

class CommitState {
public:
	WriteAheadLog *log;
	transaction_t commit_id;
	UndoFlags current_op;

	DataTable *current_table;
	unique_ptr<DataChunk> chunk;
	index_t row_identifiers[STANDARD_VECTOR_SIZE];

public:
	template <bool HAS_LOG> void CommitEntry(UndoFlags type, data_ptr_t data);

	void Flush(UndoFlags new_op);

private:
	void SwitchTable(DataTable *table, UndoFlags new_op);

	void PrepareAppend(UndoFlags op);
	void AppendToChunk(VersionInfo *info);

	void WriteCatalogEntry(CatalogEntry *entry);
	void WriteDelete(VersionInfo *info);
	void WriteUpdate(VersionInfo *info);
	void WriteInsert(VersionInfo *info);
};

void CommitState::Flush(UndoFlags new_op) {
	auto prev_op = current_op;
	current_op = new_op;
	if (prev_op == UndoFlags::EMPTY_ENTRY) {
		// nothing to flush
		return;
	}
	if (!chunk || chunk->size() == 0) {
		return;
	}
	switch (prev_op) {
	case UndoFlags::INSERT_TUPLE:
		log->WriteInsert(*chunk);
		break;
	case UndoFlags::DELETE_TUPLE:
		log->WriteDelete(*chunk);
		break;
	case UndoFlags::UPDATE_TUPLE:
		log->WriteUpdate(*chunk);
		break;
	default:
		break;
	}
}

void CommitState::SwitchTable(DataTable *table, UndoFlags new_op) {
	if (current_table != table) {
		// flush any previous appends/updates/deletes (if any)
		Flush(new_op);

		// write the current table to the log
		log->WriteSetTable(table->schema, table->table);
		current_table = table;
		chunk = nullptr;
	}
}

void CommitState::AppendToChunk(VersionInfo *info) {
	if (info->chunk) {
		// fetch from base table
		auto id = info->prev.entry;
		index_t current_offset = chunk->size();
		for (index_t i = 0; i < current_table->types.size(); i++) {
			index_t value_size = GetTypeIdSize(chunk->data[i].type);
			data_ptr_t storage_pointer = info->chunk->GetPointerToRow(i, info->chunk->start + id);
			memcpy(chunk->data[i].data + value_size * current_offset, storage_pointer, value_size);
			chunk->data[i].count++;
		}
	} else {
		// fetch from tuple data
		assert(info->prev.pointer->tuple_data);
		auto tuple_data = info->prev.pointer->tuple_data;
		index_t current_offset = chunk->size();
		for (index_t i = 0; i < current_table->types.size(); i++) {
			auto type = chunk->data[i].type;
			index_t value_size = GetTypeIdSize(type);
			memcpy(chunk->data[i].data + value_size * current_offset, tuple_data, value_size);
			tuple_data += value_size;
			chunk->data[i].count++;
		}
	}
}

void CommitState::WriteCatalogEntry(CatalogEntry *entry) {
	assert(log);
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

void CommitState::PrepareAppend(UndoFlags op) {
	if (!chunk) {
		chunk = make_unique<DataChunk>();
		switch (op) {
		case UndoFlags::INSERT_TUPLE:
			chunk->Initialize(current_table->types);
			break;
		case UndoFlags::DELETE_TUPLE: {
			vector<TypeId> delete_types = {ROW_TYPE};
			chunk->Initialize(delete_types);
			break;
		}
		default: {
			assert(op == UndoFlags::UPDATE_TUPLE);
			auto update_types = current_table->types;
			update_types.push_back(ROW_TYPE);
			chunk->Initialize(update_types);
			break;
		}
		}
	}
	if (chunk->size() >= STANDARD_VECTOR_SIZE) {
		Flush(op);
		chunk->Reset();
	}
}

void CommitState::WriteDelete(VersionInfo *info) {
	assert(log);
	assert(info->chunk);
	// switch to the current table, if necessary
	SwitchTable(&info->chunk->table, UndoFlags::DELETE_TUPLE);

	// prepare the delete chunk for appending
	PrepareAppend(UndoFlags::DELETE_TUPLE);

	// write the rowid of this tuple
	index_t rowid = info->chunk->start + info->prev.entry;
	auto row_ids = (row_t *)chunk->data[0].data;
	row_ids[chunk->data[0].count++] = rowid;
}

void CommitState::WriteUpdate(VersionInfo *info) {
	assert(log);
	if (!info->chunk) {
		// tuple was deleted or updated after this tuple update; no need to write anything
		return;
	}
	// switch to the current table, if necessary
	SwitchTable(&info->chunk->table, UndoFlags::UPDATE_TUPLE);

	// prepare the update chunk for appending
	PrepareAppend(UndoFlags::UPDATE_TUPLE);

	// append the updated info to the chunk
	AppendToChunk(info);
	// now update the rowid to the final block
	index_t rowid = info->chunk->start + info->prev.entry;
	auto &row_id_vector = chunk->data[chunk->column_count - 1];
	auto row_ids = (row_t *)row_id_vector.data;
	row_ids[row_id_vector.count++] = rowid;
}

void CommitState::WriteInsert(VersionInfo *info) {
	assert(log);
	assert(!info->tuple_data);
	// get the data for the insertion
	VersionChunk *storage = nullptr;
	if (info->chunk) {
		// versioninfo refers to data inside VersionChunk
		// fetch the data from the base rows
		storage = info->chunk;
	} else {
		// insertion was updated or deleted after insertion in the same
		// transaction iterate back to the chunk to find the VersionChunk
		auto prev = info->prev.pointer;
		while (!prev->chunk) {
			assert(info->prev.pointer);
			prev = prev->prev.pointer;
		}
		storage = prev->chunk;
	}

	// switch to the current table, if necessary
	SwitchTable(&storage->table, UndoFlags::INSERT_TUPLE);

	// prepare the insert chunk for appending
	PrepareAppend(UndoFlags::INSERT_TUPLE);

	// now append the tuple to the current chunk
	AppendToChunk(info);
}

template <bool HAS_LOG> void CommitState::CommitEntry(UndoFlags type, data_ptr_t data) {
	if (HAS_LOG && type != current_op) {
		Flush(type);
		chunk = nullptr;
	}
	switch (type) {
	case UndoFlags::CATALOG_ENTRY: {
		// set the commit timestamp of the catalog entry to the given id
		CatalogEntry *catalog_entry = *((CatalogEntry **)data);
		assert(catalog_entry->parent);
		catalog_entry->parent->timestamp = commit_id;

		if (HAS_LOG) {
			// push the catalog update to the WAL
			WriteCatalogEntry(catalog_entry);
		}
		break;
	}
	case UndoFlags::DELETE_TUPLE:
	case UndoFlags::UPDATE_TUPLE:
	case UndoFlags::INSERT_TUPLE: {
		auto info = (VersionInfo *)data;
		// Before we set the commit timestamp we write the entry to the WAL. When we set the commit timestamp it enables
		// other transactions to overwrite the data, but BEFORE we set the commit timestamp the other transactions will
		// get a concurrency conflict error if they attempt ot modify these tuples. Hence BEFORE we set the commit
		// timestamp we can safely access the data in the base table without needing any locks.
		switch (type) {
		case UndoFlags::UPDATE_TUPLE:
			if (HAS_LOG) {
				WriteUpdate(info);
			}
			break;
		case UndoFlags::DELETE_TUPLE:
			info->table->cardinality--;
			if (HAS_LOG) {
				WriteDelete(info);
			}
			break;
		default: // UndoFlags::INSERT_TUPLE
			assert(type == UndoFlags::INSERT_TUPLE);
			info->table->cardinality++;
			if (HAS_LOG) {
				// push the tuple insert to the WAL
				WriteInsert(info);
			}
			break;
		}
		// set the commit timestamp of the entry
		info->version_number = commit_id;
		break;
	}
	case UndoFlags::QUERY: {
		if (HAS_LOG) {
			string query = string((char *)data);
			log->WriteQuery(query);
		}
		break;
	}
	default:
		throw NotImplementedException("UndoBuffer - don't know how to commit this type!");
	}
}

void UndoBuffer::Commit(WriteAheadLog *log, transaction_t commit_id) {
	CommitState state;
	state.log = log;
	state.commit_id = commit_id;
	state.current_table = nullptr;
	state.current_op = UndoFlags::EMPTY_ENTRY;
	if (log) {
		// commit WITH write ahead log
		IterateEntries([&](UndoFlags type, data_ptr_t data) { state.CommitEntry<true>(type, data); });
		// final flush after writing
		state.Flush(UndoFlags::EMPTY_ENTRY);
	} else {
		// comit WITHOUT write ahead log
		IterateEntries([&](UndoFlags type, data_ptr_t data) { state.CommitEntry<false>(type, data); });
	}
}

void UndoBuffer::Rollback() {
	ReverseIterateEntries([&](UndoFlags type, data_ptr_t data) {
		switch (type) {
		case UndoFlags::CATALOG_ENTRY: {
			// undo this catalog entry
			CatalogEntry *catalog_entry = *((CatalogEntry **)data);
			assert(catalog_entry->set);
			catalog_entry->set->Undo(catalog_entry);
			break;
		}
		case UndoFlags::DELETE_TUPLE:
		case UndoFlags::UPDATE_TUPLE:
		case UndoFlags::INSERT_TUPLE: {
			// undo this entry
			auto info = (VersionInfo *)data;
			if (type == UndoFlags::UPDATE_TUPLE || type == UndoFlags::INSERT_TUPLE) {
				// update or insert rolled back
				// delete base table entry from index
				assert(info->chunk);
				if (info->table->indexes.size() > 0) {
					RollbackIndexInsert(info);
				}
			}
			assert(info->chunk);
			// parent needs to refer to a storage chunk because of our transactional model
			// the current entry is still dirty, hence no other transaction can have modified it
			info->chunk->Undo(info);
			break;
		}
		case UndoFlags::QUERY:
			break;
		default:
			assert(type == UndoFlags::EMPTY_ENTRY);
			break;
		}
		type = UndoFlags::EMPTY_ENTRY;
	});
}

static void CleanupIndexInsert(VersionInfo *info) {
	// fetch the row identifiers
	row_t row_number;
	VersionInfo *current_info = info;
	while (!current_info->chunk) {
		assert(current_info->prev.pointer);
		current_info = current_info->prev.pointer;
	}
	row_number = current_info->chunk->start + current_info->prev.entry;

	assert(info->tuple_data);
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

	Value ptr = Value::BIGINT(row_number);
	Vector row_identifiers(ptr);

	info->table->RetrieveVersionedData(result, column_ids, alternate_version_pointers, alternate_version_index, 1);
	for (auto &index : info->table->indexes) {
		index->Delete(result, row_identifiers);
	}
}

static void RollbackIndexInsert(VersionInfo *info) {
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
