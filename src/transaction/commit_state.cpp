#include "transaction/commit_state.hpp"

#include "storage/data_table.hpp"
#include "storage/write_ahead_log.hpp"

using namespace duckdb;
using namespace std;

template <bool HAS_LOG>
CommitState<HAS_LOG>::CommitState(transaction_t commit_id, WriteAheadLog *log)
    : log(log), commit_id(commit_id), current_op(UndoFlags::EMPTY_ENTRY), current_table(nullptr) {
}

template <bool HAS_LOG> void CommitState<HAS_LOG>::Flush(UndoFlags new_op) {
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

template <bool HAS_LOG> void CommitState<HAS_LOG>::SwitchTable(DataTable *table, UndoFlags new_op) {
	if (current_table != table) {
		// flush any previous appends/updates/deletes (if any)
		Flush(new_op);

		// write the current table to the log
		log->WriteSetTable(table->schema, table->table);
		current_table = table;
		chunk = nullptr;
	}
}

template <bool HAS_LOG> void CommitState<HAS_LOG>::WriteCatalogEntry(CatalogEntry *entry) {
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

template <bool HAS_LOG> void CommitState<HAS_LOG>::PrepareAppend(UndoFlags op) {
	if (!chunk) {
		chunk = make_unique<DataChunk>();
		switch (op) {
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

template <bool HAS_LOG> void CommitState<HAS_LOG>::WriteDelete(DeleteInfo *info) {
	assert(log);
	// switch to the current table, if necessary
	SwitchTable(&info->GetTable(), UndoFlags::DELETE_TUPLE);

	for(index_t i = 0; i < info->count; i++) {
		// prepare the delete chunk for appending
		PrepareAppend(UndoFlags::DELETE_TUPLE);

		// append only the row id for a delete
		AppendRowId(info->vinfo->start + info->rows[i]);
	}
}

template <bool HAS_LOG> void CommitState<HAS_LOG>::AppendRowId(row_t rowid) {
	auto &row_id_vector = chunk->data[chunk->column_count - 1];
	auto row_ids = (row_t *)row_id_vector.data;
	row_ids[row_id_vector.count++] = rowid;
}

template <bool HAS_LOG> void CommitState<HAS_LOG>::CommitEntry(UndoFlags type, data_ptr_t data) {
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
	case UndoFlags::DELETE_TUPLE: {
		// deletion:
		auto info = (DeleteInfo *)data;
		info->GetTable().cardinality -= info->count;
		if (HAS_LOG) {
			WriteDelete(info);
		}
		// mark the tuples as committed
		info->vinfo->CommitDelete(commit_id, info->rows, info->count);
		break;
	}
	case UndoFlags::UPDATE_TUPLE: {
		// update:
		auto info = (UpdateInfo *)data;
		if (HAS_LOG) {
			throw Exception("FIXME: write update in log");
		}
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
