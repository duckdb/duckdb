#include "duckdb/transaction/commit_state.hpp"
#include "duckdb/transaction/delete_info.hpp"
#include "duckdb/transaction/update_info.hpp"

#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/write_ahead_log.hpp"
#include "duckdb/storage/uncompressed_segment.hpp"
#include "duckdb/common/serializer/buffered_deserializer.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"

using namespace duckdb;
using namespace std;

CommitState::CommitState(transaction_t commit_id, WriteAheadLog *log)
    : log(log), commit_id(commit_id), current_table_info(nullptr) {
}

void CommitState::SwitchTable(DataTableInfo *table_info, UndoFlags new_op) {
	if (current_table_info != table_info) {
		// write the current table to the log
		log->WriteSetTable(table_info->schema, table_info->table);
		current_table_info = table_info;
	}
}

void CommitState::WriteCatalogEntry(CatalogEntry *entry, data_ptr_t dataptr) {
	assert(log);
	// look at the type of the parent entry
	auto parent = entry->parent;
	switch (parent->type) {
	case CatalogType::TABLE:
		if (parent->temporary) {
			return;
		}
		if (entry->type == CatalogType::TABLE) {
			// ALTER TABLE statement, read the extra data after the entry
			auto extra_data_size = *((idx_t *)dataptr);
			auto extra_data = (data_ptr_t)(dataptr + sizeof(idx_t));
			// deserialize it
			BufferedDeserializer source(extra_data, extra_data_size);
			auto info = AlterInfo::Deserialize(source);
			// write the alter table in the log
			log->WriteAlter(*info);
		} else {
			// CREATE TABLE statement
			log->WriteCreateTable((TableCatalogEntry *)parent);
		}
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

	case CatalogType::INDEX:
	case CatalogType::PREPARED_STATEMENT:
	case CatalogType::AGGREGATE_FUNCTION:
	case CatalogType::SCALAR_FUNCTION:
	case CatalogType::TABLE_FUNCTION:

		// do nothing, we log the query to recreate this
		break;
	default:
		throw NotImplementedException("UndoBuffer - don't know how to write this entry to the WAL");
	}
}

void CommitState::WriteDelete(DeleteInfo *info) {
	assert(log);
	// switch to the current table, if necessary
	SwitchTable(info->table->info.get(), UndoFlags::DELETE_TUPLE);

	if (!delete_chunk) {
		delete_chunk = make_unique<DataChunk>();
		vector<TypeId> delete_types = {ROW_TYPE};
		delete_chunk->Initialize(delete_types);
	}
	auto rows = FlatVector::GetData<row_t>(delete_chunk->data[0]);
	for (idx_t i = 0; i < info->count; i++) {
		rows[i] = info->base_row + info->rows[i];
	}
	delete_chunk->SetCardinality(info->count);
	log->WriteDelete(*delete_chunk);
}

void CommitState::WriteUpdate(UpdateInfo *info) {
	assert(log);
	// switch to the current table, if necessary
	SwitchTable(&info->column_data->table_info, UndoFlags::UPDATE_TUPLE);

	update_chunk = make_unique<DataChunk>();
	vector<TypeId> update_types = {info->column_data->type, ROW_TYPE};
	update_chunk->Initialize(update_types);

	// fetch the updated values from the base table
	ColumnScanState state;
	info->segment->InitializeScan(state);
	info->segment->Fetch(state, info->vector_index, update_chunk->data[0]);

	// write the row ids into the chunk
	auto row_ids = FlatVector::GetData<row_t>(update_chunk->data[1]);
	idx_t start = info->segment->row_start + info->vector_index * STANDARD_VECTOR_SIZE;
	for (idx_t i = 0; i < info->N; i++) {
		row_ids[info->tuples[i]] = start + info->tuples[i];
	}
	SelectionVector sel(info->tuples);
	update_chunk->Slice(sel, info->N);

	log->WriteUpdate(*update_chunk, info->column_data->column_idx);
}

template <bool HAS_LOG> void CommitState::CommitEntry(UndoFlags type, data_ptr_t data) {
	switch (type) {
	case UndoFlags::CATALOG_ENTRY: {
		// set the commit timestamp of the catalog entry to the given id
		CatalogEntry *catalog_entry = *((CatalogEntry **)data);
		assert(catalog_entry->parent);
		catalog_entry->parent->timestamp = commit_id;

		if (HAS_LOG) {
			// push the catalog update to the WAL
			WriteCatalogEntry(catalog_entry, data + sizeof(CatalogEntry *));
		}
		break;
	}
	case UndoFlags::DELETE_TUPLE: {
		// deletion:
		auto info = (DeleteInfo *)data;
		info->table->info->cardinality -= info->count;
		if (HAS_LOG && !info->table->info->IsTemporary()) {
			WriteDelete(info);
		}
		// mark the tuples as committed
		info->vinfo->CommitDelete(commit_id, info->rows, info->count);
		break;
	}
	case UndoFlags::UPDATE_TUPLE: {
		// update:
		auto info = (UpdateInfo *)data;
		if (HAS_LOG && !info->column_data->table_info.IsTemporary()) {
			WriteUpdate(info);
		}
		info->version_number = commit_id;
		break;
	}
	default:
		throw NotImplementedException("UndoBuffer - don't know how to commit this type!");
	}
}

void CommitState::RevertCommit(UndoFlags type, data_ptr_t data) {
	transaction_t transaction_id = commit_id;
	switch (type) {
	case UndoFlags::CATALOG_ENTRY: {
		// set the commit timestamp of the catalog entry to the given id
		CatalogEntry *catalog_entry = *((CatalogEntry **)data);
		assert(catalog_entry->parent);
		catalog_entry->parent->timestamp = transaction_id;
		break;
	}
	case UndoFlags::DELETE_TUPLE: {
		// deletion:
		auto info = (DeleteInfo *)data;
		info->table->info->cardinality += info->count;
		// revert the commit by writing the (uncommitted) transaction_id back into the version info
		info->vinfo->CommitDelete(transaction_id, info->rows, info->count);
		break;
	}
	case UndoFlags::UPDATE_TUPLE: {
		// update:
		auto info = (UpdateInfo *)data;
		info->version_number = transaction_id;
		break;
	}
	default:
		throw NotImplementedException("UndoBuffer - don't know how to revert commit of this type!");
	}
}

template void CommitState::CommitEntry<true>(UndoFlags type, data_ptr_t data);
template void CommitState::CommitEntry<false>(UndoFlags type, data_ptr_t data);
