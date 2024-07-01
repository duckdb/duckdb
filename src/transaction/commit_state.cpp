#include "duckdb/transaction/commit_state.hpp"

#include "duckdb/catalog/catalog_entry/duck_index_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/storage/table/chunk_info.hpp"
#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/storage/table/row_version_manager.hpp"
#include "duckdb/storage/table/update_segment.hpp"
#include "duckdb/storage/write_ahead_log.hpp"
#include "duckdb/transaction/append_info.hpp"
#include "duckdb/transaction/delete_info.hpp"
#include "duckdb/transaction/update_info.hpp"

namespace duckdb {

CommitState::CommitState(transaction_t commit_id) : commit_id(commit_id) {
}

void CommitState::CommitEntryDrop(CatalogEntry &entry, data_ptr_t dataptr) {
	if (entry.temporary || entry.Parent().temporary) {
		return;
	}

	// look at the type of the parent entry
	auto &parent = entry.Parent();

	switch (parent.type) {
	case CatalogType::TABLE_ENTRY:
	case CatalogType::VIEW_ENTRY:
	case CatalogType::INDEX_ENTRY:
	case CatalogType::SEQUENCE_ENTRY:
	case CatalogType::TYPE_ENTRY:
	case CatalogType::MACRO_ENTRY:
	case CatalogType::TABLE_MACRO_ENTRY:
		if (entry.type == CatalogType::RENAMED_ENTRY || entry.type == parent.type) {
			// ALTER statement, read the extra data after the entry
			auto extra_data_size = Load<idx_t>(dataptr);
			auto extra_data = data_ptr_cast(dataptr + sizeof(idx_t));

			MemoryStream source(extra_data, extra_data_size);
			BinaryDeserializer deserializer(source);
			deserializer.Begin();
			auto column_name = deserializer.ReadProperty<string>(100, "column_name");
			auto parse_info = deserializer.ReadProperty<unique_ptr<ParseInfo>>(101, "alter_info");
			deserializer.End();

			switch (parent.type) {
			case CatalogType::TABLE_ENTRY:
				if (!column_name.empty()) {
					D_ASSERT(entry.type != CatalogType::RENAMED_ENTRY);
					auto &table_entry = entry.Cast<DuckTableEntry>();
					D_ASSERT(table_entry.IsDuckTable());
					// write the alter table in the log
					table_entry.CommitAlter(column_name);
				}
				break;
			case CatalogType::VIEW_ENTRY:
			case CatalogType::INDEX_ENTRY:
			case CatalogType::SEQUENCE_ENTRY:
			case CatalogType::TYPE_ENTRY:
			case CatalogType::MACRO_ENTRY:
			case CatalogType::TABLE_MACRO_ENTRY:
				(void)column_name;
				break;
			default:
				throw InternalException("Don't know how to alter this type!");
			}
		} else {
			switch (parent.type) {
			case CatalogType::TABLE_ENTRY:
			case CatalogType::VIEW_ENTRY:
			case CatalogType::INDEX_ENTRY:
			case CatalogType::SEQUENCE_ENTRY:
			case CatalogType::TYPE_ENTRY:
			case CatalogType::MACRO_ENTRY:
			case CatalogType::TABLE_MACRO_ENTRY:
				break;
			default:
				throw InternalException("Don't know how to create this type!");
			}
		}
		break;
	case CatalogType::SCHEMA_ENTRY:
		break;
	case CatalogType::RENAMED_ENTRY:
		// This is a rename, nothing needs to be done for this
		break;
	case CatalogType::DELETED_ENTRY:
		switch (entry.type) {
		case CatalogType::TABLE_ENTRY: {
			auto &table_entry = entry.Cast<DuckTableEntry>();
			D_ASSERT(table_entry.IsDuckTable());

			// If the table was renamed, we do not need to drop the DataTable.
			table_entry.CommitDrop();
			break;
		}
		case CatalogType::INDEX_ENTRY: {
			auto &index_entry = entry.Cast<DuckIndexEntry>();
			index_entry.CommitDrop();
			break;
		}
		default:
			// no action required
			break;
		}
		break;
	case CatalogType::DATABASE_ENTRY:
	case CatalogType::PREPARED_STATEMENT:
	case CatalogType::AGGREGATE_FUNCTION_ENTRY:
	case CatalogType::SCALAR_FUNCTION_ENTRY:
	case CatalogType::TABLE_FUNCTION_ENTRY:
	case CatalogType::COPY_FUNCTION_ENTRY:
	case CatalogType::PRAGMA_FUNCTION_ENTRY:
	case CatalogType::COLLATION_ENTRY:
	case CatalogType::DEPENDENCY_ENTRY:
	case CatalogType::SECRET_ENTRY:
	case CatalogType::SECRET_TYPE_ENTRY:
	case CatalogType::SECRET_FUNCTION_ENTRY:
		// do nothing, these entries are not persisted to disk
		break;
	default:
		throw InternalException("UndoBuffer - don't know how to write this entry to the WAL");
	}
}

void CommitState::CommitEntry(UndoFlags type, data_ptr_t data) {
	switch (type) {
	case UndoFlags::CATALOG_ENTRY: {
		// set the commit timestamp of the catalog entry to the given id
		auto catalog_entry = Load<CatalogEntry *>(data);
		D_ASSERT(catalog_entry->HasParent());

		auto &catalog = catalog_entry->ParentCatalog();
		D_ASSERT(catalog.IsDuckCatalog());

		// Grab a write lock on the catalog
		auto &duck_catalog = catalog.Cast<DuckCatalog>();
		lock_guard<mutex> write_lock(duck_catalog.GetWriteLock());
		lock_guard<mutex> read_lock(catalog_entry->set->GetCatalogLock());
		catalog_entry->set->UpdateTimestamp(catalog_entry->Parent(), commit_id);
		if (!StringUtil::CIEquals(catalog_entry->name, catalog_entry->Parent().name)) {
			catalog_entry->set->UpdateTimestamp(*catalog_entry, commit_id);
		}
		// modify catalog on commit
		duck_catalog.ModifyCatalog();

		// drop any blocks associated with the catalog entry if possible (e.g. in case of a DROP or ALTER)
		CommitEntryDrop(*catalog_entry, data + sizeof(CatalogEntry *));
		break;
	}
	case UndoFlags::INSERT_TUPLE: {
		// append:
		auto info = reinterpret_cast<AppendInfo *>(data);
		// mark the tuples as committed
		info->table->CommitAppend(commit_id, info->start_row, info->count);
		break;
	}
	case UndoFlags::DELETE_TUPLE: {
		// deletion:
		auto info = reinterpret_cast<DeleteInfo *>(data);
		// mark the tuples as committed
		info->version_info->CommitDelete(info->vector_idx, commit_id, *info);
		break;
	}
	case UndoFlags::UPDATE_TUPLE: {
		// update:
		auto info = reinterpret_cast<UpdateInfo *>(data);
		info->version_number = commit_id;
		break;
	}
	case UndoFlags::SEQUENCE_VALUE: {
		break;
	}
	default:
		throw InternalException("UndoBuffer - don't know how to commit this type!");
	}
}

void CommitState::RevertCommit(UndoFlags type, data_ptr_t data) {
	transaction_t transaction_id = commit_id;
	switch (type) {
	case UndoFlags::CATALOG_ENTRY: {
		// set the commit timestamp of the catalog entry to the given id
		auto catalog_entry = Load<CatalogEntry *>(data);
		D_ASSERT(catalog_entry->HasParent());
		catalog_entry->set->UpdateTimestamp(catalog_entry->Parent(), transaction_id);
		if (catalog_entry->name != catalog_entry->Parent().name) {
			catalog_entry->set->UpdateTimestamp(*catalog_entry, transaction_id);
		}
		break;
	}
	case UndoFlags::INSERT_TUPLE: {
		auto info = reinterpret_cast<AppendInfo *>(data);
		// revert this append
		info->table->RevertAppend(info->start_row, info->count);
		break;
	}
	case UndoFlags::DELETE_TUPLE: {
		// deletion:
		auto info = reinterpret_cast<DeleteInfo *>(data);
		// revert the commit by writing the (uncommitted) transaction_id back into the version info
		info->version_info->CommitDelete(info->vector_idx, transaction_id, *info);
		break;
	}
	case UndoFlags::UPDATE_TUPLE: {
		// update:
		auto info = reinterpret_cast<UpdateInfo *>(data);
		info->version_number = transaction_id;
		break;
	}
	case UndoFlags::SEQUENCE_VALUE: {
		break;
	}
	default:
		throw InternalException("UndoBuffer - don't know how to revert commit of this type!");
	}
}

} // namespace duckdb
