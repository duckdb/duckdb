#include "duckdb/transaction/commit_state.hpp"

#include "duckdb/catalog/catalog_entry/duck_index_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/dependency/dependency_entry.hpp"
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
#include "duckdb/transaction/duck_transaction.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// IndexDataRemover
//===--------------------------------------------------------------------===//
IndexDataRemover::IndexDataRemover(QueryContext context, IndexRemovalType removal_type)
    : context(context), removal_type(removal_type) {
}

void IndexDataRemover::PushDelete(DeleteInfo &info) {
	auto &version_table = *info.table;
	if (!version_table.HasIndexes()) {
		// this table has no indexes: no cleanup to be done
		return;
	}

	idx_t count = 0;
	row_t row_numbers[STANDARD_VECTOR_SIZE];
	if (info.is_consecutive) {
		for (idx_t i = 0; i < info.count; i++) {
			row_numbers[count++] = UnsafeNumericCast<int64_t>(info.base_row + i);
		}
	} else {
		auto rows = info.GetRows();
		for (idx_t i = 0; i < info.count; i++) {
			row_numbers[count++] = UnsafeNumericCast<int64_t>(info.base_row + rows[i]);
		}
	}
	Flush(version_table, row_numbers, count);
}

void IndexDataRemover::Flush(DataTable &table, row_t *row_numbers, idx_t count) {
	if (count == 0) {
		return;
	}

	// set up the row identifiers vector
	Vector row_identifiers(LogicalType::ROW_TYPE, data_ptr_cast(row_numbers));

	// delete the tuples from all the indexes.
	// If there is any issue with removal, a FatalException must be thrown since there may be a corruption of
	// data, hence the transaction cannot be guaranteed.
	try {
		table.RemoveFromIndexes(context, row_identifiers, count, removal_type);
	} catch (std::exception &ex) {
		throw FatalException(ErrorData(ex).Message());
	} catch (...) {
		throw FatalException("unknown failure in CommitState::Flush");
	}

	count = 0;
}

//===--------------------------------------------------------------------===//
// CommitState
//===--------------------------------------------------------------------===//
CommitState::CommitState(DuckTransaction &transaction_p, transaction_t commit_id,
                         ActiveTransactionState transaction_state, CommitMode commit_mode)
    : transaction(transaction_p), commit_id(commit_id),
      index_data_remover(*transaction.context.lock(), GetIndexRemovalType(transaction_state, commit_mode)) {
}

IndexRemovalType CommitState::GetIndexRemovalType(ActiveTransactionState transaction_state, CommitMode commit_mode) {
	if (commit_mode == CommitMode::PERFORM_COMMIT) {
		if (transaction_state == ActiveTransactionState::NO_OTHER_TRANSACTIONS) {
			// if there are no other active transactions we don't need to store removed rows in deleted_rows_in_use
			return IndexRemovalType::MAIN_INDEX_ONLY;
		}
		return IndexRemovalType::MAIN_INDEX;
	}
	// revert the appends to the indexes
	if (transaction_state == ActiveTransactionState::NO_OTHER_TRANSACTIONS) {
		return IndexRemovalType::REVERT_MAIN_INDEX_ONLY_APPEND;
	}
	return IndexRemovalType::REVERT_MAIN_INDEX_APPEND;
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
		auto &old_entry = *Load<CatalogEntry *>(data);
		D_ASSERT(old_entry.HasParent());

		auto &catalog = old_entry.ParentCatalog();
		D_ASSERT(catalog.IsDuckCatalog());

		auto &new_entry = old_entry.Parent();
		if (new_entry.type == CatalogType::DEPENDENCY_ENTRY) {
			auto &dep = new_entry.Cast<DependencyEntry>();
			if (dep.Side() == DependencyEntryType::SUBJECT) {
				new_entry.set->VerifyExistenceOfDependency(commit_id, new_entry);
			}
		} else if (new_entry.type == CatalogType::DELETED_ENTRY && old_entry.set) {
			old_entry.set->CommitDrop(commit_id, transaction.start_time, old_entry);
		}
		// Grab a write lock on the catalog
		auto &duck_catalog = catalog.Cast<DuckCatalog>();
		lock_guard<mutex> write_lock(duck_catalog.GetWriteLock());
		lock_guard<mutex> read_lock(old_entry.set->GetCatalogLock());
		// Set the timestamp of the catalog entry to the given commit_id, marking it as committed
		CatalogSet::UpdateTimestamp(old_entry.Parent(), commit_id);

		// drop any blocks associated with the catalog entry if possible (e.g. in case of a DROP or ALTER)
		CommitEntryDrop(old_entry, data + sizeof(CatalogEntry *));
		break;
	}
	case UndoFlags::INSERT_TUPLE: {
		// append:
		auto info = reinterpret_cast<AppendInfo *>(data);
		if (!info->table->IsMainTable()) {
			auto table_name = info->table->GetTableName();
			auto table_modification = info->table->TableModification();
			throw TransactionException("Attempting to modify table %s but another transaction has %s this table",
			                           table_name, table_modification);
		}
		// mark the tuples as committed
		info->table->CommitAppend(commit_id, info->start_row, info->count);
		break;
	}
	case UndoFlags::DELETE_TUPLE: {
		// deletion:
		auto info = reinterpret_cast<DeleteInfo *>(data);
		if (!info->table->IsMainTable()) {
			auto table_name = info->table->GetTableName();
			auto table_modification = info->table->TableModification();
			throw TransactionException("Attempting to modify table %s but another transaction has %s this table",
			                           table_name, table_modification);
		}
		CommitDelete(*info);
		break;
	}
	case UndoFlags::UPDATE_TUPLE: {
		// update:
		auto info = reinterpret_cast<UpdateInfo *>(data);
		if (!info->table->IsMainTable()) {
			auto table_name = info->table->GetTableName();
			auto table_modification = info->table->TableModification();
			throw TransactionException("Attempting to modify table %s but another transaction has %s this table",
			                           table_name, table_modification);
		}
		info->version_number = commit_id;
		break;
	}
	case UndoFlags::ATTACHED_DATABASE:
	case UndoFlags::SEQUENCE_VALUE: {
		break;
	}
	default:
		throw InternalException("UndoBuffer - don't know how to commit this type!");
	}
}

void CommitState::CommitDelete(DeleteInfo &info) {
	// mark the tuples as committed
	info.version_info->CommitDelete(info.vector_idx, commit_id, info);
	// delete from indexes
	index_data_remover.PushDelete(info);
}

void CommitState::RevertCommit(UndoFlags type, data_ptr_t data) {
	transaction_t transaction_id = commit_id;
	switch (type) {
	case UndoFlags::CATALOG_ENTRY: {
		// set the commit timestamp of the catalog entry to the given id
		auto catalog_entry = Load<CatalogEntry *>(data);
		D_ASSERT(catalog_entry->HasParent());
		CatalogSet::UpdateTimestamp(catalog_entry->Parent(), transaction_id);
		if (catalog_entry->name != catalog_entry->Parent().name) {
			CatalogSet::UpdateTimestamp(*catalog_entry, transaction_id);
		}
		break;
	}
	case UndoFlags::INSERT_TUPLE: {
		auto info = reinterpret_cast<AppendInfo *>(data);
		// revert this append
		info->table->RevertAppend(transaction, info->start_row, info->count);
		break;
	}
	case UndoFlags::DELETE_TUPLE: {
		// deletion:
		auto info = reinterpret_cast<DeleteInfo *>(data);
		// revert the commit by writing the (uncommitted) transaction_id back into the version info
		CommitDelete(*info);
		break;
	}
	case UndoFlags::UPDATE_TUPLE: {
		// update:
		auto info = reinterpret_cast<UpdateInfo *>(data);
		info->version_number = transaction_id;
		break;
	}
	case UndoFlags::ATTACHED_DATABASE:
	case UndoFlags::SEQUENCE_VALUE: {
		break;
	}
	default:
		throw InternalException("UndoBuffer - don't know how to revert commit of this type!");
	}
}

} // namespace duckdb
