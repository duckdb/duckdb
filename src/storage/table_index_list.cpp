#include "duckdb/storage/table/table_index_list.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/types/conflict_manager.hpp"
#include "duckdb/execution/index/index_type_set.hpp"
#include "duckdb/execution/index/unbound_index.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/planner/expression_binder/index_binder.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/data_table_info.hpp"

namespace duckdb {

IndexEntry::IndexEntry(unique_ptr<Index> index_p) : index(std::move(index_p)) {
	if (index->IsBound()) {
		bind_state = IndexBindState::BOUND;
	} else {
		bind_state = IndexBindState::UNBOUND;
	}
}

void TableIndexList::AddIndex(unique_ptr<Index> index) {
	D_ASSERT(index);
	lock_guard<mutex> lock(index_entries_lock);
	auto index_entry = make_uniq<IndexEntry>(std::move(index));
	index_entries.push_back(std::move(index_entry));
	if (!index_entries.back()->index->IsBound()) {
		unbound_count++;
	}
}

void TableIndexList::RemoveIndex(const string &name) {
	lock_guard<mutex> lock(index_entries_lock);
	for (idx_t i = 0; i < index_entries.size(); i++) {
		auto &index = *index_entries[i]->index;
		if (index.GetIndexName() == name) {
			if (!index.IsBound()) {
				unbound_count--;
			}
			index_entries.erase_at(i);
			return;
		}
	}
}

void TableIndexList::CommitDrop(const string &name) {
	lock_guard<mutex> lock(index_entries_lock);
	for (auto &entry : index_entries) {
		auto &index = *entry->index;
		if (index.GetIndexName() == name) {
			index.CommitDrop();
			return;
		}
	}
}

bool TableIndexList::NameIsUnique(const string &name) {
	// Only covers PK, FK, and UNIQUE indexes.
	lock_guard<mutex> lock(index_entries_lock);
	for (auto &entry : index_entries) {
		auto &index = *entry->index;
		if (index.IsPrimary() || index.IsForeign() || index.IsUnique()) {
			if (index.GetIndexName() == name) {
				return false;
			}
		}
	}
	return true;
}

optional_ptr<BoundIndex> TableIndexList::Find(const string &name) {
	for (auto &entry : index_entries) {
		auto &index = *entry->index;
		if (index.GetIndexName() == name) {
			if (!index.IsBound()) {
				throw InternalException("cannot return an unbound index in TableIndexList::Find");
			}
			return index.Cast<BoundIndex>();
		}
	}
	return nullptr;
}

void TableIndexList::Bind(ClientContext &context, DataTableInfo &table_info, const char *index_type) {
	{
		// Early-out, if we have no unbound indexes.
		lock_guard<mutex> lock(index_entries_lock);
		if (unbound_count == 0) {
			return;
		}
	}

	// Get the table from the catalog, so we can add it to the binder.
	auto &catalog = table_info.GetDB().GetCatalog();
	auto schema = table_info.GetSchemaName();
	auto table_name = table_info.GetTableName();
	auto &table_entry = catalog.GetEntry<TableCatalogEntry>(context, schema, table_name);
	auto &table = table_entry.Cast<DuckTableEntry>();

	vector<LogicalType> column_types;
	vector<string> column_names;
	for (auto &col : table.GetColumns().Logical()) {
		column_types.push_back(col.Type());
		column_names.push_back(col.Name());
	}

	unique_lock<mutex> lock(index_entries_lock);
	// Busy-spin trying to bind all indexes.
	while (true) {
		optional_ptr<IndexEntry> index_entry;
		for (auto &entry : index_entries) {
			auto &index = *entry->index;
			if (!index.IsBound() && (index_type == nullptr || index.GetIndexType() == index_type)) {
				index_entry = entry.get();
				break;
			}
		}
		if (!index_entry) {
			// We bound all indexes.
			D_ASSERT(unbound_count == 0);
			break;
		}
		if (index_entry->bind_state == IndexBindState::BINDING) {
			// Another thread is binding the index.
			// Lock and unlock the index entries so that the other thread can commit its changes.
			lock.unlock();
			lock.lock();
			continue;

		} else if (index_entry->bind_state == IndexBindState::UNBOUND) {
			// We are the thread that'll bind the index.
			index_entry->bind_state = IndexBindState::BINDING;
			lock.unlock();

		} else {
			throw InternalException("index entry bind state cannot be BOUND here");
		}

		// Create a binder to bind this index.
		auto binder = Binder::CreateBinder(context);

		// Add the table to the binder.
		vector<ColumnIndex> dummy_column_ids;
		binder->bind_context.AddBaseTable(0, string(), column_names, column_types, dummy_column_ids, table);

		// Create an IndexBinder to bind the index
		IndexBinder idx_binder(*binder, context);

		// Apply any outstanding buffered replays and replace the unbound index with a bound index.
		auto &unbound_index = index_entry->index->Cast<UnboundIndex>();
		auto bound_idx = idx_binder.BindIndex(unbound_index);
		if (unbound_index.HasBufferedReplays()) {
			// For replaying buffered index operations, we only want the physical column types (skip over
			// generated column types).
			vector<LogicalType> physical_column_types;
			for (auto &col : table.GetColumns().Physical()) {
				physical_column_types.push_back(col.Type());
			}
			bound_idx->ApplyBufferedReplays(physical_column_types, unbound_index.GetBufferedReplays(),
			                                unbound_index.GetMappedColumnIds());
		}

		// Commit the bound index to the index entry.
		lock.lock();
		index_entry->bind_state = IndexBindState::BOUND;
		index_entry->index = std::move(bound_idx);
		unbound_count--;
	}
}

bool IsForeignKeyIndex(const vector<PhysicalIndex> &fk_keys, Index &index, ForeignKeyType fk_type) {
	if (fk_type == ForeignKeyType::FK_TYPE_PRIMARY_KEY_TABLE ? !index.IsUnique() : !index.IsForeign()) {
		return false;
	}
	if (fk_keys.size() != index.GetColumnIds().size()) {
		return false;
	}

	auto &column_ids = index.GetColumnIds();
	for (auto &fk_key : fk_keys) {
		bool found = false;
		for (auto &index_key : column_ids) {
			if (fk_key.index == index_key) {
				found = true;
				break;
			}
		}
		if (!found) {
			return false;
		}
	}
	return true;
}

optional_ptr<Index> TableIndexList::FindForeignKeyIndex(const vector<PhysicalIndex> &fk_keys,
                                                        const ForeignKeyType fk_type) {
	for (auto &entry : index_entries) {
		auto &index = *entry->index;
		if (IsForeignKeyIndex(fk_keys, index, fk_type)) {
			return index;
		}
	}
	return nullptr;
}

void TableIndexList::VerifyForeignKey(optional_ptr<LocalTableStorage> storage, const vector<PhysicalIndex> &fk_keys,
                                      DataChunk &chunk, ConflictManager &conflict_manager) {
	auto fk_type = conflict_manager.GetVerifyExistenceType() == VerifyExistenceType::APPEND_FK
	                   ? ForeignKeyType::FK_TYPE_PRIMARY_KEY_TABLE
	                   : ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE;

	// Check whether the chunk can be inserted in or deleted from the referenced table storage.
	auto index = FindForeignKeyIndex(fk_keys, fk_type);
	D_ASSERT(index && index->IsBound());
	if (storage) {
		auto delete_index = storage->delete_indexes.Find(index->GetIndexName());
		IndexAppendInfo index_append_info(IndexAppendMode::DEFAULT, delete_index);
		index->Cast<BoundIndex>().VerifyConstraint(chunk, index_append_info, conflict_manager);
	} else {
		IndexAppendInfo index_append_info;
		index->Cast<BoundIndex>().VerifyConstraint(chunk, index_append_info, conflict_manager);
	}
}

unordered_set<column_t> TableIndexList::GetRequiredColumns() {
	lock_guard<mutex> lock(index_entries_lock);
	unordered_set<column_t> column_ids;
	for (auto &entry : index_entries) {
		auto &index = *entry->index;
		for (auto col_id : index.GetColumnIds()) {
			column_ids.insert(col_id);
		}
	}
	return column_ids;
}

vector<IndexStorageInfo> TableIndexList::SerializeToDisk(QueryContext context,
                                                         const case_insensitive_map_t<Value> &options) {
	vector<IndexStorageInfo> infos;
	for (auto &entry : index_entries) {
		auto &index = *entry->index;
		if (index.IsBound()) {
			auto info = index.Cast<BoundIndex>().SerializeToDisk(context, options);
			D_ASSERT(info.IsValid() && !info.name.empty());
			infos.push_back(info);
			continue;
		}

		auto info = index.Cast<UnboundIndex>().GetStorageInfo();
		D_ASSERT(!info.name.empty());
		infos.push_back(info);
	}
	return infos;
}

void TableIndexList::InitializeIndexChunk(DataChunk &index_chunk, const vector<LogicalType> &table_types,
                                          vector<StorageIndex> &mapped_column_ids, DataTableInfo &data_table_info) {
	// table_chunk contains all table columns.
	// We only reference the index columns in the index chunk.
	auto &index_list = data_table_info.GetIndexes();
	auto indexed_columns = index_list.GetRequiredColumns();

	// Store the mapped_column_ids and index_types in sorted canonical form, needed for
	// buffering WAL index operations during replay (see notes in unbound_index.hpp).
	// First sort mapped_column_ids, then populate index_types according to the sorted order.
	for (auto &col : indexed_columns) {
		mapped_column_ids.emplace_back(col);
	}
	std::sort(mapped_column_ids.begin(), mapped_column_ids.end());

	vector<LogicalType> index_types;
	for (auto &col : mapped_column_ids) {
		index_types.push_back(table_types[col.GetPrimaryIndex()]);
	}

	index_chunk.InitializeEmpty(index_types);
}

void TableIndexList::ReferenceIndexChunk(DataChunk &table_chunk, DataChunk &index_chunk,
                                         vector<StorageIndex> &mapped_column_ids) {
	for (idx_t i = 0; i < mapped_column_ids.size(); i++) {
		auto col_id = mapped_column_ids[i].GetPrimaryIndex();
		index_chunk.data[i].Reference(table_chunk.data[col_id]);
	}
	index_chunk.SetCardinality(table_chunk);
}

} // namespace duckdb
