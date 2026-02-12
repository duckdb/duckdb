#include "duckdb/storage/table/table_index_list.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/types/conflict_manager.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/execution/index/index_type_set.hpp"
#include "duckdb/execution/index/unbound_index.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/planner/expression_binder/index_binder.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/data_table_info.hpp"
#include "duckdb/storage/table/scan_state.hpp"

namespace duckdb {

IndexEntry::IndexEntry(unique_ptr<Index> index_p) : index(std::move(index_p)) {
	if (index->IsBound()) {
		bind_state = IndexBindState::BOUND;
	} else {
		bind_state = IndexBindState::UNBOUND;
	}
}

template <class T>
TableIndexIterationHelper<T>::TableIndexIterationHelper(mutex &index_lock,
                                                        const vector<unique_ptr<IndexEntry>> &index_entries)
    : lock(index_lock), index_entries(index_entries) {
}

template <class T>
TableIndexIterationHelper<T>::TableIndexIterator::TableIndexIterator(
    optional_ptr<const vector<unique_ptr<IndexEntry>>> index_entries_p)
    : index_entries(index_entries_p) {
	if (index_entries) {
		if (index_entries->empty()) {
			index_entries = nullptr;
		} else {
			index = 0;
		}
	}
}

template <class T>
typename TableIndexIterationHelper<T>::TableIndexIterator &
TableIndexIterationHelper<T>::TableIndexIterator::operator++() {
	if (index_entries) {
		auto next_index = index.GetIndex() + 1;
		if (next_index >= index_entries->size()) {
			// reached the end
			index = optional_idx();
			index_entries = nullptr;
		} else {
			// next index
			index = next_index;
		}
	}
	return *this;
}

template <class T>
bool TableIndexIterationHelper<T>::TableIndexIterator::operator!=(const TableIndexIterator &other) const {
	return index != other.index || index_entries != other.index_entries;
}

template <>
IndexEntry &TableIndexIterationHelper<IndexEntry>::TableIndexIterator::operator*() const {
	return *index_entries->at(index.GetIndex());
}

template <>
Index &TableIndexIterationHelper<Index>::TableIndexIterator::operator*() const {
	return *index_entries->at(index.GetIndex())->index;
}

TableIndexIterationHelper<IndexEntry> TableIndexList::IndexEntries() const {
	return TableIndexIterationHelper<IndexEntry>(index_entries_lock, index_entries);
}

TableIndexIterationHelper<Index> TableIndexList::Indexes() const {
	return TableIndexIterationHelper<Index>(index_entries_lock, index_entries);
}

template class TableIndexIterationHelper<IndexEntry>;
template class TableIndexIterationHelper<Index>;

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
			// We bound all indexes. (of this type)
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

optional_ptr<IndexEntry> TableIndexList::FindForeignKeyIndex(const vector<PhysicalIndex> &fk_keys,
                                                             const ForeignKeyType fk_type) {
	lock_guard<mutex> lock(index_entries_lock);
	for (auto &entry : index_entries) {
		auto &index = *entry->index;
		if (IsForeignKeyIndex(fk_keys, index, fk_type)) {
			return entry;
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
	auto entry = FindForeignKeyIndex(fk_keys, fk_type);
	auto &index = *entry->index;
	lock_guard<mutex> guard(entry->lock);
	D_ASSERT(index.IsBound());
	IndexAppendInfo index_append_info;
	if (storage) {
		auto delete_index = storage->delete_indexes.Find(index.GetIndexName());
		if (delete_index) {
			index_append_info.delete_indexes.push_back(*delete_index);
		}
	}
	if (entry->removed_data_during_checkpoint) {
		index_append_info.delete_indexes.push_back(*entry->removed_data_during_checkpoint);
	}

	auto &main_index = index.Cast<BoundIndex>();
	main_index.VerifyConstraint(chunk, index_append_info, conflict_manager);
	if (entry->added_data_during_checkpoint) {
		// if we have added any rows during checkpoint - check in that index as well
		IndexAppendInfo added_during_checkpoint_info;
		entry->added_data_during_checkpoint->VerifyConstraint(chunk, added_during_checkpoint_info, conflict_manager);
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

IndexSerializationResult TableIndexList::SerializeToDisk(QueryContext context, const IndexSerializationInfo &info) {
	lock_guard<mutex> lock(index_entries_lock);

	IndexSerializationResult result;

	idx_t bound_count = 0;
	for (auto &entry : index_entries) {
		if (entry->index->IsBound()) {
			bound_count++;
		}
	}
	result.bound_infos.reserve(bound_count);
	for (auto &entry : index_entries) {
		auto &index = *entry->index;
		if (!index.IsBound()) {
			// Unbound: reference existing storage info
			auto &unbound_index = index.Cast<UnboundIndex>();
			D_ASSERT(!unbound_index.GetStorageInfo().name.empty());
			result.ordered_infos.push_back(unbound_index.GetStorageInfo());
			continue;
		}
		// Bound: move new storage info into bound_infos, then reference it
		auto &bound_index = index.Cast<BoundIndex>();
		auto storage_info = bound_index.SerializeToDisk(context, info.options);
		D_ASSERT(storage_info.IsValid() && !storage_info.name.empty());
		result.bound_infos.push_back(std::move(storage_info));
		result.ordered_infos.push_back(result.bound_infos.back());
	}

	return result;
}

void TableIndexList::MergeCheckpointDeltas(transaction_t checkpoint_id) {
	lock_guard<mutex> lock(index_entries_lock);
	for (auto &entry : index_entries) {
		// Merge any data appended to the index while the checkpoint was running.
		auto &index = *entry->index;
		if (!index.IsBound()) {
			continue;
		}
		lock_guard<mutex> guard(entry->lock);
		auto &bound_index = index.Cast<BoundIndex>();
		auto &art = bound_index.Cast<ART>();

		if (entry->removed_data_during_checkpoint) {
			art.RemovalMerge(*entry->removed_data_during_checkpoint);
		}
		if (entry->added_data_during_checkpoint) {
			// NOTE: we insert duplicates here (IndexAppendMode::INSERT_DUPLICATES)
			// this is necessary due to the way that data is inserted into indexes during transaction commit
			// essentially we always FIRST insert data into the index, THEN remove data
			// even if the data was logically removed first
			// i.e. if we have a transaction like: DELETE FROM tbl WHERE i=42; INSERT INTO tbl VALUES (42);
			// we will FIRST insert 42, THEN delete 42 from the index
			// We plan to change this in the future - see https://github.com/duckdblabs/duckdb-internal/issues/6886
			auto error = art.InsertMerge(*entry->added_data_during_checkpoint, IndexAppendMode::INSERT_DUPLICATES);
			if (error.HasError()) {
				throw InternalException("Failed to append while merging checkpoint deltas - this "
				                        "signifies a bug or broken index: %s",
				                        error.Message());
			}
		}
		entry->removed_data_during_checkpoint.reset();
		entry->added_data_during_checkpoint.reset();
		entry->last_written_checkpoint = checkpoint_id;
	}
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
