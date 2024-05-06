#include "duckdb/storage/table/table_index_list.hpp"

#include "duckdb/common/types/conflict_manager.hpp"
#include "duckdb/execution/index/index_type_set.hpp"
#include "duckdb/execution/index/unbound_index.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/data_table_info.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/planner/expression_binder/index_binder.hpp"

namespace duckdb {
void TableIndexList::AddIndex(unique_ptr<Index> index) {
	D_ASSERT(index);
	lock_guard<mutex> lock(indexes_lock);
	indexes.push_back(std::move(index));
}

void TableIndexList::RemoveIndex(const string &name) {
	lock_guard<mutex> lock(indexes_lock);

	for (idx_t index_idx = 0; index_idx < indexes.size(); index_idx++) {
		auto &index_entry = indexes[index_idx];

		if (index_entry->GetIndexName() == name) {
			indexes.erase_at(index_idx);
			break;
		}
	}
}

void TableIndexList::CommitDrop(const string &name) {
	lock_guard<mutex> lock(indexes_lock);

	for (idx_t index_idx = 0; index_idx < indexes.size(); index_idx++) {
		auto &index_entry = indexes[index_idx];
		if (index_entry->GetIndexName() == name) {
			index_entry->CommitDrop();
		}
	}
}

bool TableIndexList::NameIsUnique(const string &name) {
	lock_guard<mutex> lock(indexes_lock);

	// only cover PK, FK, and UNIQUE, which are not (yet) catalog entries
	for (idx_t index_idx = 0; index_idx < indexes.size(); index_idx++) {
		auto &index_entry = indexes[index_idx];
		if (index_entry->IsPrimary() || index_entry->IsForeign() || index_entry->IsUnique()) {
			if (index_entry->GetIndexName() == name) {
				return false;
			}
		}
	}

	return true;
}

void TableIndexList::InitializeIndexes(ClientContext &context, DataTableInfo &table_info, const char *index_type) {
	// Fast path: do we have any unbound indexes?
	bool needs_binding = false;
	{
		lock_guard<mutex> lock(indexes_lock);
		for (auto &index : indexes) {
			if (!index->IsBound() && (index_type == nullptr || index->GetIndexType() == index_type)) {
				needs_binding = true;
				break;
			}
		}
	}
	if (!needs_binding) {
		return;
	}

	// Get the table from the catalog so we can add it to the binder
	auto &catalog = table_info.GetDB().GetCatalog();
	auto &table =
	    catalog.GetEntry(context, CatalogType::TABLE_ENTRY, table_info.GetSchemaName(), table_info.GetTableName())
	        .Cast<DuckTableEntry>();
	vector<LogicalType> column_types;
	vector<string> column_names;
	for (auto &col : table.GetColumns().Logical()) {
		column_types.push_back(col.Type());
		column_names.push_back(col.Name());
	}

	lock_guard<mutex> lock(indexes_lock);
	for (auto &index : indexes) {
		if (!index->IsBound() && (index_type == nullptr || index->GetIndexType() == index_type)) {
			// Create a binder to bind this index (we cant reuse this binder for other indexes)
			auto binder = Binder::CreateBinder(context);

			// Add the table to the binder
			// We're not interested in the column_ids here, so just pass a dummy vector
			vector<column_t> dummy_column_ids;
			binder->bind_context.AddBaseTable(0, table_info.GetTableName(), column_names, column_types,
			                                  dummy_column_ids, &table);

			// Create an IndexBinder to bind the index
			IndexBinder idx_binder(*binder, context);

			// Replace the unbound index with a bound index
			auto bound_idx = idx_binder.BindIndex(index->Cast<UnboundIndex>());
			index = std::move(bound_idx);
		}
	}
}

bool TableIndexList::Empty() {
	lock_guard<mutex> lock(indexes_lock);
	return indexes.empty();
}

idx_t TableIndexList::Count() {
	lock_guard<mutex> lock(indexes_lock);
	return indexes.size();
}

void TableIndexList::Move(TableIndexList &other) {
	D_ASSERT(indexes.empty());
	indexes = std::move(other.indexes);
}

Index *TableIndexList::FindForeignKeyIndex(const vector<PhysicalIndex> &fk_keys, ForeignKeyType fk_type) {
	Index *result = nullptr;
	Scan([&](Index &index) {
		if (DataTable::IsForeignKeyIndex(fk_keys, index, fk_type)) {
			result = &index;
		}
		return false;
	});
	return result;
}

void TableIndexList::VerifyForeignKey(const vector<PhysicalIndex> &fk_keys, DataChunk &chunk,
                                      ConflictManager &conflict_manager) {
	auto fk_type = conflict_manager.LookupType() == VerifyExistenceType::APPEND_FK
	                   ? ForeignKeyType::FK_TYPE_PRIMARY_KEY_TABLE
	                   : ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE;

	// check whether the chunk can be inserted or deleted into the referenced table storage
	auto index = FindForeignKeyIndex(fk_keys, fk_type);
	if (!index) {
		throw InternalException("Internal Foreign Key error: could not find index to verify...");
	}
	if (!index->IsBound()) {
		throw InternalException("Internal Foreign Key error: trying to verify an unbound index...");
	}
	conflict_manager.SetIndexCount(1);
	index->Cast<BoundIndex>().CheckConstraintsForChunk(chunk, conflict_manager);
}

vector<column_t> TableIndexList::GetRequiredColumns() {
	lock_guard<mutex> lock(indexes_lock);
	set<column_t> unique_indexes;
	for (auto &index : indexes) {
		for (auto col_index : index->GetColumnIds()) {
			unique_indexes.insert(col_index);
		}
	}
	vector<column_t> result;
	result.reserve(unique_indexes.size());
	for (auto column_index : unique_indexes) {
		result.emplace_back(column_index);
	}
	return result;
}

vector<IndexStorageInfo> TableIndexList::GetStorageInfos() {

	vector<IndexStorageInfo> index_storage_infos;
	for (auto &index : indexes) {
		if (index->IsBound()) {
			auto index_storage_info = index->Cast<BoundIndex>().GetStorageInfo(false);
			D_ASSERT(index_storage_info.IsValid() && !index_storage_info.name.empty());
			index_storage_infos.push_back(index_storage_info);
		} else {
			// TODO: Will/should this ever happen?
			auto index_storage_info = index->Cast<UnboundIndex>().GetStorageInfo();
			D_ASSERT(index_storage_info.IsValid() && !index_storage_info.name.empty());
			index_storage_infos.push_back(index_storage_info);
		}
	}
	return index_storage_infos;
}

} // namespace duckdb
