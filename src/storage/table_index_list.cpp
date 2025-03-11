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

void TableIndexList::AddIndex(unique_ptr<Index> index) {
	D_ASSERT(index);
	lock_guard<mutex> lock(indexes_lock);
	indexes.push_back(std::move(index));
}

void TableIndexList::RemoveIndex(const string &name) {
	lock_guard<mutex> lock(indexes_lock);

	for (idx_t i = 0; i < indexes.size(); i++) {
		auto &index = indexes[i];
		if (index->GetIndexName() == name) {
			indexes.erase_at(i);
			break;
		}
	}
}

void TableIndexList::CommitDrop(const string &name) {
	lock_guard<mutex> lock(indexes_lock);

	for (auto &index : indexes) {
		if (index->GetIndexName() == name) {
			index->CommitDrop();
		}
	}
}

bool TableIndexList::NameIsUnique(const string &name) {
	lock_guard<mutex> lock(indexes_lock);

	// Only covers PK, FK, and UNIQUE indexes.
	for (const auto &index : indexes) {
		if (index->IsPrimary() || index->IsForeign() || index->IsUnique()) {
			if (index->GetIndexName() == name) {
				return false;
			}
		}
	}
	return true;
}

optional_ptr<BoundIndex> TableIndexList::Find(const string &name) {
	for (auto &index : indexes) {
		if (index->GetIndexName() == name) {
			return index->Cast<BoundIndex>();
		}
	}
	return nullptr;
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

	// Get the table from the catalog, so we can add it to the binder.
	auto &catalog = table_info.GetDB().GetCatalog();
	auto schema = table_info.GetSchemaName();
	auto table_name = table_info.GetTableName();
	auto &table_entry = catalog.GetEntry(context, CatalogType::TABLE_ENTRY, schema, table_name);
	auto &table = table_entry.Cast<DuckTableEntry>();

	vector<LogicalType> column_types;
	vector<string> column_names;
	for (auto &col : table.GetColumns().Logical()) {
		column_types.push_back(col.Type());
		column_names.push_back(col.Name());
	}

	lock_guard<mutex> lock(indexes_lock);
	for (auto &index : indexes) {
		if (!index->IsBound() && (index_type == nullptr || index->GetIndexType() == index_type)) {
			// Create a binder to bind this index.
			auto binder = Binder::CreateBinder(context);

			// Add the table to the binder.
			vector<ColumnIndex> dummy_column_ids;
			binder->bind_context.AddBaseTable(0, string(), column_names, column_types, dummy_column_ids, table);

			// Create an IndexBinder to bind the index
			IndexBinder idx_binder(*binder, context);

			// Replace the unbound index with a bound index.
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
	for (auto &index_elem : indexes) {
		if (IsForeignKeyIndex(fk_keys, *index_elem, fk_type)) {
			return index_elem;
		}
	}
	return nullptr;
}

void TableIndexList::VerifyForeignKey(optional_ptr<LocalTableStorage> storage, const vector<PhysicalIndex> &fk_keys,
                                      DataChunk &chunk, ConflictManager &conflict_manager) {
	auto fk_type = conflict_manager.LookupType() == VerifyExistenceType::APPEND_FK
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
	lock_guard<mutex> lock(indexes_lock);
	unordered_set<column_t> column_ids;
	for (auto &index : indexes) {
		for (auto col_id : index->GetColumnIds()) {
			column_ids.insert(col_id);
		}
	}
	return column_ids;
}

vector<IndexStorageInfo> TableIndexList::GetStorageInfos(const case_insensitive_map_t<Value> &options) {
	vector<IndexStorageInfo> infos;
	for (auto &index : indexes) {
		if (index->IsBound()) {
			auto info = index->Cast<BoundIndex>().GetStorageInfo(options, false);
			D_ASSERT(info.IsValid() && !info.name.empty());
			infos.push_back(info);
			continue;
		}

		auto info = index->Cast<UnboundIndex>().GetStorageInfo();
		D_ASSERT(!info.name.empty());
		infos.push_back(info);
	}
	return infos;
}

} // namespace duckdb
