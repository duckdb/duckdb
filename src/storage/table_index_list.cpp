#include "duckdb/storage/table/table_index_list.hpp"

#include "duckdb/storage/data_table.hpp"
#include "duckdb/common/types/conflict_manager.hpp"
#include "duckdb/execution/index/unknown_index.hpp"
#include "duckdb/execution/index/index_type_set.hpp"
#include "duckdb/storage/table/data_table_info.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/config.hpp"

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
		if (index_entry->name == name) {
			indexes.erase(indexes.begin() + index_idx);
			break;
		}
	}
}

void TableIndexList::CommitDrop(const string &name) {
	lock_guard<mutex> lock(indexes_lock);

	for (idx_t index_idx = 0; index_idx < indexes.size(); index_idx++) {
		auto &index_entry = indexes[index_idx];
		if (index_entry->name == name) {
			index_entry->CommitDrop();
			break;
		}
	}
}

bool TableIndexList::NameIsUnique(const string &name) {
	lock_guard<mutex> lock(indexes_lock);

	// only cover PK, FK, and UNIQUE, which are not (yet) catalog entries
	for (idx_t index_idx = 0; index_idx < indexes.size(); index_idx++) {
		auto &index_entry = indexes[index_idx];
		if (index_entry->IsPrimary() || index_entry->IsForeign() || index_entry->IsUnique()) {
			if (index_entry->name == name) {
				return false;
			}
		}
	}

	return true;
}

void TableIndexList::InitializeIndexes(ClientContext &context, DataTableInfo &table_info) {
	lock_guard<mutex> lock(indexes_lock);
	for (auto &index : indexes) {
		if (!index->IsUnknown()) {
			continue;
		}

		auto &unknown_index = index->Cast<UnknownIndex>();
		auto &index_type_name = unknown_index.GetIndexType();

		// Do we know the type of this index now?
		auto index_type = context.db->config.GetIndexTypes().FindByName(index_type_name);
		if (!index_type) {
			continue;
		}

		// Swap this with a new index
		auto &create_info = unknown_index.GetCreateInfo();
		auto &storage_info = unknown_index.GetStorageInfo();

		auto index_instance = index_type->create_instance(create_info.index_name, create_info.constraint_type,
		                                                  create_info.column_ids, unknown_index.unbound_expressions,
		                                                  *table_info.table_io_manager, table_info.db, storage_info);

		index = std::move(index_instance);
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
	conflict_manager.SetIndexCount(1);
	index->CheckConstraintsForChunk(chunk, conflict_manager);
}

vector<column_t> TableIndexList::GetRequiredColumns() {
	lock_guard<mutex> lock(indexes_lock);
	set<column_t> unique_indexes;
	for (auto &index : indexes) {
		for (auto col_index : index->column_ids) {
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
		auto index_storage_info = index->GetStorageInfo(false);
		D_ASSERT(index_storage_info.IsValid() && !index_storage_info.name.empty());
		index_storage_infos.push_back(index_storage_info);
	}
	return index_storage_infos;
}

} // namespace duckdb
