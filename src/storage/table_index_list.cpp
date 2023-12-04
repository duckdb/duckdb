#include "duckdb/storage/table/table_index_list.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/common/types/conflict_manager.hpp"
#include "duckdb/execution/index/art/art.hpp"

namespace duckdb {
void TableIndexList::AddIndex(unique_ptr<Index> index) {
	D_ASSERT(index);
	lock_guard<mutex> lock(indexes_lock);
	indexes.push_back(std::move(index));
}

void TableIndexList::RemoveIndex(Index &index) {
	lock_guard<mutex> lock(indexes_lock);

	for (idx_t index_idx = 0; index_idx < indexes.size(); index_idx++) {
		auto &index_entry = indexes[index_idx];
		if (index_entry.get() == &index) {
			indexes.erase(indexes.begin() + index_idx);
			break;
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

vector<BlockPointer> TableIndexList::SerializeIndexes(duckdb::MetadataWriter &writer) {
	vector<BlockPointer> blocks_info;
	for (auto &index : indexes) {
		blocks_info.emplace_back(index->Serialize(writer));
	}
	return blocks_info;
}

} // namespace duckdb
