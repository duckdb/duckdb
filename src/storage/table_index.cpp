#include "duckdb/storage/table_index.hpp"
#include "duckdb/storage/data_table.hpp"

namespace duckdb {
void TableIndex::AddIndex(unique_ptr<Index> index) {
	D_ASSERT(index);
	lock_guard<mutex> lock(indexes_lock);
	indexes.push_back(move(index));
}
void TableIndex::RemoveIndex(Index *index) {
	D_ASSERT(index);
	lock_guard<mutex> lock(indexes_lock);

	for (idx_t index_idx = 0; index_idx < indexes.size(); index_idx++) {
		auto &index_entry = indexes[index_idx];
		if (index_entry.get() == index) {
			indexes.erase(indexes.begin() + index_idx);
			break;
		}
	}
}

bool TableIndex::Empty() {
	lock_guard<mutex> lock(indexes_lock);
	return indexes.empty();
}

idx_t TableIndex::Count() {
	lock_guard<mutex> lock(indexes_lock);
	return indexes.size();
}

Index *TableIndex::FindForeignKeyIndex(const vector<idx_t> &fk_keys, ForeignKeyType fk_type) {
	Index *result = nullptr;
	Scan([&](Index &index) {
		if (DataTable::IsForeignKeyIndex(fk_keys, index, fk_type)) {
			result = &index;
		}
		return false;
	});
	return result;
}

vector<BlockPointer> TableIndex::SerializeIndexes(duckdb::MetaBlockWriter &writer) {
	vector<BlockPointer> blocks_info;
	for (auto &index : indexes) {
		blocks_info.emplace_back(index->Serialize(writer));
	}
	return blocks_info;
}

} // namespace duckdb
