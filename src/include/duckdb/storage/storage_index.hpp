//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/storage_index.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/column_index.hpp"

namespace duckdb {

struct StorageIndex {
	StorageIndex() : index(DConstants::INVALID_INDEX) {
	}
	explicit StorageIndex(idx_t index) : index(index) {
	}
	StorageIndex(idx_t index, vector<StorageIndex> child_indexes_p)
	    : index(index), child_indexes(std::move(child_indexes_p)) {
	}

	inline bool operator==(const StorageIndex &rhs) const {
		return index == rhs.index;
	}
	inline bool operator!=(const StorageIndex &rhs) const {
		return index != rhs.index;
	}
	inline bool operator<(const StorageIndex &rhs) const {
		return index < rhs.index;
	}
	idx_t GetPrimaryIndex() const {
		return index;
	}
	PhysicalIndex ToPhysical() const {
		return PhysicalIndex(index);
	}
	bool HasChildren() const {
		return !child_indexes.empty();
	}
	idx_t ChildIndexCount() const {
		return child_indexes.size();
	}
	static StorageIndex FromColumnIndex(const ColumnIndex &column_id) {
		vector<StorageIndex> result;
		for (auto &child_id : column_id.GetChildIndexes()) {
			result.push_back(StorageIndex::FromColumnIndex(child_id));
		}
		auto storage_index = StorageIndex(column_id.GetPrimaryIndex(), std::move(result));
		return storage_index;
	}
	const StorageIndex &GetChildIndex(idx_t idx) const {
		return child_indexes[idx];
	}
	StorageIndex &GetChildIndex(idx_t idx) {
		return child_indexes[idx];
	}
	const vector<StorageIndex> &GetChildIndexes() const {
		return child_indexes;
	}
	void AddChildIndex(StorageIndex new_index) {
		this->child_indexes.push_back(std::move(new_index));
	}
	void SetIndex(idx_t new_index) {
		index = new_index;
	}
	bool IsRowIdColumn() const {
		return index == DConstants::INVALID_INDEX;
	}

private:
	idx_t index;
	vector<StorageIndex> child_indexes;
};

} // namespace duckdb
