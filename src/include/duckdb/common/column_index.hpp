//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/column_index.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

struct ColumnIndex {
	ColumnIndex() : index(DConstants::INVALID_INDEX) {
	}
	explicit ColumnIndex(idx_t index) : index(index) {
	}
	ColumnIndex(idx_t index, vector<ColumnIndex> child_indexes_p)
	    : index(index), child_indexes(std::move(child_indexes_p)) {
	}

	inline bool operator==(const ColumnIndex &rhs) const {
		return index == rhs.index;
	}
	inline bool operator!=(const ColumnIndex &rhs) const {
		return index != rhs.index;
	}
	inline bool operator<(const ColumnIndex &rhs) const {
		return index < rhs.index;
	}
	idx_t GetPrimaryIndex() const {
		return index;
	}
	LogicalIndex ToLogical() const {
		return LogicalIndex(index);
	}
	bool HasChildren() const {
		return !child_indexes.empty();
	}
	idx_t ChildIndexCount() const {
		return child_indexes.size();
	}
	const ColumnIndex &GetChildIndex(idx_t idx) const {
		return child_indexes[idx];
	}
	ColumnIndex &GetChildIndex(idx_t idx) {
		return child_indexes[idx];
	}
	const vector<ColumnIndex> &GetChildIndexes() const {
		return child_indexes;
	}
	vector<ColumnIndex> &GetChildIndexesMutable() {
		return child_indexes;
	}
	void AddChildIndex(ColumnIndex new_index) {
		this->child_indexes.push_back(std::move(new_index));
	}
	bool IsRowIdColumn() const {
		return index == DConstants::INVALID_INDEX;
	}
	bool IsVirtualColumn() const {
		return index >= VIRTUAL_COLUMN_START;
	}
	void Serialize(Serializer &serializer) const;
	static ColumnIndex Deserialize(Deserializer &deserializer);

private:
	idx_t index;
	vector<ColumnIndex> child_indexes;
};

} // namespace duckdb
