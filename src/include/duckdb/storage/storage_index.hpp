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
#include "duckdb/common/types.hpp"

namespace duckdb {

enum class StorageIndexType : uint8_t { DIRECT_READ, OPTIONAL_PRUNE_HINT, PUSHDOWN_EXTRACT };

struct StorageIndex {
public:
	StorageIndex() : index(COLUMN_IDENTIFIER_ROW_ID), index_type(StorageIndexType::DIRECT_READ) {
	}
	explicit StorageIndex(idx_t index) : index(index), index_type(StorageIndexType::DIRECT_READ) {
	}
	StorageIndex(idx_t index, vector<StorageIndex> child_indexes_p)
	    : index(index), index_type(StorageIndexType::DIRECT_READ), child_indexes(std::move(child_indexes_p)) {
		if (!child_indexes.empty()) {
			index_type = StorageIndexType::OPTIONAL_PRUNE_HINT;
		}
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

public:
	idx_t GetPrimaryIndex() const {
		return index;
	}
	PhysicalIndex ToPhysical() const {
		return PhysicalIndex(index);
	}
	const LogicalType &GetType() const {
		return type;
	}
	bool HasChildren() const {
		return !child_indexes.empty();
	}
	idx_t ChildIndexCount() const {
		return child_indexes.size();
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
		if (index_type == StorageIndexType::DIRECT_READ) {
			index_type = StorageIndexType::OPTIONAL_PRUNE_HINT;
		}
	}
	void SetType(const LogicalType &type_information) {
		type = type_information;
	}
	void SetPushdownExtract() {
		D_ASSERT(!IsPushdownExtract());
		D_ASSERT(index_type == StorageIndexType::OPTIONAL_PRUNE_HINT);
		index_type = StorageIndexType::PUSHDOWN_EXTRACT;
	}
	bool IsPushdownExtract() const {
		return index_type == StorageIndexType::PUSHDOWN_EXTRACT;
	}
	void SetIndex(idx_t new_index) {
		index = new_index;
	}
	bool IsRowIdColumn() const {
		return index == DConstants::INVALID_INDEX;
	}

private:
	idx_t index;
	LogicalType type = LogicalType::INVALID;
	StorageIndexType index_type;
	vector<StorageIndex> child_indexes;
};

} // namespace duckdb
