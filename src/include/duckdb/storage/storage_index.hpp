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
#include "duckdb/common/column_index.hpp"

namespace duckdb {

enum class StorageIndexType : uint8_t { FULL_READ, PUSHDOWN_EXTRACT };

struct StorageIndex {
public:
	StorageIndex() : has_index(true), index(COLUMN_IDENTIFIER_ROW_ID), index_type(StorageIndexType::FULL_READ) {
	}
	explicit StorageIndex(idx_t index) : has_index(true), index(index), index_type(StorageIndexType::FULL_READ) {
	}
	explicit StorageIndex(const string &field)
	    : has_index(false), field(field), index_type(StorageIndexType::FULL_READ) {
	}
	StorageIndex(idx_t index, vector<StorageIndex> child_indexes_p)
	    : has_index(true), index(index), index_type(StorageIndexType::FULL_READ),
	      child_indexes(std::move(child_indexes_p)) {
	}
	StorageIndex(const string &field, vector<StorageIndex> child_indexes_p)
	    : has_index(false), field(field), index_type(StorageIndexType::FULL_READ),
	      child_indexes(std::move(child_indexes_p)) {
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
	static StorageIndex FromColumnIndex(const ColumnIndex &column_id) {
		vector<StorageIndex> result;
		for (auto &child_id : column_id.GetChildIndexes()) {
			result.push_back(StorageIndex::FromColumnIndex(child_id));
		}
		StorageIndex storage_index;
		if (column_id.HasPrimaryIndex()) {
			storage_index = StorageIndex(column_id.GetPrimaryIndex(), std::move(result));
		} else {
			storage_index = StorageIndex(column_id.GetFieldName(), std::move(result));
		}
		if (column_id.HasType()) {
			storage_index.SetType(column_id.GetType());
		}
		if (column_id.IsPushdownExtract()) {
			storage_index.SetPushdownExtract();
		}
		return storage_index;
	}

public:
	bool HasPrimaryIndex() const {
		return has_index;
	}
	idx_t GetPrimaryIndex() const {
		D_ASSERT(has_index);
		return index;
	}
	const string &GetFieldName() const {
		D_ASSERT(!has_index);
		return field;
	}
	PhysicalIndex ToPhysical() const {
		D_ASSERT(has_index);
		return PhysicalIndex(index);
	}
	bool HasType() const {
		return type.id() != LogicalTypeId::INVALID;
	}
	const LogicalType &GetScanType() const {
		D_ASSERT(HasType());
		if (IsPushdownExtract()) {
			return child_indexes[0].GetScanType();
		}
		return GetType();
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
	}
	void SetType(const LogicalType &type_information) {
		type = type_information;
	}
	void SetPushdownExtract() {
		D_ASSERT(!IsPushdownExtract());
		index_type = StorageIndexType::PUSHDOWN_EXTRACT;
	}
	bool IsPushdownExtract() const {
		return index_type == StorageIndexType::PUSHDOWN_EXTRACT;
	}
	void SetIndex(idx_t new_index) {
		D_ASSERT(has_index);
		index = new_index;
	}
	bool IsRowIdColumn() const {
		if (!has_index) {
			return false;
		}
		return index == DConstants::INVALID_INDEX;
	}

private:
	bool has_index = true;
	idx_t index;
	string field;

	LogicalType type = LogicalType::INVALID;
	StorageIndexType index_type;
	vector<StorageIndex> child_indexes;
};

} // namespace duckdb
