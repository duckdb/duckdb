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
	StorageIndex();
	explicit StorageIndex(idx_t index);
	explicit StorageIndex(const string &field);
	StorageIndex(idx_t index, vector<StorageIndex> child_indexes_p);
	StorageIndex(const string &field, vector<StorageIndex> child_indexes_p);

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
	static StorageIndex FromColumnIndex(const ColumnIndex &column_id);

public:
	bool HasPrimaryIndex() const;
	idx_t GetPrimaryIndex() const;
	const string &GetFieldName() const;
	PhysicalIndex ToPhysical() const;
	bool HasType() const;
	const LogicalType &GetScanType() const;
	const LogicalType &GetType() const;
	bool HasChildren() const;
	idx_t ChildIndexCount() const;
	const StorageIndex &GetChildIndex(idx_t idx) const;
	StorageIndex &GetChildIndex(idx_t idx);
	const vector<StorageIndex> &GetChildIndexes() const;
	void AddChildIndex(StorageIndex new_index);
	void SetType(const LogicalType &type_information);
	void SetPushdownExtract();
	bool IsPushdownExtract() const;
	void SetIndex(idx_t new_index);
	bool IsRowIdColumn() const;

	void Serialize(Serializer &serializer) const;
	static StorageIndex Deserialize(Deserializer &deserializer);

private:
	bool has_index = true;
	idx_t index;
	string field;

	LogicalType type = LogicalType::INVALID;
	StorageIndexType index_type;
	vector<StorageIndex> child_indexes;
};

} // namespace duckdb
