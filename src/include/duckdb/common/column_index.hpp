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
#include "duckdb/common/types.hpp"

namespace duckdb {

enum class ColumnIndexType : uint8_t {
	INVALID,
	//! Regular column index
	DIRECT_READ,
	//! Hint to scans that unreferenced fields *can* be pruned.
	OPTIONAL_PRUNE_HINT,
	//! This index references a struct/variant field that MUST be extracted by the scan and emitted directly
	PUSHDOWN_EXTRACT
};

struct ColumnIndex {
public:
	//! FIXME: this initializes the index to COLUMN_IDENTIFIER_ROW_ID (same numeric representation as INVALID_INDEX)
	ColumnIndex() : index(DConstants::INVALID_INDEX), index_type(ColumnIndexType::INVALID) {
	}
	explicit ColumnIndex(idx_t index)
	    : index(index), type(LogicalType::INVALID), index_type(ColumnIndexType::DIRECT_READ) {
	}
	ColumnIndex(idx_t index, const LogicalType &type)
	    : index(index), type(type), index_type(ColumnIndexType::DIRECT_READ) {
	}
	ColumnIndex(idx_t index, const LogicalType &type, vector<ColumnIndex> child_indexes_p)
	    : index(index), type(type), index_type(ColumnIndexType::DIRECT_READ),
	      child_indexes(std::move(child_indexes_p)) {
		if (!child_indexes.empty()) {
			index_type = ColumnIndexType::OPTIONAL_PRUNE_HINT;
		}
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

public:
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

	bool IsPushdownExtract() const {
		return index_type == ColumnIndexType::PUSHDOWN_EXTRACT;
	}
	void SetPushdownExtractType() {
		//! TODO: this probably has to be discussed with the storage scan first, to determine if it's supported
		//! We can upgrade the optional prune hint to a PUSHDOWN_EXTRACT, which is no longer optional
		D_ASSERT(index_type == ColumnIndexType::OPTIONAL_PRUNE_HINT);
		index_type = ColumnIndexType::PUSHDOWN_EXTRACT;
	}
	const LogicalType &GetType() const {
		return type;
	}
	void AddChildIndex(ColumnIndex new_index) {
		this->child_indexes.push_back(std::move(new_index));
		if (index_type == ColumnIndexType::DIRECT_READ) {
			index_type = ColumnIndexType::OPTIONAL_PRUNE_HINT;
		}
	}
	bool IsRowIdColumn() const {
		return index == COLUMN_IDENTIFIER_ROW_ID;
	}
	bool IsEmptyColumn() const {
		return index == COLUMN_IDENTIFIER_EMPTY;
	}
	bool IsVirtualColumn() const {
		return index >= VIRTUAL_COLUMN_START;
	}

public:
	void Serialize(Serializer &serializer) const;
	static ColumnIndex Deserialize(Deserializer &deserializer);

private:
	idx_t index;
	//! The logical type of the column this references
	LogicalType type;
	//! The type of index, controlling how it's interpreted
	ColumnIndexType index_type;
	vector<ColumnIndex> child_indexes;
};

} // namespace duckdb
