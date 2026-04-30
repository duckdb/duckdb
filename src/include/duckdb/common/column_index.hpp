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
	//! Regular column index, refers to a column in its entirety
	FULL_READ,
	//! This index references a struct/variant field that MUST be extracted by the scan and emitted directly
	PUSHDOWN_EXTRACT
};

struct ColumnIndex {
public:
	//! FIXME: this initializes the index to COLUMN_IDENTIFIER_ROW_ID (same numeric representation as INVALID_INDEX)
	ColumnIndex() : has_index(true), index(DConstants::INVALID_INDEX), index_type(ColumnIndexType::FULL_READ) {
	}
	explicit ColumnIndex(idx_t index)
	    : has_index(true), index(index), type(LogicalType::INVALID), index_type(ColumnIndexType::FULL_READ) {
	}
	explicit ColumnIndex(const string &field)
	    : has_index(false), field(field), type(LogicalType::INVALID), index_type(ColumnIndexType::FULL_READ) {
	}

	ColumnIndex(idx_t index, vector<ColumnIndex> child_indexes_p)
	    : has_index(true), index(index), index_type(ColumnIndexType::FULL_READ),
	      child_indexes(std::move(child_indexes_p)) {
	}
	ColumnIndex(const string &field, vector<ColumnIndex> child_indexes_p)
	    : has_index(false), field(field), index_type(ColumnIndexType::FULL_READ),
	      child_indexes(std::move(child_indexes_p)) {
	}

public:
	inline bool operator==(const ColumnIndex &rhs) const {
		if (has_index != rhs.has_index) {
			return false;
		}
		if (has_index) {
			if (index != rhs.index) {
				return false;
			}
		} else {
			if (field != rhs.field) {
				return false;
			}
		}
		if (type != rhs.type) {
			return false;
		}
		if (index_type != rhs.index_type) {
			return false;
		}

		if (child_indexes.size() != rhs.child_indexes.size()) {
			return false;
		}
		for (idx_t i = 0; i < child_indexes.size(); i++) {
			auto &a = child_indexes[i];
			auto &b = rhs.child_indexes[i];
			if (a != b) {
				return false;
			}
		}
		return true;
	}
	inline bool operator!=(const ColumnIndex &rhs) const {
		return !(*this == rhs);
	}
	inline bool operator<(const ColumnIndex &rhs) const {
		//! FIXME: does it make sense to check children here?
		return index < rhs.index;
	}

public:
	bool HasPrimaryIndex() const {
		return has_index;
	}
	idx_t GetPrimaryIndex() const {
		if (!has_index) {
			throw InternalException("Attempted to get the primary index (numeric) for an index that consists of a "
			                        "field identifier (string: %s)",
			                        field);
		}
		return index;
	}
	const string &GetFieldName() const {
		if (has_index) {
			throw InternalException("Attempted to get the field identifier (string) for an index that consists of a "
			                        "primary index (numeric: %d)",
			                        index);
		}
		return field;
	}
	LogicalIndex ToLogical() const {
		return LogicalIndex(GetPrimaryIndex());
	}
	bool HasChildren() const {
		return !child_indexes.empty();
	}
	bool HasType() const {
		return type.id() != LogicalTypeId::INVALID;
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

	ColumnIndex RemapRootIndex(idx_t new_index) const {
		auto res = *this;
		if (!has_index) {
			throw InternalException("Can't perform 'RemapRootIndex' on ColumnIndex without a primary index!");
		}
		res.index = new_index;
		return res;
	}

	bool IsPushdownExtract() const {
		return index_type == ColumnIndexType::PUSHDOWN_EXTRACT;
	}
	void SetType(const LogicalType &type_information) {
		type = type_information;
	}
	void SetPushdownExtract() {
		if (!HasType()) {
			throw InternalException("Can't set pushdown-extract on a ColumnIndex without type information");
		}
		if (child_indexes.size() != 1) {
			throw InternalException("Can't set pushdown-extract on a ColumnIndex with %d children, expected 1",
			                        child_indexes.size());
		}
		index_type = ColumnIndexType::PUSHDOWN_EXTRACT;
	}
	void SetPushdownExtractType(const LogicalType &type_information,
	                            optional_ptr<const LogicalType> cast_type = nullptr);
	const LogicalType &GetScanType() const;
	const LogicalType &GetType() const;
	void AddChildIndex(ColumnIndex new_index);
	bool IsRowIdColumn() const;
	bool IsRowNumberColumn() const;
	bool IsEmptyColumn() const;
	bool IsVirtualColumn() const;
	void VerifySinglePath() const;
	bool IsChildPathOf(const ColumnIndex &path) const;

	string GetName(const string &column_name) const;

public:
	void Serialize(Serializer &serializer) const;
	static ColumnIndex Deserialize(Deserializer &deserializer);

private:
	//! The column/field index (if structured type)
	bool has_index = true;
	idx_t index;
	//! The column/field name (if semi-structured type)
	string field;

	//! The logical type of the column this references (if pushdown extract)
	LogicalType type = LogicalType::INVALID;
	//! The type of index, controlling how it's interpreted
	ColumnIndexType index_type;
	vector<ColumnIndex> child_indexes;
};

} // namespace duckdb
