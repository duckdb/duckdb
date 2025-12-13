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
	ColumnIndex() : index(DConstants::INVALID_INDEX), index_type(ColumnIndexType::INVALID) {
	}
	explicit ColumnIndex(idx_t index)
	    : index(index), type(LogicalType::INVALID), index_type(ColumnIndexType::FULL_READ) {
	}
	ColumnIndex(idx_t index, vector<ColumnIndex> child_indexes_p)
	    : index(index), index_type(ColumnIndexType::FULL_READ), child_indexes(std::move(child_indexes_p)) {
	}

	inline bool operator==(const ColumnIndex &rhs) const {
		if (index != rhs.index) {
			return false;
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
	idx_t GetPrimaryIndex() const {
		return index;
	}
	LogicalIndex ToLogical() const {
		return LogicalIndex(index);
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

	bool IsPushdownExtract() const {
		return index_type == ColumnIndexType::PUSHDOWN_EXTRACT;
	}
	void SetType(const LogicalType &type_information) {
		type = type_information;
	}
	void SetPushdownExtractType(const LogicalType &type_information) {
		//! We can upgrade the optional prune hint to a PUSHDOWN_EXTRACT, which is no longer optional
		index_type = ColumnIndexType::PUSHDOWN_EXTRACT;
		type = type_information;
		D_ASSERT(child_indexes.size() == 1);

		auto &child = child_indexes[0];
		auto &child_types = StructType::GetChildTypes(type);
		if (child.child_indexes.empty()) {
			child.SetType(child_types[child.GetPrimaryIndex()].second);
		} else {
			child.SetPushdownExtractType(child_types[child.GetPrimaryIndex()].second);
		}
	}
	const LogicalType &GetScanType() const {
		D_ASSERT(HasType());
		if (IsPushdownExtract()) {
			return child_indexes[0].GetScanType();
		}
		return GetType();
	}
	const LogicalType &GetType() const {
		D_ASSERT(type.id() != LogicalTypeId::INVALID);
		return type;
	}
	void AddChildIndex(ColumnIndex new_index) {
		this->child_indexes.push_back(std::move(new_index));
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
	void VerifySinglePath() const {
		if (child_indexes.empty()) {
			return;
		}
		if (child_indexes.size() != 1) {
			throw InternalException(
			    "We were expecting to find a single path in the index, meaning 0 or 1 children, found: %d",
			    child_indexes.size());
		}
		child_indexes[0].VerifySinglePath();
	}
	bool IsChildPathOf(const ColumnIndex &path) const {
		VerifySinglePath();
		path.VerifySinglePath();
		reference<const ColumnIndex> a(*this);
		reference<const ColumnIndex> b(path);

		while (true) {
			if (a.get().GetPrimaryIndex() != b.get().GetPrimaryIndex()) {
				return false;
			}
			const bool a_has_children = a.get().HasChildren();
			const bool b_has_children = b.get().HasChildren();
			if (!a_has_children && !b_has_children) {
				return false;
			}
			if (!a_has_children) {
				//! a's path has stopped short of b's path
				return false;
			}
			if (!b_has_children) {
				//! b's path is a subset of a's path, so it's a parent path
				return true;
			}
			a = a.get().GetChildIndexes()[0];
			b = b.get().GetChildIndexes()[0];
		}
		return true;
	}

public:
	void Serialize(Serializer &serializer) const;
	static ColumnIndex Deserialize(Deserializer &deserializer);

private:
	idx_t index;
	//! The logical type of the column this references (if pushdown extract)
	LogicalType type = LogicalType::INVALID;
	//! The type of index, controlling how it's interpreted
	ColumnIndexType index_type;
	vector<ColumnIndex> child_indexes;
};

} // namespace duckdb
