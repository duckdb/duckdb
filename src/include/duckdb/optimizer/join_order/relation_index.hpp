//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_order/relation_index.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {
struct RelationIndex {
	RelationIndex() : index(DConstants::INVALID_INDEX) {
	}
	explicit RelationIndex(idx_t index) : index(index) {
	}

	idx_t index;

	inline bool operator==(const RelationIndex &rhs) const {
		return index == rhs.index;
	};
	inline bool operator<(const RelationIndex &rhs) const {
		return index < rhs.index;
	};
	bool operator!=(const RelationIndex &other) const {
		return !(*this == other);
	}
	bool operator>(const RelationIndex &other) const {
		return other < *this;
	}
	bool operator<=(const RelationIndex &other) const {
		return !(other < *this);
	}
	bool operator>=(const RelationIndex &other) const {
		return !(*this < other);
	}
	bool IsValid() const {
		return index != DConstants::INVALID_INDEX;
	}
};
} // namespace duckdb

namespace std {
template <>
struct hash<duckdb::RelationIndex> {
	size_t operator()(const duckdb::RelationIndex &rel_index) const {
		return std::hash<uint64_t> {}(rel_index.index);
	}
};
} // namespace std
