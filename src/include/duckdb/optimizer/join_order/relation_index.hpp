//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_order/relation_index.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/typed_index.hpp"

namespace duckdb {
struct RelationIndex : public TypedIndex<RelationIndex> {
	RelationIndex() = default;
	explicit RelationIndex(idx_t index) : TypedIndex(index) {
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
