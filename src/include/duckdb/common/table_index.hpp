//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/table_index.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include <functional>

namespace duckdb {

struct TableIndex {
	TableIndex() : index(DConstants::INVALID_INDEX) {
	}
	explicit TableIndex(idx_t index) : index(index) {
	}

	idx_t index;

	inline bool operator==(const TableIndex &rhs) const {
		return index == rhs.index;
	};
	inline bool operator<(const TableIndex &rhs) const {
		return index < rhs.index;
	};
	bool operator!=(const TableIndex &other) const {
		return !(*this == other);
	}
	bool operator>(const TableIndex &other) const {
		return other < *this;
	}
	bool operator<=(const TableIndex &other) const {
		return !(other < *this);
	}
	bool operator>=(const TableIndex &other) const {
		return !(*this < other);
	}
	bool IsValid() const {
		return index != DConstants::INVALID_INDEX;
	}
};

} // namespace duckdb

namespace std {

template <>
struct hash<duckdb::TableIndex> {
	size_t operator()(const duckdb::TableIndex &tbl_index) const {
		return std::hash<uint64_t> {}(tbl_index.index);
	}
};
} // namespace std
