//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/typed_index.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

//! CRTP base for strongly typed index wrappers
template <class T>
struct TypedIndex {
	TypedIndex() : index(DConstants::INVALID_INDEX) {
	}
	explicit TypedIndex(idx_t index) : index(index) {
	}

	idx_t index;

	bool IsValid() const {
		return index != DConstants::INVALID_INDEX;
	}

	friend bool operator==(const T &lhs, const T &rhs) {
		return lhs.index == rhs.index;
	}
	friend bool operator!=(const T &lhs, const T &rhs) {
		return lhs.index != rhs.index;
	}
	friend bool operator<(const T &lhs, const T &rhs) {
		return lhs.index < rhs.index;
	}
	friend bool operator>(const T &lhs, const T &rhs) {
		return lhs.index > rhs.index;
	}
	friend bool operator<=(const T &lhs, const T &rhs) {
		return lhs.index <= rhs.index;
	}
	friend bool operator>=(const T &lhs, const T &rhs) {
		return lhs.index >= rhs.index;
	}
};

} // namespace duckdb
