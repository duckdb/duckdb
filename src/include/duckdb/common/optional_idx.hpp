//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/optional_idx.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/exception.hpp"

namespace duckdb {

class optional_idx {
	static constexpr const idx_t INVALID_INDEX = idx_t(-1);

public:
	optional_idx() : index(INVALID_INDEX) {
	}
	optional_idx(idx_t index) : index(index) { // NOLINT: allow implicit conversion from idx_t
		if (index == INVALID_INDEX) {
			throw InternalException("optional_idx cannot be initialized with an invalid index");
		}
	}

	static optional_idx Invalid() {
		return optional_idx();
	}

	bool IsValid() const {
		return index != INVALID_INDEX;
	}

	idx_t GetIndex() const {
		if (index == INVALID_INDEX) {
			throw InternalException("Attempting to get the index of an optional_idx that is not set");
		}
		return index;
	}

	inline bool operator==(const optional_idx &rhs) const {
		return index == rhs.index;
	}

private:
	idx_t index;
};

} // namespace duckdb
