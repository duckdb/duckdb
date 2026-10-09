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
	constexpr optional_idx() : index(INVALID_INDEX) {
	}
	// NOLINTNEXTLINE: allow implicit conversion from idx_t
	constexpr optional_idx(idx_t index_p)
	    : index(index_p == INVALID_INDEX ? (ThrowInvalidInitialization(), index_p) : index_p) {
	}

	static optional_idx Invalid() {
		return optional_idx();
	}

	bool IsValid() const {
		return index != INVALID_INDEX;
	}

	void SetInvalid() {
		index = INVALID_INDEX;
	}

	idx_t GetIndex() const {
		if (index == INVALID_INDEX) {
			ThrowNotSet();
		}
		return index;
	}

	inline bool operator==(const optional_idx &rhs) const {
		return index == rhs.index;
	}

	inline bool operator!=(const optional_idx &rhs) const {
		return index != rhs.index;
	}

private:
	//! Kept out-of-line so that the throwing paths do not block inlining of the accessors
	[[noreturn]] DUCKDB_API static void ThrowInvalidInitialization();
	[[noreturn]] DUCKDB_API static void ThrowNotSet();

private:
	idx_t index;
};

} // namespace duckdb
