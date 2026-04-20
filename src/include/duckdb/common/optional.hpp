//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/optional.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/exception.hpp"

namespace duckdb {

//! FIXME: Remove this in favor of std::optional once all migration to c++17  is done.
template <typename T>
class optional {
public:
	optional() : has_val(false), value(T()) {
	}
	optional(T value) : has_val(true), value(std::move(value)) { // NOLINT: allow implicit conversion
	}

	bool IsValid() const {
		return has_val;
	}

	explicit operator bool() const {
		return has_val;
	}

	void Invalidate() {
		has_val = false;
		value = T();
	}

	const T &GetValue() const {
		if (!has_val) {
			throw InternalException("Attempting to get the value of an optional that is not set");
		}
		return value;
	}

	inline bool operator==(const optional &rhs) const {
		if (has_val != rhs.has_val) {
			return false;
		}
		if (!has_val) {
			return true;
		}
		return value == rhs.value;
	}

	inline bool operator!=(const optional &rhs) const {
		return !(*this == rhs);
	}

private:
	bool has_val;
	T value;
};

} // namespace duckdb
