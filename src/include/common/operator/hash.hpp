//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// common/operator/hash.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#include "common/internal_types.hpp"
#include "common/types/hash.hpp"
#include "common/types/null_value.hpp"

#pragma once

namespace operators {

struct Hash {
	template <class T> static inline int32_t Operation(T input, bool is_null) {
		if (is_null) {
			return duckdb::Hash<T>(duckdb::NullValue<T>());
		}
		return duckdb::Hash<T>(input);
	}
};

} // namespace operators