//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/operator/hash_operators.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/types/null_value.hpp"

namespace duckdb {

struct HashOp {
	template <class T> static inline uint64_t Operation(T input, bool is_null) {
		if (is_null) {
			return duckdb::Hash<T>(duckdb::NullValue<T>());
		}
		return duckdb::Hash<T>(input);
	}
};

} // namespace duckdb
