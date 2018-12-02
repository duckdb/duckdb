//===----------------------------------------------------------------------===// 
// 
//                         DuckDB 
// 
// common/operator/hash_operators.hpp
// 
// 
// 
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"
#include "common/types/hash.hpp"
#include "common/types/null_value.hpp"

namespace operators {

struct Hash {
	template <class T> static inline uint64_t Operation(T input, bool is_null) {
		if (is_null) {
			return duckdb::Hash<T>(duckdb::NullValue<T>());
		}
		return duckdb::Hash<T>(input);
	}
};

} // namespace operators