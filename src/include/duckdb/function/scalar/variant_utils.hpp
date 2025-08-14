//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/scalar/variant_utils.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/function/built_in_functions.hpp"

namespace duckdb {

struct VariantUtils {
	DUCKDB_API static void SortVariantKeys(Vector &dictionary, idx_t dictionary_size, SelectionVector &sel,
	                                       idx_t sel_size);
};

} // namespace duckdb
