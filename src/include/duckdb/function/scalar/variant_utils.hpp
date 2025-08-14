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
#include "duckdb/common/types/variant.hpp"

namespace duckdb {

struct VariantUtils {
	DUCKDB_API static void SortVariantKeys(Vector &dictionary, idx_t dictionary_size, SelectionVector &sel,
	                                       idx_t sel_size);
	DUCKDB_API static bool FindChildValues(RecursiveUnifiedVectorFormat &source, const VariantPathComponent &component,
	                                       optional_idx row, uint32_t *res, VariantNestedData *nested_data,
	                                       idx_t count);
	DUCKDB_API static bool CollectNestedData(RecursiveUnifiedVectorFormat &variant, VariantLogicalType expected_type,
	                                         uint32_t *value_indices, idx_t count, optional_idx row,
	                                         VariantNestedData *child_data, string &error);
};

} // namespace duckdb
