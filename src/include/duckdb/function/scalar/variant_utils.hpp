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
#include "duckdb/common/owning_string_map.hpp"

namespace duckdb {

struct VariantUtils {
	DUCKDB_API static bool IsNestedType(const UnifiedVariantVectorData &variant, idx_t row, uint32_t value_index);
	DUCKDB_API static VariantDecimalData DecodeDecimalData(const UnifiedVariantVectorData &variant, idx_t row,
	                                                       uint32_t value_index);
	DUCKDB_API static VariantNestedData DecodeNestedData(const UnifiedVariantVectorData &variant, idx_t row,
	                                                     uint32_t value_index);
	DUCKDB_API static bool FindChildValues(const UnifiedVariantVectorData &variant,
	                                       const VariantPathComponent &component, optional_idx row,
	                                       SelectionVector &res, VariantNestedData *nested_data, idx_t count);
	DUCKDB_API static bool CollectNestedData(const UnifiedVariantVectorData &variant, VariantLogicalType expected_type,
	                                         const SelectionVector &sel, idx_t count, optional_idx row, idx_t offset,
	                                         VariantNestedData *child_data, ValidityMask &validity, string &error);
	DUCKDB_API static vector<uint32_t> ValueIsNull(const UnifiedVariantVectorData &variant, const SelectionVector &sel,
	                                               idx_t count, optional_idx row);
	DUCKDB_API static Value ConvertVariantToValue(const UnifiedVariantVectorData &variant, idx_t row, idx_t values_idx);
	DUCKDB_API static bool Verify(Vector &variant, const SelectionVector &sel_p, idx_t count);
};

} // namespace duckdb
