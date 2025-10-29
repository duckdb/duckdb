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

struct VariantNestedDataCollectionResult {
public:
	VariantNestedDataCollectionResult() : success(true) {
	}
	explicit VariantNestedDataCollectionResult(VariantLogicalType wrong_type) : success(false), wrong_type(wrong_type) {
	}

public:
	bool success;
	//! If success is false, the type that was encountered that caused the collection failure
	VariantLogicalType wrong_type;
};

struct VariantChildDataCollectionResult {
public:
	enum class Type : uint8_t { SUCCESS, INDEX_ZERO, COMPONENT_NOT_FOUND };

public:
	VariantChildDataCollectionResult() : type(Type::SUCCESS) {
	}

public:
	static VariantChildDataCollectionResult IndexZero() {
		return VariantChildDataCollectionResult(Type::INDEX_ZERO);
	}
	static VariantChildDataCollectionResult NotFound(idx_t nested_index) {
		return VariantChildDataCollectionResult(Type::COMPONENT_NOT_FOUND, nested_index);
	}

public:
	bool Success() const {
		return type == Type::SUCCESS;
	}

private:
	explicit VariantChildDataCollectionResult(Type type, idx_t index = DConstants::INVALID_INDEX)
	    : type(type), nested_data_index(index) {
	}

public:
	Type type;
	idx_t nested_data_index;
};

struct VariantUtils {
	DUCKDB_API static bool IsNestedType(const UnifiedVariantVectorData &variant, idx_t row, uint32_t value_index);
	DUCKDB_API static VariantDecimalData DecodeDecimalData(const UnifiedVariantVectorData &variant, idx_t row,
	                                                       uint32_t value_index);
	DUCKDB_API static VariantNestedData DecodeNestedData(const UnifiedVariantVectorData &variant, idx_t row,
	                                                     uint32_t value_index);
	DUCKDB_API static string_t DecodeStringData(const UnifiedVariantVectorData &variant, idx_t row,
	                                            uint32_t value_index);
	DUCKDB_API static vector<string> GetObjectKeys(const UnifiedVariantVectorData &variant, idx_t row,
	                                               const VariantNestedData &nested_data);
	DUCKDB_API static void FindChildValues(const UnifiedVariantVectorData &variant,
	                                       const VariantPathComponent &component,
	                                       optional_ptr<const SelectionVector> sel, SelectionVector &res,
	                                       ValidityMask &res_validity, VariantNestedData *nested_data, idx_t count);
	DUCKDB_API static VariantNestedDataCollectionResult
	CollectNestedData(const UnifiedVariantVectorData &variant, VariantLogicalType expected_type,
	                  const SelectionVector &sel, idx_t count, optional_idx row, idx_t offset,
	                  VariantNestedData *child_data, ValidityMask &validity);
	DUCKDB_API static vector<uint32_t> ValueIsNull(const UnifiedVariantVectorData &variant, const SelectionVector &sel,
	                                               idx_t count, optional_idx row);
	DUCKDB_API static Value ConvertVariantToValue(const UnifiedVariantVectorData &variant, idx_t row,
	                                              uint32_t values_idx);
	DUCKDB_API static bool Verify(Vector &variant, const SelectionVector &sel_p, idx_t count);
	DUCKDB_API static void FinalizeVariantKeys(Vector &variant, OrderedOwningStringMap<uint32_t> &dictionary,
	                                           SelectionVector &sel, idx_t sel_size);
};

} // namespace duckdb
