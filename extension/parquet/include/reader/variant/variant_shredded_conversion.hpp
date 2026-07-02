#pragma once

#include "duckdb/common/types/variant_value.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "reader/variant/variant_binary_decoder.hpp"

namespace duckdb {

class VariantShreddedConversion {
public:
	VariantShreddedConversion() = delete;

public:
	static vector<VariantValue> Convert(Vector &metadata, Vector &group, idx_t offset, idx_t length, idx_t total_size);
	static void ConvertBinaryToVariant(Vector &metadata_and_value, idx_t offset, idx_t length, idx_t total_size,
	                                   Vector &result);
	static vector<VariantValue> ConvertShreddedLeaf(Vector &metadata, Vector &value, Vector &typed_value, idx_t offset,
	                                                idx_t length, idx_t total_size);
	static vector<VariantValue> ConvertShreddedArray(Vector &metadata, Vector &value, Vector &typed_value, idx_t offset,
	                                                 idx_t length, idx_t total_size);
	static vector<VariantValue> ConvertShreddedObject(Vector &metadata, Vector &value, Vector &typed_value,
	                                                  idx_t offset, idx_t length, idx_t total_size);
	//! Inverse of GetTransformFunction: decode a binary Variant value (metadata followed by value) into a VARIANT.
	static ScalarFunction GetBytesToVariantFunction();
};

} // namespace duckdb
