#pragma once

#include "duckdb/common/types/variant_value.hpp"
#include "reader/variant/variant_binary_decoder.hpp"

namespace duckdb {

class VariantShreddedConversion {
public:
	VariantShreddedConversion() = delete;

public:
	static vector<VariantValueIntermediate> Convert(Vector &metadata, Vector &group, idx_t offset, idx_t length,
	                                                idx_t total_size, bool is_field);
	static vector<VariantValueIntermediate> ConvertShreddedLeaf(Vector &metadata, Vector &value, Vector &typed_value,
	                                                            idx_t offset, idx_t length, idx_t total_size,
	                                                            const bool is_field);
	static vector<VariantValueIntermediate> ConvertShreddedArray(Vector &metadata, Vector &value, Vector &typed_value,
	                                                             idx_t offset, idx_t length, idx_t total_size,
	                                                             const bool is_field);
	static vector<VariantValueIntermediate> ConvertShreddedObject(Vector &metadata, Vector &value, Vector &typed_value,
	                                                              idx_t offset, idx_t length, idx_t total_size,
	                                                              const bool is_field);
};

} // namespace duckdb
