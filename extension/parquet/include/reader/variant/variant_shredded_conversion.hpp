#pragma once

#include "reader/variant/variant_value.hpp"
#include "reader/variant/variant_binary_decoder.hpp"

namespace duckdb {

class VariantShreddedConversion {
public:
	VariantShreddedConversion() = delete;

public:
	static vector<VariantValue> Convert(Vector &metadata, Vector &value, Vector &typed_value, idx_t count);
};

} // namespace duckdb
