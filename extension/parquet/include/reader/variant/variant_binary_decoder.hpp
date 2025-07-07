#pragma once

#include "duckdb/common/types/string_type.hpp"

namespace duckdb {

class VariantBinaryDecoder {
public:
	VariantBinaryDecoder();

public:
	string_t Decode(const string_t &metadata, const string_t &blob);
};

} // namespace duckdb
