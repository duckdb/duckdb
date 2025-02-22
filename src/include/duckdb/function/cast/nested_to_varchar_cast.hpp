#pragma once

#include "duckdb/common/typedefs.hpp"

namespace duckdb {

struct NestedToVarcharCast {
public:
	static constexpr const uint8_t LIST_VALUE = 1 << 0;
	static constexpr const uint8_t STRUCT_KEY = 1 << 1;
	static constexpr const uint8_t STRUCT_VALUE = 1 << 2;
	static constexpr const uint8_t MAP_KEY = 1 << 3;
	static constexpr const uint8_t MAP_VALUE = 1 << 4;
	static constexpr const uint8_t ALL = LIST_VALUE | STRUCT_KEY | STRUCT_VALUE | MAP_VALUE | MAP_KEY;

	static const uint8_t LookupTable[256];
};

} // namespace duckdb
