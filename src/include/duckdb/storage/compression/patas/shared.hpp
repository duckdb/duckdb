#pragma once

namespace duckdb {

class PatasPrimitives {
public:
	static constexpr uint32_t PATAS_GROUP_SIZE = 1024;
	static constexpr uint8_t HEADER_SIZE = sizeof(uint32_t);
	static constexpr uint8_t BYTECOUNT_BITSIZE = 3;
	static constexpr uint8_t INDEX_BITSIZE = 7;
};

} // namespace duckdb
