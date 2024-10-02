//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/compression/alprd/alprd_constants.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

class AlpRDConstants {
public:
	static constexpr uint32_t ALP_VECTOR_SIZE = 1024;

	static constexpr uint8_t MAX_DICTIONARY_BIT_WIDTH = 3;
	static constexpr uint8_t MAX_DICTIONARY_SIZE = (1 << MAX_DICTIONARY_BIT_WIDTH); // 8
	static constexpr uint8_t CUTTING_LIMIT = 16;
	static constexpr uint8_t DICTIONARY_ELEMENT_SIZE = sizeof(uint16_t);
	static constexpr uint8_t MAX_DICTIONARY_SIZE_BYTES = MAX_DICTIONARY_SIZE * DICTIONARY_ELEMENT_SIZE;

	static constexpr uint8_t EXCEPTION_SIZE = sizeof(uint16_t);
	static constexpr uint8_t METADATA_POINTER_SIZE = sizeof(uint32_t);
	static constexpr uint8_t EXCEPTIONS_COUNT_SIZE = sizeof(uint16_t);
	static constexpr uint8_t EXCEPTION_POSITION_SIZE = sizeof(uint16_t);
	static constexpr uint8_t RIGHT_BIT_WIDTH_SIZE = sizeof(uint8_t);
	static constexpr uint8_t LEFT_BIT_WIDTH_SIZE = sizeof(uint8_t);
	static constexpr uint8_t N_DICTIONARY_ELEMENTS_SIZE = sizeof(uint8_t);
	static constexpr uint8_t HEADER_SIZE =
	    METADATA_POINTER_SIZE + RIGHT_BIT_WIDTH_SIZE + LEFT_BIT_WIDTH_SIZE +
	    N_DICTIONARY_ELEMENTS_SIZE; // Pointer to metadata + Right BW + Left BW + Dict Elems
};

} // namespace duckdb
