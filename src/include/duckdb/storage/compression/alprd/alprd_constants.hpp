//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/compression/alprd/alprd_constants.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/limits.hpp"

namespace duckdb {

class AlpRDConstants {
public:
	using METADATA_POINTER_TYPE = uint32_t;
	using BIT_WIDTH_TYPE = uint8_t;
	using DICTIONARY_COUNT_TYPE = uint8_t;
	using DICTIONARY_ELEMENT_TYPE = uint16_t;
	using EXCEPTION_TYPE = uint16_t;
	using EXCEPTIONS_COUNT_TYPE = uint16_t;
	using EXCEPTION_POSITION_TYPE = uint16_t;

	static constexpr uint32_t ALP_VECTOR_SIZE = 1024;

	static constexpr BIT_WIDTH_TYPE MAX_DICTIONARY_BIT_WIDTH = 3;
	static constexpr DICTIONARY_COUNT_TYPE MAX_DICTIONARY_SIZE = (1 << MAX_DICTIONARY_BIT_WIDTH); // 8
	static constexpr uint8_t CUTTING_LIMIT = 16;
	static constexpr uint8_t DICTIONARY_ELEMENT_SIZE = sizeof(DICTIONARY_ELEMENT_TYPE);
	static constexpr uint8_t MAX_DICTIONARY_SIZE_BYTES = MAX_DICTIONARY_SIZE * DICTIONARY_ELEMENT_SIZE;

	static constexpr uint8_t EXCEPTION_SIZE = sizeof(EXCEPTION_TYPE);
	static constexpr uint8_t METADATA_POINTER_SIZE = sizeof(METADATA_POINTER_TYPE);
	//! exceptions_count can store the UNCOMPRESSED_MODE_SENTINEL value
	static constexpr uint8_t EXCEPTIONS_COUNT_SIZE = sizeof(EXCEPTIONS_COUNT_TYPE);
	static constexpr EXCEPTIONS_COUNT_TYPE UNCOMPRESSED_MODE_SENTINEL =
	    std::numeric_limits<EXCEPTIONS_COUNT_TYPE>::max();
	static constexpr uint8_t EXCEPTION_POSITION_SIZE = sizeof(EXCEPTION_POSITION_TYPE);
	static constexpr uint8_t RIGHT_BIT_WIDTH_SIZE = sizeof(BIT_WIDTH_TYPE);
	static constexpr uint8_t LEFT_BIT_WIDTH_SIZE = sizeof(BIT_WIDTH_TYPE);
	static constexpr uint8_t N_DICTIONARY_ELEMENTS_SIZE = sizeof(DICTIONARY_COUNT_TYPE);
	static constexpr uint8_t HEADER_SIZE =
	    METADATA_POINTER_SIZE + RIGHT_BIT_WIDTH_SIZE + LEFT_BIT_WIDTH_SIZE +
	    N_DICTIONARY_ELEMENTS_SIZE; // Pointer to metadata + Right BW + Left BW + Dict Elems
};

} // namespace duckdb
