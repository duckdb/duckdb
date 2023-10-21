//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/compression/alprd/shared.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

class AlpRDConstants {
public:
	static constexpr uint32_t ALP_VECTOR_SIZE = 1024;
	static constexpr uint32_t RG_SAMPLES_VECTOR_JUMP = 15; // We take every 15 vectors; Assuming row groups of 120K
	static constexpr uint16_t SAMPLES_PER_VECTOR = 64;

	static constexpr uint8_t DICTIONARY_BW = 3;
	static constexpr uint8_t DICTIONARY_SIZE = (1 << DICTIONARY_BW); // 8
	static constexpr uint8_t CUTTING_LIMIT = 16;
	static constexpr uint8_t DICTIONARY_SIZE_BYTES = 16;


	static constexpr uint8_t EXCEPTION_SIZE = sizeof(uint16_t);

	static constexpr uint8_t METADATA_POINTER_SIZE = sizeof(uint32_t);
	static constexpr uint8_t EXCEPTIONS_COUNT_SIZE = sizeof(uint16_t);
	static constexpr uint8_t EXCEPTION_POSITION_SIZE = sizeof(uint16_t);
	static constexpr uint8_t R_BW_SIZE = sizeof(uint8_t);
	static constexpr uint8_t HEADER_SIZE = METADATA_POINTER_SIZE + R_BW_SIZE; // Pointer to metadata + Right BW


};

template <class T>
struct AlpRDPrimitives {};

template <>
struct AlpRDPrimitives<float> {

};

template <>
struct AlpRDPrimitives<double> {

};


} // namespace duckdb