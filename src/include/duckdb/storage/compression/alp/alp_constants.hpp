//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/compression/alp/alp_constants.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/limits.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

class AlpConstants {
public:
	static constexpr uint32_t ALP_VECTOR_SIZE = 1024;
	static constexpr uint32_t RG_SAMPLES = 8;
	static constexpr uint16_t SAMPLES_PER_VECTOR = 32;
	// We calculate how many equidistant vector we must jump within a rowgroup
	static constexpr uint32_t RG_SAMPLES_DUCKDB_JUMP = (DEFAULT_ROW_GROUP_SIZE / RG_SAMPLES) / STANDARD_VECTOR_SIZE;

	static constexpr uint8_t HEADER_SIZE = sizeof(uint32_t);
	static constexpr uint8_t EXPONENT_SIZE = sizeof(uint8_t);
	static constexpr uint8_t FACTOR_SIZE = sizeof(uint8_t);
	static constexpr uint8_t EXCEPTIONS_COUNT_SIZE = sizeof(uint16_t);
	static constexpr uint8_t EXCEPTION_POSITION_SIZE = sizeof(uint16_t);
	static constexpr uint8_t FOR_SIZE = sizeof(uint64_t);
	static constexpr uint8_t BIT_WIDTH_SIZE = sizeof(uint8_t);
	static constexpr uint8_t METADATA_POINTER_SIZE = sizeof(uint32_t);

	static constexpr uint8_t SAMPLING_EARLY_EXIT_THRESHOLD = 2;

	static constexpr double COMPACT_BLOCK_THRESHOLD = 0.80;

	// Largest double which fits into an int64
	static constexpr double ENCODING_UPPER_LIMIT = 9223372036854774784.0;
	static constexpr double ENCODING_LOWER_LIMIT = -9223372036854774784.0;

	static constexpr uint8_t MAX_COMBINATIONS = 5;

	static constexpr const int64_t FACT_ARR[] = {1,
	                                             10,
	                                             100,
	                                             1000,
	                                             10000,
	                                             100000,
	                                             1000000,
	                                             10000000,
	                                             100000000,
	                                             1000000000,
	                                             10000000000,
	                                             100000000000,
	                                             1000000000000,
	                                             10000000000000,
	                                             100000000000000,
	                                             1000000000000000,
	                                             10000000000000000,
	                                             100000000000000000,
	                                             1000000000000000000};
};

template <class T>
struct AlpTypedConstants {};

template <>
struct AlpTypedConstants<float> {
	static constexpr float MAGIC_NUMBER = 12582912.0; //! 2^22 + 2^23
	static constexpr uint8_t MAX_EXPONENT = 10;

	static constexpr const float EXP_ARR[] = {1.0F,         10.0F,         100.0F,        1000.0F,
	                                          10000.0F,     100000.0F,     1000000.0F,    10000000.0F,
	                                          100000000.0F, 1000000000.0F, 10000000000.0F};

	static constexpr float FRAC_ARR[] = {1.0F,      0.1F,       0.01F,       0.001F,       0.0001F,      0.00001F,
	                                     0.000001F, 0.0000001F, 0.00000001F, 0.000000001F, 0.0000000001F};
};

template <>
struct AlpTypedConstants<double> {
	static constexpr double MAGIC_NUMBER = 6755399441055744.0; //! 2^51 + 2^52
	static constexpr uint8_t MAX_EXPONENT = 18;                //! 10^18 is the maximum int64

	static constexpr const double EXP_ARR[] = {1.0,
	                                           10.0,
	                                           100.0,
	                                           1000.0,
	                                           10000.0,
	                                           100000.0,
	                                           1000000.0,
	                                           10000000.0,
	                                           100000000.0,
	                                           1000000000.0,
	                                           10000000000.0,
	                                           100000000000.0,
	                                           1000000000000.0,
	                                           10000000000000.0,
	                                           100000000000000.0,
	                                           1000000000000000.0,
	                                           10000000000000000.0,
	                                           100000000000000000.0,
	                                           1000000000000000000.0,
	                                           10000000000000000000.0,
	                                           100000000000000000000.0,
	                                           1000000000000000000000.0,
	                                           10000000000000000000000.0,
	                                           100000000000000000000000.0};

	static constexpr double FRAC_ARR[] = {1.0,
	                                      0.1,
	                                      0.01,
	                                      0.001,
	                                      0.0001,
	                                      0.00001,
	                                      0.000001,
	                                      0.0000001,
	                                      0.00000001,
	                                      0.000000001,
	                                      0.0000000001,
	                                      0.00000000001,
	                                      0.000000000001,
	                                      0.0000000000001,
	                                      0.00000000000001,
	                                      0.000000000000001,
	                                      0.0000000000000001,
	                                      0.00000000000000001,
	                                      0.000000000000000001,
	                                      0.0000000000000000001,
	                                      0.00000000000000000001};
};

} // namespace duckdb
