//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/compression/alp/shared.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

class AlpConstants {
public:
	static constexpr uint32_t ALP_VECTOR_SIZE = 1024;
	static constexpr uint32_t RG_SAMPLES_VECTOR_JUMP = 15; // We take every 15 vectors; Assuming row groups of 120K
	static constexpr uint16_t SAMPLES_PER_VECTOR = 64;
	static constexpr uint8_t HEADER_SIZE = sizeof(uint32_t);
	static constexpr uint8_t EXCEPTION_POSITION_SIZE = sizeof(uint16_t);
	static constexpr double MAX_COMBINATIONS = 5;

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

	static constexpr const int64_t U_FACT_ARR[] = {1,
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
struct AlpPrimitives {};

template <>
struct AlpPrimitives<float> {

	static constexpr float MAGIC_NUMBER = 12582912.0; //! 2^22 + 2^23
	static constexpr double MAX_EXPONENT = 9;

	static constexpr const float EXP_ARR[] = {
	    1.0,
	    10.0,
	    100.0,
	    1000.0,
	    10000.0,
	    100000.0,
	    1000000.0,
	    10000000.0,
	    100000000.0,
	    1000000000.0,
	    10000000000.0
	};

	static constexpr float FRAC_ARR[] = {
	    1.0,
	    0.1,
	    0.01,
	    0.001,
	    0.0001,
	    0.00001,
	    0.000001,
	    0.0000001,
	    0.00000001,
	    0.000000001,
	    0.0000000001
	};
};

template <>
struct AlpPrimitives<double> {

	static constexpr double MAGIC_NUMBER = 6755399441055744.0; //! 2^51 + 2^52
	static constexpr double MAX_EXPONENT = 18;

	static constexpr const double EXP_ARR[] = {
	    1.0,
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
	    100000000000000000000000.0
	};

	static constexpr double FRAC_ARR[] = {
	    1.0,
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
	    0.00000000000000000001
	};
};


} // namespace duckdb