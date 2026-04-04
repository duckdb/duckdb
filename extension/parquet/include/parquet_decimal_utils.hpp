//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parquet_decimal_utils.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "column_reader.hpp"
#include "reader/templated_column_reader.hpp"

namespace duckdb {

class ParquetDecimalUtils {
public:
	template <class PHYSICAL_TYPE>
	static PHYSICAL_TYPE ReadDecimalValue(const_data_ptr_t pointer, idx_t size, const ParquetColumnSchema &) {
		PHYSICAL_TYPE res = 0;

		auto res_ptr = (uint8_t *)&res;
		bool positive = (*pointer & 0x80) == 0;

		// Numbers are stored as big-endian two's complement.
		// Convert to little-endian by reversing the byte order.
		for (idx_t i = 0; i < MinValue<idx_t>(size, sizeof(PHYSICAL_TYPE)); i++) {
			res_ptr[i] = *(pointer + (size - i - 1));
		}
		// Sign-extend if the encoded size is smaller than the target type
		if (size < sizeof(PHYSICAL_TYPE)) {
			uint8_t sign_byte = positive ? 0x00 : 0xFF;
			for (idx_t i = size; i < sizeof(PHYSICAL_TYPE); i++) {
				res_ptr[i] = sign_byte;
			}
		}
		// Verify that excess bytes are consistent with the sign (no overflow)
		if (size > sizeof(PHYSICAL_TYPE)) {
			uint8_t expected = positive ? 0x00 : 0xFF;
			for (idx_t i = sizeof(PHYSICAL_TYPE); i < size; i++) {
				if (*(pointer + (size - i - 1)) != expected) {
					throw InvalidInputException("Invalid decimal encoding in Parquet file");
				}
			}
		}
		return res;
	}

	static unique_ptr<ColumnReader> CreateReader(const ParquetReader &reader, const ParquetColumnSchema &schema);
};

template <>
double ParquetDecimalUtils::ReadDecimalValue(const_data_ptr_t pointer, idx_t size, const ParquetColumnSchema &schema);

} // namespace duckdb
