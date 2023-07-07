//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parquet_decimal_utils.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "column_reader.hpp"
#include "templated_column_reader.hpp"

namespace duckdb {

class ParquetDecimalUtils {
public:
	template <class PHYSICAL_TYPE>
	static PHYSICAL_TYPE ReadDecimalValue(const_data_ptr_t pointer, idx_t size) {
		D_ASSERT(size <= sizeof(PHYSICAL_TYPE));
		PHYSICAL_TYPE res = 0;

		auto res_ptr = (uint8_t *)&res;
		bool positive = (*pointer & 0x80) == 0;

		// numbers are stored as two's complement so some muckery is required
		for (idx_t i = 0; i < size; i++) {
			auto byte = *(pointer + (size - i - 1));
			res_ptr[i] = positive ? byte : byte ^ 0xFF;
		}
		if (!positive) {
			res += 1;
			return -res;
		}
		return res;
	}

	static unique_ptr<ColumnReader> CreateReader(ParquetReader &reader, const LogicalType &type_p,
	                                             const SchemaElement &schema_p, idx_t file_idx_p, idx_t max_define,
	                                             idx_t max_repeat);
};

} // namespace duckdb
