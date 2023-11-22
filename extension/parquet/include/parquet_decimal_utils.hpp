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
	static PHYSICAL_TYPE ReadDecimalValueInternal(const_data_ptr_t pointer, idx_t size, bool positive) {
		// numbers are stored as two's complement so some muckery is required
		PHYSICAL_TYPE res = 0;
		auto res_ptr = (uint8_t *)&res;
		for (idx_t i = 0; i < size; i++) {
			auto byte = *(pointer + (size - i - 1));
			res_ptr[i] = positive ? byte : byte ^ 0xFF;
		}
		return res;
	}

	template <class PHYSICAL_TYPE>
	static PHYSICAL_TYPE ReadDecimalValue(const_data_ptr_t pointer, idx_t size,
	                                      const duckdb_parquet::format::SchemaElement &schema_ele) {
		D_ASSERT(size <= sizeof(PHYSICAL_TYPE));

		bool positive = (*pointer & 0x80) == 0;
		auto res = ReadDecimalValueInternal<PHYSICAL_TYPE>(pointer, size, positive);
		if (!positive) {
			res += 1;
			return -res;
		} else {
			return res;
		}
	}

	static unique_ptr<ColumnReader> CreateReader(ParquetReader &reader, const LogicalType &type_p,
	                                             const SchemaElement &schema_p, idx_t file_idx_p, idx_t max_define,
	                                             idx_t max_repeat);
};

template <>
double ParquetDecimalUtils::ReadDecimalValue(const_data_ptr_t pointer, idx_t size,
                                             const duckdb_parquet::format::SchemaElement &schema_ele);

} // namespace duckdb
