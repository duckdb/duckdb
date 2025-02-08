//===----------------------------------------------------------------------===//
//                         DuckDB
//
// reader/decimal_column_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "column_reader.hpp"
#include "reader/templated_column_reader.hpp"
#include "parquet_reader.hpp"
#include "parquet_decimal_utils.hpp"

namespace duckdb {

template <class DUCKDB_PHYSICAL_TYPE, bool FIXED_LENGTH>
struct DecimalParquetValueConversion {
	template <bool CHECKED>
	static DUCKDB_PHYSICAL_TYPE PlainRead(ByteBuffer &plain_data, ColumnReader &reader) {
		idx_t byte_len;
		if (FIXED_LENGTH) {
			byte_len = (idx_t)reader.Schema().type_length; /* sure, type length needs to be a signed int */
		} else {
			byte_len = plain_data.read<uint32_t>();
		}
		plain_data.available(byte_len);
		auto res = ParquetDecimalUtils::ReadDecimalValue<DUCKDB_PHYSICAL_TYPE>(const_data_ptr_cast(plain_data.ptr),
		                                                                       byte_len, reader.Schema());

		plain_data.inc(byte_len);
		return res;
	}

	template <bool CHECKED>
	static void PlainSkip(ByteBuffer &plain_data, ColumnReader &reader) {
		uint32_t decimal_len = FIXED_LENGTH ? reader.Schema().type_length : plain_data.read<uint32_t>();
		plain_data.inc(decimal_len);
	}

	static bool PlainAvailable(const ByteBuffer &plain_data, const idx_t count) {
		return true;
	}

	static idx_t PlainConstantSize() {
		return 0;
	}
};

template <class DUCKDB_PHYSICAL_TYPE, bool FIXED_LENGTH>
class DecimalColumnReader
    : public TemplatedColumnReader<DUCKDB_PHYSICAL_TYPE,
                                   DecimalParquetValueConversion<DUCKDB_PHYSICAL_TYPE, FIXED_LENGTH>> {
	using BaseType =
	    TemplatedColumnReader<DUCKDB_PHYSICAL_TYPE, DecimalParquetValueConversion<DUCKDB_PHYSICAL_TYPE, FIXED_LENGTH>>;

public:
	DecimalColumnReader(ParquetReader &reader, LogicalType type_p, const SchemaElement &schema_p, // NOLINT
	                    idx_t file_idx_p, idx_t max_define_p, idx_t max_repeat_p)
	    : TemplatedColumnReader<DUCKDB_PHYSICAL_TYPE,
	                            DecimalParquetValueConversion<DUCKDB_PHYSICAL_TYPE, FIXED_LENGTH>>(
	          reader, std::move(type_p), schema_p, file_idx_p, max_define_p, max_repeat_p) {
	}
};

} // namespace duckdb
