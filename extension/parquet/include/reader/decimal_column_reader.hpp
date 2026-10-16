//===----------------------------------------------------------------------===//
//                         DuckDB
//
// reader/decimal_column_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <stdint.h>

#include "column_reader.hpp"
#include "reader/templated_column_reader.hpp"
#include "parquet_reader.hpp"
#include "parquet_decimal_utils.hpp"
#include "duckdb/common/typedefs.hpp"
#include "parquet_column_schema.hpp"
#include "resizable_buffer.hpp"

namespace duckdb {
class ParquetReader;

template <class DUCKDB_PHYSICAL_TYPE, bool FIXED_LENGTH>
struct DecimalParquetValueConversion {
	template <bool CHECKED>
	static DUCKDB_PHYSICAL_TYPE PlainRead(ByteBuffer &plain_data, ColumnReader &reader) {
		idx_t byte_len;
		if (FIXED_LENGTH) {
			byte_len = reader.Schema().type_length;
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

//! Wide decimals (width > 38) are stored as BIGNUM blobs: the two's complement bytes are converted while reading and
//! the result is allocated in the vector that is being filled
class WideDecimalColumnReader;

template <bool FIXED_LENGTH>
struct WideDecimalParquetValueConversion {
	template <bool CHECKED>
	static string_t PlainRead(ByteBuffer &plain_data, ColumnReader &reader);

	template <bool CHECKED>
	static void PlainSkip(ByteBuffer &plain_data, ColumnReader &reader) {
		uint32_t decimal_len = FIXED_LENGTH ? reader.Schema().type_length : plain_data.read<uint32_t>();
		plain_data.inc(decimal_len);
	}

	static bool PlainAvailable(const ByteBuffer &plain_data, const idx_t count) {
		return false;
	}

	static idx_t PlainConstantSize() {
		return 0;
	}
};

class WideDecimalColumnReader : public ColumnReader {
public:
	static constexpr const PhysicalType TYPE = PhysicalType::VARCHAR;

public:
	WideDecimalColumnReader(const ParquetReader &reader, const ParquetColumnSchema &schema)
	    : ColumnReader(reader, schema),
	      fixed_length(schema.parquet_type == duckdb_parquet::Type::FIXED_LEN_BYTE_ARRAY) {
	}

	//! The vector the plain values are currently read into, so the conversion can allocate the blobs in it
	Vector *current_result = nullptr;
	bool fixed_length;

public:
	void Plain(ByteBuffer &plain_data, uint8_t *defines, idx_t num_values, idx_t result_offset,
	           Vector &result) override {
		current_result = &result;
		if (fixed_length) {
			PlainTemplated<string_t, WideDecimalParquetValueConversion<true>>(plain_data, defines, num_values,
			                                                                  result_offset, result);
		} else {
			PlainTemplated<string_t, WideDecimalParquetValueConversion<false>>(plain_data, defines, num_values,
			                                                                   result_offset, result);
		}
		current_result = nullptr;
	}

	void PlainSkip(ByteBuffer &plain_data, uint8_t *defines, idx_t num_values) override {
		if (fixed_length) {
			PlainSkipTemplated<WideDecimalParquetValueConversion<true>>(plain_data, defines, num_values);
		} else {
			PlainSkipTemplated<WideDecimalParquetValueConversion<false>>(plain_data, defines, num_values);
		}
	}
};

template <bool FIXED_LENGTH>
template <bool CHECKED>
string_t WideDecimalParquetValueConversion<FIXED_LENGTH>::PlainRead(ByteBuffer &plain_data, ColumnReader &reader) {
	idx_t byte_len;
	if (FIXED_LENGTH) {
		byte_len = reader.Schema().type_length;
	} else {
		byte_len = plain_data.read<uint32_t>();
	}
	plain_data.available(byte_len);
	auto blob = ParquetDecimalUtils::ReadWideDecimalValue(const_data_ptr_cast(plain_data.ptr), byte_len);
	plain_data.inc(byte_len);
	auto &wide_reader = reader.Cast<WideDecimalColumnReader>();
	D_ASSERT(wide_reader.current_result);
	return StringVector::AddStringOrBlob(*wide_reader.current_result, blob.data(), blob.size());
}

template <class DUCKDB_PHYSICAL_TYPE, bool FIXED_LENGTH>
class DecimalColumnReader
    : public TemplatedColumnReader<DUCKDB_PHYSICAL_TYPE,
                                   DecimalParquetValueConversion<DUCKDB_PHYSICAL_TYPE, FIXED_LENGTH>> {
	using BaseType =
	    TemplatedColumnReader<DUCKDB_PHYSICAL_TYPE, DecimalParquetValueConversion<DUCKDB_PHYSICAL_TYPE, FIXED_LENGTH>>;

public:
	DecimalColumnReader(const ParquetReader &reader, const ParquetColumnSchema &schema)
	    : TemplatedColumnReader<DUCKDB_PHYSICAL_TYPE,
	                            DecimalParquetValueConversion<DUCKDB_PHYSICAL_TYPE, FIXED_LENGTH>>(reader, schema) {
	}
};

} // namespace duckdb
