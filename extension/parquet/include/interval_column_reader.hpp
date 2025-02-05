//===----------------------------------------------------------------------===//
//                         DuckDB
//
// interval_column_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "column_reader.hpp"
#include "templated_column_reader.hpp"
#include "parquet_reader.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Interval Column Reader
//===--------------------------------------------------------------------===//
struct IntervalValueConversion {
	static constexpr const idx_t PARQUET_INTERVAL_SIZE = 12;

	static interval_t ReadParquetInterval(const_data_ptr_t input) {
		interval_t result;
		result.months = Load<int32_t>(input);
		result.days = Load<int32_t>(input + sizeof(uint32_t));
		result.micros = int64_t(Load<uint32_t>(input + sizeof(uint32_t) * 2)) * 1000;
		return result;
	}

	static interval_t PlainRead(ByteBuffer &plain_data, ColumnReader &reader) {
		plain_data.available(PARQUET_INTERVAL_SIZE);
		return UnsafePlainRead(plain_data, reader);
	}

	static void PlainSkip(ByteBuffer &plain_data, ColumnReader &reader) {
		plain_data.inc(PARQUET_INTERVAL_SIZE);
	}

	static bool PlainAvailable(const ByteBuffer &plain_data, const idx_t count) {
		return plain_data.check_available(count * PARQUET_INTERVAL_SIZE);
	}

	static interval_t UnsafePlainRead(ByteBuffer &plain_data, ColumnReader &reader) {
		auto res = ReadParquetInterval(const_data_ptr_cast(plain_data.ptr));
		plain_data.unsafe_inc(PARQUET_INTERVAL_SIZE);
		return res;
	}

	static void UnsafePlainSkip(ByteBuffer &plain_data, ColumnReader &reader) {
		plain_data.unsafe_inc(PARQUET_INTERVAL_SIZE);
	}
};

class IntervalColumnReader : public TemplatedColumnReader<interval_t, IntervalValueConversion> {

public:
	IntervalColumnReader(ParquetReader &reader, LogicalType type_p, const SchemaElement &schema_p, idx_t file_idx_p,
						 idx_t max_define_p, idx_t max_repeat_p)
		: TemplatedColumnReader<interval_t, IntervalValueConversion>(reader, std::move(type_p), schema_p, file_idx_p,
																	 max_define_p, max_repeat_p) {};
};

} // namespace duckdb
