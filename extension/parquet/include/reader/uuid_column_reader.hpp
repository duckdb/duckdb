//===----------------------------------------------------------------------===//
//                         DuckDB
//
// reader/uuid_column_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "column_reader.hpp"
#include "templated_column_reader.hpp"
#include "parquet_reader.hpp"

namespace duckdb {

struct UUIDValueConversion {
	static hugeint_t ReadParquetUUID(const_data_ptr_t input) {
		hugeint_t result;
		result.lower = 0;
		uint64_t unsigned_upper = 0;
		for (idx_t i = 0; i < sizeof(uint64_t); i++) {
			unsigned_upper <<= 8;
			unsigned_upper += input[i];
		}
		for (idx_t i = sizeof(uint64_t); i < sizeof(hugeint_t); i++) {
			result.lower <<= 8;
			result.lower += input[i];
		}
		result.upper = static_cast<int64_t>(unsigned_upper ^ (uint64_t(1) << 63));
		return result;
	}

	static hugeint_t PlainRead(ByteBuffer &plain_data, ColumnReader &reader) {
		plain_data.available(sizeof(hugeint_t));
		return UnsafePlainRead(plain_data, reader);
	}

	static void PlainSkip(ByteBuffer &plain_data, ColumnReader &reader) {
		plain_data.inc(sizeof(hugeint_t));
	}

	static bool PlainAvailable(const ByteBuffer &plain_data, const idx_t count) {
		return plain_data.check_available(count * sizeof(hugeint_t));
	}

	static hugeint_t UnsafePlainRead(ByteBuffer &plain_data, ColumnReader &reader) {
		auto res = ReadParquetUUID(const_data_ptr_cast(plain_data.ptr));
		plain_data.unsafe_inc(sizeof(hugeint_t));
		return res;
	}

	static void UnsafePlainSkip(ByteBuffer &plain_data, ColumnReader &reader) {
		plain_data.unsafe_inc(sizeof(hugeint_t));
	}
};

class UUIDColumnReader : public TemplatedColumnReader<hugeint_t, UUIDValueConversion> {

public:
	UUIDColumnReader(ParquetReader &reader, LogicalType type_p, const SchemaElement &schema_p, idx_t file_idx_p,
	                 idx_t max_define_p, idx_t max_repeat_p)
	    : TemplatedColumnReader<hugeint_t, UUIDValueConversion>(reader, std::move(type_p), schema_p, file_idx_p,
	                                                            max_define_p, max_repeat_p) {};
};

} // namespace duckdb
