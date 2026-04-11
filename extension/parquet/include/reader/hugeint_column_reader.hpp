//===----------------------------------------------------------------------===//
//                         DuckDB
//
// reader/hugeint_column_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "column_reader.hpp"
#include "templated_column_reader.hpp"
#include "duckdb/common/bswap.hpp"
#include "duckdb/common/types/uhugeint.hpp"

namespace duckdb {

static constexpr idx_t PARQUET_HUGEINT_SIZE = 16;

//===--------------------------------------------------------------------===//
// HUGEINT: order-preserving encoding (big-endian + sign-bit flip)
//===--------------------------------------------------------------------===//
struct HugeintValueConversion {
	static hugeint_t Decode(const_data_ptr_t input) {
		// Read 16 bytes, unflip sign bit, byte-swap from big-endian to native
		uint64_t high, low;
		memcpy(&high, input, sizeof(uint64_t));
		memcpy(&low, input + sizeof(uint64_t), sizeof(uint64_t));
		// Unflip the sign bit
		auto high_bytes = reinterpret_cast<uint8_t *>(&high);
		high_bytes[0] ^= 0x80;
		hugeint_t result;
		result.upper = static_cast<int64_t>(BSWAP64(high));
		result.lower = BSWAP64(low);
		return result;
	}

	template <bool CHECKED>
	static hugeint_t PlainRead(ByteBuffer &plain_data, ColumnReader &reader) {
		if (CHECKED) {
			plain_data.available(PARQUET_HUGEINT_SIZE);
		}
		auto res = Decode(const_data_ptr_cast(plain_data.ptr));
		plain_data.unsafe_inc(PARQUET_HUGEINT_SIZE);
		return res;
	}

	template <bool CHECKED>
	static void PlainSkip(ByteBuffer &plain_data, ColumnReader &reader) {
		if (CHECKED) {
			plain_data.inc(PARQUET_HUGEINT_SIZE);
		} else {
			plain_data.unsafe_inc(PARQUET_HUGEINT_SIZE);
		}
	}

	static bool PlainAvailable(const ByteBuffer &plain_data, const idx_t count) {
		return plain_data.check_available(count * PARQUET_HUGEINT_SIZE);
	}

	static idx_t PlainConstantSize() {
		return 0;
	}
};

class HugeintColumnReader : public TemplatedColumnReader<hugeint_t, HugeintValueConversion> {
public:
	HugeintColumnReader(const ParquetReader &reader, const ParquetColumnSchema &schema)
	    : TemplatedColumnReader<hugeint_t, HugeintValueConversion>(reader, schema) {
	}
};

//===--------------------------------------------------------------------===//
// UHUGEINT: big-endian unsigned encoding
//===--------------------------------------------------------------------===//
struct UhugeintValueConversion {
	static uhugeint_t Decode(const_data_ptr_t input) {
		uint64_t high, low;
		memcpy(&high, input, sizeof(uint64_t));
		memcpy(&low, input + sizeof(uint64_t), sizeof(uint64_t));
		uhugeint_t result;
		result.upper = BSWAP64(high);
		result.lower = BSWAP64(low);
		return result;
	}

	template <bool CHECKED>
	static uhugeint_t PlainRead(ByteBuffer &plain_data, ColumnReader &reader) {
		if (CHECKED) {
			plain_data.available(PARQUET_HUGEINT_SIZE);
		}
		auto res = Decode(const_data_ptr_cast(plain_data.ptr));
		plain_data.unsafe_inc(PARQUET_HUGEINT_SIZE);
		return res;
	}

	template <bool CHECKED>
	static void PlainSkip(ByteBuffer &plain_data, ColumnReader &reader) {
		if (CHECKED) {
			plain_data.inc(PARQUET_HUGEINT_SIZE);
		} else {
			plain_data.unsafe_inc(PARQUET_HUGEINT_SIZE);
		}
	}

	static bool PlainAvailable(const ByteBuffer &plain_data, const idx_t count) {
		return plain_data.check_available(count * PARQUET_HUGEINT_SIZE);
	}

	static idx_t PlainConstantSize() {
		return 0;
	}
};

class UhugeintColumnReader : public TemplatedColumnReader<uhugeint_t, UhugeintValueConversion> {
public:
	UhugeintColumnReader(const ParquetReader &reader, const ParquetColumnSchema &schema)
	    : TemplatedColumnReader<uhugeint_t, UhugeintValueConversion>(reader, schema) {
	}
};

} // namespace duckdb
