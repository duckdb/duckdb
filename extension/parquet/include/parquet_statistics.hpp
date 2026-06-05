//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parquet_statistics.hpp
//
//
//===----------------------------------------------------------------------===/

#pragma once

#include "duckdb.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "parquet_types.h"
#include "resizable_buffer.hpp"

namespace duckdb {

using duckdb_parquet::ColumnChunk;
using duckdb_parquet::SchemaElement;

struct LogicalType;
struct ParquetColumnSchema;
class ResizeableBuffer;

struct ParquetStatisticsUtils {
	static unique_ptr<BaseStatistics> TransformColumnStatistics(const ParquetColumnSchema &reader,
	                                                            const vector<ColumnChunk> &columns, bool can_have_nan);

	static Value ConvertValue(const LogicalType &type, const ParquetColumnSchema &schema_ele, const std::string &stats);

	static bool BloomFilterSupported(const LogicalTypeId &type_id);

	static bool BloomFilterExcludes(const TableFilter &filter, const duckdb_parquet::ColumnMetaData &column_meta_data,
	                                duckdb_apache::thrift::protocol::TProtocol &file_proto, Allocator &allocator);

	static unique_ptr<BaseStatistics> CreateNumericStats(const LogicalType &type, const ParquetColumnSchema &schema_ele,
	                                                     const duckdb_parquet::Statistics &parquet_stats);

private:
	static Value ConvertValueInternal(const LogicalType &type, const ParquetColumnSchema &schema_ele,
	                                  const std::string &stats);
};

class ParquetBloomFilter {
	static constexpr const idx_t DEFAULT_BLOCK_COUNT = 32; // 4k filter

public:
	ParquetBloomFilter(idx_t num_entries, double bloom_filter_false_positive_ratio);
	ParquetBloomFilter(unique_ptr<ResizeableBuffer> data_p);
	void FilterInsert(uint64_t x);
	bool FilterCheck(uint64_t x);
	void Shrink(idx_t new_block_count);
	double OneRatio();
	ResizeableBuffer *Get();

private:
	unique_ptr<ResizeableBuffer> data;
	idx_t block_count;
};

// see https://github.com/apache/parquet-format/blob/master/BloomFilter.md

struct ParquetBloomBlock {
	struct ParquetBloomMaskResult {
		uint8_t bit_set[8] = {0};
	};

	uint32_t block[8] = {0};

	static bool check_bit(uint32_t &x, const uint8_t i) {
		D_ASSERT(i < 32);
		return (x >> i) & (uint32_t)1;
	}

	static void set_bit(uint32_t &x, const uint8_t i) {
		D_ASSERT(i < 32);
		x |= (uint32_t)1 << i;
		D_ASSERT(check_bit(x, i));
	}

	static ParquetBloomMaskResult Mask(uint32_t x) {
		static const uint32_t parquet_bloom_salt[8] = {0x47b6137bU, 0x44974d91U, 0x8824ad5bU, 0xa2b7289dU,
		                                               0x705495c7U, 0x2df1424bU, 0x9efc4947U, 0x5c6bfb31U};
		ParquetBloomMaskResult result;
		for (idx_t i = 0; i < 8; i++) {
			result.bit_set[i] = (x * parquet_bloom_salt[i]) >> 27;
		}
		return result;
	}

	static void BlockInsert(ParquetBloomBlock &b, uint32_t x) {
		auto masked = Mask(x);
		for (idx_t i = 0; i < 8; i++) {
			set_bit(b.block[i], masked.bit_set[i]);
			D_ASSERT(check_bit(b.block[i], masked.bit_set[i]));
		}
	}

	static bool BlockCheck(ParquetBloomBlock &b, uint32_t x) {
		auto masked = Mask(x);
		for (idx_t i = 0; i < 8; i++) {
			if (!check_bit(b.block[i], masked.bit_set[i])) {
				return false;
			}
		}
		return true;
	}
};

} // namespace duckdb
