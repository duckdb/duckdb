//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parquet_rle_bp_encoder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "decode_utils.hpp"

namespace duckdb {

class RleBpEncoder {
public:
	explicit RleBpEncoder(uint32_t bit_width);

public:
	void BeginWrite();
	void WriteValue(WriteStream &writer, uint32_t value);
	void FinishWrite(WriteStream &writer);

private:
	//! Meta information
	uint32_t bit_width;
	uint32_t byte_width;

	//! RLE stuff
	static constexpr idx_t MINIMUM_RLE_COUNT = 4;
	uint32_t rle_value;
	idx_t rle_count;

	//! BP stuff
	static constexpr idx_t BP_BLOCK_SIZE = BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;
	uint32_t bp_block[BP_BLOCK_SIZE] = {0};
	uint32_t bp_block_packed[BP_BLOCK_SIZE] = {0};
	idx_t bp_block_count;

private:
	void WriteRun(WriteStream &writer);
	void WriteCurrentBlockRLE(WriteStream &writer);
	void WriteCurrentBlockBP(WriteStream &writer);
};

} // namespace duckdb
