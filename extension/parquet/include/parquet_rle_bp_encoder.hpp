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
	explicit RleBpEncoder(uint32_t bit_width_p) : bit_width(bit_width_p), byte_width((bit_width + 7) / 8) {
	}

public:
	void BeginWrite() {
		rle_count = 0;
		bp_block_count = 0;
	}

	void WriteValue(WriteStream &writer, const uint32_t &value) {
		if (bp_block_count != 0) {
			// We already committed to a BP run
			D_ASSERT(rle_count == 0);
			bp_block[bp_block_count++] = value;
			if (bp_block_count == BP_BLOCK_SIZE) {
				WriteRun(writer);
			}
			return;
		}

		if (rle_count == 0) {
			// Starting fresh, try for an RLE run first
			rle_value = value;
			rle_count = 1;
			return;
		}

		// We're trying for an RLE run
		if (rle_value == value) {
			// Same as current RLE value
			rle_count++;
			return;
		}

		// Value differs from current RLE value
		if (rle_count >= MINIMUM_RLE_COUNT) {
			// We have enough values for an RLE run
			WriteRun(writer);
			rle_value = value;
			rle_count = 1;
			return;
		}

		// Not enough values, convert and commit to a BP run
		D_ASSERT(bp_block_count == 0);
		for (idx_t i = 0; i < rle_count; i++) {
			bp_block[bp_block_count++] = rle_value;
		}
		bp_block[bp_block_count++] = value;
		rle_count = 0;
	}

	void WriteMany(WriteStream &writer, uint32_t value, idx_t count) {
		if (rle_count != 0) {
			// If an RLE run is going on, write a single value to either finish it or convert to BP
			WriteValue(writer, value);
			count--;
		}

		if (bp_block_count != 0) {
			// If a BP run is going on, finish it
			while (bp_block_count != 0 && count > 0) {
				WriteValue(writer, value);
				count--;
			}
		}

		// Set remaining as current RLE run
		rle_value = value;
		rle_count += count;
	}

	void FinishWrite(WriteStream &writer) {
		WriteRun(writer);
	}

private:
	//! Meta information
	uint32_t bit_width;
	uint32_t byte_width;

	//! RLE stuff
	static constexpr idx_t MINIMUM_RLE_COUNT = 4;
	uint32_t rle_value;
	idx_t rle_count;

	//! BP stuff
	static constexpr idx_t BP_BLOCK_SIZE = 256;
	static_assert(BP_BLOCK_SIZE % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE == 0,
	              "BP_BLOCK_SIZE must be divisible by BITPACKING_ALGORITHM_GROUP_SIZE");
	uint32_t bp_block[BP_BLOCK_SIZE] = {0};
	uint32_t bp_block_packed[BP_BLOCK_SIZE] = {0};
	idx_t bp_block_count;

private:
	void WriteRun(WriteStream &writer) {
		if (rle_count != 0) {
			WriteCurrentBlockRLE(writer);
		} else {
			WriteCurrentBlockBP(writer);
		}
	}

	void WriteCurrentBlockRLE(WriteStream &writer) {
		ParquetDecodeUtils::VarintEncode(rle_count << 1 | 0, writer); // (... | 0) signals RLE run
		D_ASSERT(rle_value >> (byte_width * 8) == 0);
		switch (byte_width) {
		case 1:
			writer.Write<uint8_t>(rle_value);
			break;
		case 2:
			writer.Write<uint16_t>(rle_value);
			break;
		case 3:
			writer.Write<uint8_t>(rle_value & 0xFF);
			writer.Write<uint8_t>((rle_value >> 8) & 0xFF);
			writer.Write<uint8_t>((rle_value >> 16) & 0xFF);
			break;
		case 4:
			writer.Write<uint32_t>(rle_value);
			break;
		default:
			throw InternalException("unsupported byte width for RLE encoding");
		}
		rle_count = 0;
	}

	void WriteCurrentBlockBP(WriteStream &writer) {
		ParquetDecodeUtils::VarintEncode(BP_BLOCK_SIZE / 8 << 1 | 1, writer); // (... | 1) signals BP run
		ParquetDecodeUtils::BitPackAligned(bp_block, data_ptr_cast(bp_block_packed), BP_BLOCK_SIZE, bit_width);
		writer.WriteData(data_ptr_cast(bp_block_packed), BP_BLOCK_SIZE * bit_width / 8);
		bp_block_count = 0;
	}
};

} // namespace duckdb
