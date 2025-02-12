//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parquet_rle_bp_encoder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parquet_types.h"
#include "thrift_tools.hpp"
#include "resizable_buffer.hpp"

namespace duckdb {

class RleBpEncoder {
public:
	explicit RleBpEncoder(uint32_t bit_width)
	    : byte_width((bit_width + 7) / 8), byte_count(idx_t(-1)), run_count(idx_t(-1)) {
	}

public:
	//! NOTE: Prepare is only required if a byte count is required BEFORE writing
	//! This is the case with e.g. writing repetition/definition levels
	//! If GetByteCount() is not required, prepare can be safely skipped
	void BeginPrepare(uint32_t first_value) {
		byte_count = 0;
		run_count = 1;
		current_run_count = 1;
		last_value = first_value;
	}
	void PrepareValue(uint32_t value) {
		if (value != last_value) {
			FinishRun();
			last_value = value;
		} else {
			current_run_count++;
		}
	}
	void FinishPrepare() {
		FinishRun();
	}

	void BeginWrite(WriteStream &writer, uint32_t first_value) {
		// start the RLE runs
		last_value = first_value;
		current_run_count = 1;
	}
	void WriteValue(WriteStream &writer, uint32_t value) {
		if (value != last_value) {
			WriteRun(writer);
			last_value = value;
		} else {
			current_run_count++;
		}
	}
	void FinishWrite(WriteStream &writer) {
		WriteRun(writer);
	}

	idx_t GetByteCount() {
		D_ASSERT(byte_count != idx_t(-1));
		return byte_count;
	}

private:
	//! meta information
	uint32_t byte_width;
	//! RLE run information
	idx_t byte_count;
	idx_t run_count;
	idx_t current_run_count;
	uint32_t last_value;

private:
	void FinishRun() {
		// last value, or value has changed
		// write out the current run
		byte_count += ParquetDecodeUtils::GetVarintSize(current_run_count << 1) + byte_width;
		current_run_count = 1;
		run_count++;
	}
	void WriteRun(WriteStream &writer) {
		// write the header of the run
		ParquetDecodeUtils::VarintEncode(current_run_count << 1, writer);
		// now write the value
		D_ASSERT(last_value >> (byte_width * 8) == 0);
		switch (byte_width) {
		case 1:
			writer.Write<uint8_t>(last_value);
			break;
		case 2:
			writer.Write<uint16_t>(last_value);
			break;
		case 3:
			writer.Write<uint8_t>(last_value & 0xFF);
			writer.Write<uint8_t>((last_value >> 8) & 0xFF);
			writer.Write<uint8_t>((last_value >> 16) & 0xFF);
			break;
		case 4:
			writer.Write<uint32_t>(last_value);
			break;
		default:
			throw InternalException("unsupported byte width for RLE encoding");
		}
		current_run_count = 1;
	}
};

} // namespace duckdb
