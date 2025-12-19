//===----------------------------------------------------------------------===//
//                         DuckDB
//
// decoder/delta_byte_array_decoder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "parquet_dbp_decoder.hpp"
#include "resizable_buffer.hpp"

namespace duckdb {
class ColumnReader;

class DeltaByteArrayDecoder {
public:
	explicit DeltaByteArrayDecoder(ColumnReader &reader);

public:
	void InitializePage();

	void Read(uint8_t *defines, idx_t read_count, Vector &result, idx_t result_offset);
	void Skip(uint8_t *defines, idx_t skip_count);

	static void ReadDbpData(Allocator &allocator, ResizeableBuffer &buffer, ResizeableBuffer &result_buffer,
	                        idx_t &value_count);

private:
	//! Initialize for variable-length byte arrays (strings/blobs)
	void InitializeVariableLength(idx_t value_count, uint32_t *prefix_data, uint32_t *suffix_data);
	//! Initialize for fixed-length byte arrays (UUIDs, fixed-len decimals, etc.)
	void InitializeFixedLength(idx_t value_count, uint32_t *prefix_data, uint32_t *suffix_data, idx_t fixed_length);

	//! Read for variable-length mode
	void ReadVariableLength(uint8_t *defines, idx_t read_count, Vector &result, idx_t result_offset);
	//! Read for fixed-length mode
	void ReadFixedLength(uint8_t *defines, idx_t read_count, Vector &result, idx_t result_offset);

private:
	ColumnReader &reader;

	//! Variable-length mode: stores decoded string values
	unique_ptr<Vector> byte_array_data;

	//! Fixed-length mode: stores decoded fixed-length byte arrays
	ResizeableBuffer fixed_byte_buffer;
	//! Temporary buffer for building valid-only buffer in ReadFixedLength
	ResizeableBuffer temp_buffer;
	//! Whether we are in fixed-length mode
	bool is_fixed_length_mode = false;
	//! The fixed length of each value (only valid when is_fixed_length_mode is true)
	idx_t fixed_length = 0;

	//! Shared state: number of decoded values
	idx_t byte_array_count = 0;
	//! Shared state: current read offset into decoded values
	idx_t delta_offset = 0;
};

} // namespace duckdb
