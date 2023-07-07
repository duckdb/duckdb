#pragma once
#include "decode_utils.hpp"

namespace duckdb {
class DbpDecoder {
public:
	DbpDecoder(data_ptr_t buffer, uint32_t buffer_len) : buffer_(buffer, buffer_len) {
		//<block size in values> <number of miniblocks in a block> <total value count> <first value>
		// overall header
		block_value_count = ParquetDecodeUtils::VarintDecode<uint64_t>(buffer_);
		miniblocks_per_block = ParquetDecodeUtils::VarintDecode<uint64_t>(buffer_);
		total_value_count = ParquetDecodeUtils::VarintDecode<uint64_t>(buffer_);
		start_value = ParquetDecodeUtils::ZigzagToInt(ParquetDecodeUtils::VarintDecode<int64_t>(buffer_));

		// some derivatives
		D_ASSERT(miniblocks_per_block > 0);
		values_per_miniblock = block_value_count / miniblocks_per_block;
		miniblock_bit_widths = unique_ptr<uint8_t[]>(new data_t[miniblocks_per_block]);

		// init state to something sane
		values_left_in_block = 0;
		values_left_in_miniblock = 0;
		miniblock_offset = 0;
		min_delta = 0;
		bitpack_pos = 0;
		is_first_value = true;
	};

	ByteBuffer BufferPtr() {
		if (bitpack_pos != 0) {
			buffer_.inc(1);
			bitpack_pos = 0;
		}
		return buffer_;
	}

	template <typename T>
	void GetBatch(data_ptr_t values_target_ptr, uint32_t batch_size) {
		auto values = reinterpret_cast<T *>(values_target_ptr);

		if (batch_size == 0) {
			return;
		}
		idx_t value_offset = 0;

		if (is_first_value) {
			values[0] = start_value;
			value_offset++;
			is_first_value = false;
		}

		if (total_value_count == 1) { // I guess it's a special case
			if (batch_size > 1) {
				throw std::runtime_error("DBP decode did not find enough values (have 1)");
			}
			return;
		}

		while (value_offset < batch_size) {
			if (values_left_in_block == 0) { // need to open new block
				if (bitpack_pos > 0) {       // have to eat the leftovers if any
					buffer_.inc(1);
				}
				min_delta = ParquetDecodeUtils::ZigzagToInt(ParquetDecodeUtils::VarintDecode<uint64_t>(buffer_));
				for (idx_t miniblock_idx = 0; miniblock_idx < miniblocks_per_block; miniblock_idx++) {
					miniblock_bit_widths[miniblock_idx] = buffer_.read<uint8_t>();
					// TODO what happens if width is 0?
				}
				values_left_in_block = block_value_count;
				miniblock_offset = 0;
				bitpack_pos = 0;
				values_left_in_miniblock = values_per_miniblock;
			}
			if (values_left_in_miniblock == 0) {
				miniblock_offset++;
				values_left_in_miniblock = values_per_miniblock;
			}

			auto read_now = MinValue(values_left_in_miniblock, (idx_t)batch_size - value_offset);
			ParquetDecodeUtils::BitUnpack<T>(buffer_, bitpack_pos, &values[value_offset], read_now,
			                                 miniblock_bit_widths[miniblock_offset]);
			for (idx_t i = value_offset; i < value_offset + read_now; i++) {
				values[i] = ((i == 0) ? start_value : values[i - 1]) + min_delta + values[i];
			}
			value_offset += read_now;
			values_left_in_miniblock -= read_now;
			values_left_in_block -= read_now;
		}

		if (value_offset != batch_size) {
			throw std::runtime_error("DBP decode did not find enough values");
		}
		start_value = values[batch_size - 1];
	}
	void Finalize() {
		if (values_left_in_miniblock == 0) {
			return;
		}
		auto data = unique_ptr<uint32_t[]>(new uint32_t[values_left_in_miniblock]);
		GetBatch<uint32_t>(data_ptr_cast(data.get()), values_left_in_miniblock);
	}

	uint64_t TotalValues() {
		return total_value_count;
	}

private:
	ByteBuffer buffer_;
	idx_t block_value_count;
	idx_t miniblocks_per_block;
	idx_t total_value_count;
	int64_t start_value;
	idx_t values_per_miniblock;

	unique_ptr<uint8_t[]> miniblock_bit_widths;
	idx_t values_left_in_block;
	idx_t values_left_in_miniblock;
	idx_t miniblock_offset;
	int64_t min_delta;

	bool is_first_value;

	uint8_t bitpack_pos;
};
} // namespace duckdb
