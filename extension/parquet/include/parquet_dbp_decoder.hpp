#pragma once
namespace duckdb {
class DbpDecoder {
public:
	DbpDecoder(const uint8_t *buffer, uint32_t buffer_len) : buffer_((char *)buffer, buffer_len) {

		//<block size in values> <number of miniblocks in a block> <total value count> <first value>
		// overall header
		block_value_count = VarintDecode<uint64_t>(buffer_);
		miniblocks_per_block = VarintDecode<uint64_t>(buffer_);
		total_value_count = VarintDecode<uint64_t>(buffer_);
		first_value = zigzagToInt(VarintDecode<int64_t>(buffer_));

		// some derivatives
		values_per_miniblock = block_value_count / miniblocks_per_block;
		miniblock_bit_widths = std::unique_ptr<uint8_t[]>(new data_t[miniblocks_per_block]);

		// init state to something sane
		values_left_in_block = 0;
		values_left_in_miniblock = 0;
		miniblock_offset = 0;
		value_offset = 1;
		min_delta = 0;
		bitpack_pos = 0;
	};

	template <typename T>
	void GetBatch(char *values_target_ptr, uint32_t batch_size) {
		auto values = (T *)values_target_ptr;

		if (batch_size == 0) {
			return;
		}
		values[0] = first_value;

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
				min_delta = zigzagToInt(VarintDecode<uint64_t>(buffer_));
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
			BitUnpack<T>(&values[value_offset], read_now, miniblock_bit_widths[miniblock_offset]);
			for (idx_t i = value_offset; i < value_offset + read_now; i++) {
				values[i] = values[i - 1] + min_delta + values[i];
			}
			value_offset += read_now;
			values_left_in_miniblock -= read_now;
			values_left_in_block -= read_now;
		}

		if (value_offset != batch_size) {
			throw std::runtime_error("DBP decode did not find enough values");
		}
	}

private:
	ByteBuffer buffer_;
	idx_t block_value_count;
	idx_t miniblocks_per_block;
	idx_t total_value_count;
	int64_t first_value;
	idx_t values_per_miniblock;

	// TODO make this re-entrant
	std::unique_ptr<uint8_t[]> miniblock_bit_widths;
	idx_t values_left_in_block;
	idx_t values_left_in_miniblock;
	idx_t miniblock_offset;
	idx_t value_offset;
	int64_t min_delta;

	uint8_t bitpack_pos;

	template <class T>
	T zigzagToInt(const T n) {
		return (n >> 1) ^ -(n & 1);
	}

	static const uint32_t BITPACK_MASKS[];
	static const uint8_t BITPACK_DLEN;

	template <typename T>
	uint32_t BitUnpack(T *dest, uint32_t count, uint8_t width) {
		auto mask = BITPACK_MASKS[width];

		for (uint32_t i = 0; i < count; i++) {
			T val = (buffer_.get<uint8_t>() >> bitpack_pos) & mask;
			bitpack_pos += width;
			while (bitpack_pos > BITPACK_DLEN) {
				buffer_.inc(1);
				val |= (buffer_.get<uint8_t>() << (BITPACK_DLEN - (bitpack_pos - width))) & mask;
				bitpack_pos -= BITPACK_DLEN;
			}
			dest[i] = val;
		}
		return count;
	}

	template <class T>
	T VarintDecode(ByteBuffer &buf) {
		T result = 0;
		uint8_t shift = 0;
		while (true) {
			auto byte = buf.read<uint8_t>();
			result |= (byte & 127) << shift;
			if ((byte & 128) == 0)
				break;
			shift += 7;
			if (shift > sizeof(T) * 8) {
				throw std::runtime_error("Varint-decoding found too large number");
			}
		}
		return result;
	}
};
} // namespace duckdb
