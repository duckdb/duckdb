#pragma once
namespace duckdb {
class RleBpDecoder {
public:
	/// Create a decoder object. buffer/buffer_len is the decoded data.
	/// bit_width is the width of each value (before encoding).
	RleBpDecoder(const uint8_t *buffer, uint32_t buffer_len, uint32_t bit_width)
	    : buffer_((char *)buffer, buffer_len), bit_width_(bit_width), current_value_(0), repeat_count_(0),
	      literal_count_(0) {
		if (bit_width >= 64) {
			throw std::runtime_error("Decode bit width too large");
		}
		byte_encoded_len = ((bit_width_ + 7) / 8);
		max_val = (1 << bit_width_) - 1;
	}

	template <typename T>
	void GetBatch(char *values_target_ptr, uint32_t batch_size) {
		auto values = (T *)values_target_ptr;
		uint32_t values_read = 0;

		while (values_read < batch_size) {
			if (repeat_count_ > 0) {
				int repeat_batch = MinValue(batch_size - values_read, static_cast<uint32_t>(repeat_count_));
				std::fill(values + values_read, values + values_read + repeat_batch, static_cast<T>(current_value_));
				repeat_count_ -= repeat_batch;
				values_read += repeat_batch;
			} else if (literal_count_ > 0) {
				uint32_t literal_batch = MinValue(batch_size - values_read, static_cast<uint32_t>(literal_count_));
				uint32_t actual_read = BitUnpack<T>(values + values_read, literal_batch);
				if (literal_batch != actual_read) {
					throw std::runtime_error("Did not find enough values");
				}
				literal_count_ -= literal_batch;
				values_read += literal_batch;
			} else {
				if (!NextCounts<T>()) {
					if (values_read != batch_size) {
						throw std::runtime_error("RLE decode did not find enough values");
					}
					return;
				}
			}
		}
		if (values_read != batch_size) {
			throw std::runtime_error("RLE decode did not find enough values");
		}
	}

private:
	ByteBuffer buffer_;

	/// Number of bits needed to encode the value. Must be between 0 and 64.
	int bit_width_;
	uint64_t current_value_;
	uint32_t repeat_count_;
	uint32_t literal_count_;
	uint8_t byte_encoded_len;
	uint32_t max_val;

	uint8_t bitpack_pos = 0;

	// this is slow but whatever, calls are rare
	uint32_t VarintDecode() {
		uint32_t result = 0;
		uint8_t shift = 0;
		uint8_t len = 0;
		while (true) {
			auto byte = buffer_.read<uint8_t>();
			len++;
			result |= (byte & 127) << shift;
			if ((byte & 128) == 0)
				break;
			shift += 7;
			if (shift > 32) {
				throw std::runtime_error("Varint-decoding found too large number");
			}
		}
		return result;
	}

	/// Fills literal_count_ and repeat_count_ with next values. Returns false if there
	/// are no more.
	template <typename T>
	bool NextCounts() {
		// Read the next run's indicator int, it could be a literal or repeated run.
		// The int is encoded as a vlq-encoded value.
		if (bitpack_pos != 0) {
			buffer_.inc(1);
			bitpack_pos = 0;
		}
		auto indicator_value = VarintDecode();

		// lsb indicates if it is a literal run or repeated run
		bool is_literal = indicator_value & 1;
		if (is_literal) {
			literal_count_ = (indicator_value >> 1) * 8;
		} else {
			repeat_count_ = indicator_value >> 1;
			// (ARROW-4018) this is not big-endian compatible, lol
			current_value_ = 0;
			for (auto i = 0; i < byte_encoded_len; i++) {
				current_value_ |= (buffer_.read<uint8_t>() << (i * 8));
			}
			// sanity check
			if (repeat_count_ > 0 && current_value_ > max_val) {
				throw std::runtime_error("Payload value bigger than allowed. Corrupted file?");
			}
		}
		// TODO complain if we run out of buffer
		return true;
	}

	// somewhat optimized implementation that avoids non-alignment

	static const uint32_t BITPACK_MASKS[];
	static const uint8_t BITPACK_DLEN;

	template <typename T>
	uint32_t BitUnpack(T *dest, uint32_t count) {
		auto mask = BITPACK_MASKS[bit_width_];

		for (uint32_t i = 0; i < count; i++) {
			T val = (buffer_.get<uint8_t>() >> bitpack_pos) & mask;
			bitpack_pos += bit_width_;
			while (bitpack_pos > BITPACK_DLEN) {
				buffer_.inc(1);
				val |= (buffer_.get<uint8_t>() << (BITPACK_DLEN - (bitpack_pos - bit_width_))) & mask;
				bitpack_pos -= BITPACK_DLEN;
			}
			dest[i] = val;
		}
		return count;
	}
};
} // namespace duckdb
