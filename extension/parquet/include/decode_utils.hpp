//===----------------------------------------------------------------------===//
//                         DuckDB
//
// decode_utils.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "resizable_buffer.hpp"

namespace duckdb {

class ParquetDecodeUtils {
public:
	template <class T>
	static T ZigzagToInt(const uint64_t n) {
		return T(n >> 1) ^ -T(n & 1);
	}

	template <class T>
	static uint64_t IntToZigzag(const T n) {
		// TODO
		return n;
	}

	static const uint64_t BITPACK_MASKS[];
	static const uint64_t BITPACK_MASKS_SIZE;
	static const uint8_t BITPACK_DLEN;

	template <class T>
	static void BitPack(const T *src, data_ptr_t &dst, uint8_t &bitpack_pos, const idx_t count, const uint8_t width) {
		D_ASSERT(width < BITPACK_MASKS_SIZE);
		for (idx_t i = 0; i < count; i++) {
			D_ASSERT(src[i] < (1 << width));
			const auto val = src[i];
			// this is kinda nasty but the DbpEncoder uses the same buffer for src/dst
			// this guarantees zero-initialization for bitpacking
			src[i] = 0;
			bitpack_pos += width;
			while (bitpack_pos > BITPACK_DLEN) {
				*dst |= static_cast<data_t>(val >> (bitpack_pos - BITPACK_DLEN));
				bitpack_pos -= BITPACK_DLEN;
			}
		}
	}

	template <class T>
	static uint32_t BitUnpack(ByteBuffer &src, uint8_t &bitpack_pos, const T *dst, const idx_t count,
	                          const uint8_t width) {
		if (width >= BITPACK_MASKS_SIZE) {
			throw InvalidInputException("The width (%d) of the bitpacked data exceeds the supported max width (%d), "
			                            "the file might be corrupted.",
			                            width, BITPACK_MASKS_SIZE);
		}
		const auto mask = BITPACK_MASKS[width];

		src.available(count * width / BITPACK_DLEN); // check if buffer has enough space available once
		for (idx_t i = 0; i < count; i++) {
			T val = (src.unsafe_get<uint8_t>() >> bitpack_pos) & mask;
			bitpack_pos += width;
			while (bitpack_pos > BITPACK_DLEN) {
				src.unsafe_inc(1);
				val |= (static_cast<T>(src.unsafe_get<uint8_t>())
				        << static_cast<T>(BITPACK_DLEN - (bitpack_pos - width))) &
				       mask;
				bitpack_pos -= BITPACK_DLEN;
			}
			dst[i] = val;
		}
		return count;
	}

	template <class T>
	static uint8_t GetVarintSize(T val) {
		uint8_t res = 0;
		do {
			val >>= 7;
			res++;
		} while (val != 0);
		return res;
	}

	template <class T>
	static void VarintEncode(T val, WriteStream &ser) {
		do {
			uint8_t byte = val & 127;
			val >>= 7;
			if (val != 0) {
				byte |= 128;
			}
			ser.Write<uint8_t>(byte);
		} while (val != 0);
	}

	template <class T>
	static T VarintDecode(ByteBuffer &buf) {
		T result = 0;
		uint8_t shift = 0;
		while (true) {
			auto byte = buf.read<uint8_t>();
			result |= T(byte & 127) << shift;
			if ((byte & 128) == 0) {
				break;
			}
			shift += 7;
			if (shift > sizeof(T) * 8) {
				throw std::runtime_error("Varint-decoding found too large number");
			}
		}
		return result;
	}
};
} // namespace duckdb
