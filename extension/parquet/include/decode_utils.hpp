//===----------------------------------------------------------------------===//
//                         DuckDB
//
// decode_utils.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/bitpacking.hpp"
#include "resizable_buffer.hpp"

namespace duckdb {

class ParquetDecodeUtils {
	//===--------------------------------------------------------------------===//
	// Bitpacking
	//===--------------------------------------------------------------------===//
private:
	static const uint64_t BITPACK_MASKS[];
	static const uint64_t BITPACK_MASKS_SIZE;
	static const uint8_t BITPACK_DLEN;

	static void CheckWidth(const uint8_t width) {
		if (width >= BITPACK_MASKS_SIZE) {
			throw InvalidInputException("The width (%d) of the bitpacked data exceeds the supported max width (%d), "
			                            "the file might be corrupted.",
			                            width, BITPACK_MASKS_SIZE);
		}
	}

public:
	template <class T>
	static uint32_t BitUnpack(ByteBuffer &src, bitpacking_width_t &bitpack_pos, T *dst, const idx_t count,
	                          const bitpacking_width_t width) {
		CheckWidth(width);
		const auto mask = BITPACK_MASKS[width];
		src.available(count * width / BITPACK_DLEN); // check if buffer has enough space available once
		for (idx_t i = 0; i < count; i++) {
			auto val = (src.unsafe_get<uint8_t>() >> bitpack_pos) & mask;
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
	static void BitPackAligned(T *src, data_ptr_t dst, const idx_t count, const bitpacking_width_t width) {
		D_ASSERT(width < BITPACK_MASKS_SIZE);
		D_ASSERT(count % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE == 0);
		BitpackingPrimitives::PackBuffer<T, true>(dst, src, count, width);
	}

	template <class T>
	static void BitUnpackAligned(ByteBuffer &src, T *dst, const idx_t count, const bitpacking_width_t width) {
		CheckWidth(width);
		if (count % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE != 0) {
			throw InvalidInputException("Aligned bitpacking count must be a multiple of %llu",
			                            BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE);
		}
		const auto read_size = count * width / BITPACK_DLEN;
		src.available(read_size); // check if buffer has enough space available once
		for (idx_t i = 0; i < count; i += BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE) {
			// Buffer for alignment
			T aligned_data[BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE];

			// Copy over to aligned buffer
			const auto next_read = BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE * width / 8;
			memcpy(aligned_data, src.ptr, next_read);
			src.unsafe_inc(next_read);

			// Unpack
			BitpackingPrimitives::UnPackBlock<T>(data_ptr_cast(dst), data_ptr_cast(aligned_data), width);
		}
	}

	//===--------------------------------------------------------------------===//
	// Zigzag
	//===--------------------------------------------------------------------===//
private:
	//! https://lemire.me/blog/2022/11/25/making-all-your-integers-positive-with-zigzag-encoding/
	template <class UNSIGNED>
	static typename std::enable_if<std::is_unsigned<UNSIGNED>::value, typename std::make_signed<UNSIGNED>::type>::type
	ZigzagToIntInternal(UNSIGNED x) {
		return (x >> 1) ^ (-(x & 1));
	}

	template <typename SIGNED>
	static typename std::enable_if<std::is_signed<SIGNED>::value, typename std::make_unsigned<SIGNED>::type>::type
	IntToZigzagInternal(SIGNED x) {
		using UNSIGNED = typename std::make_unsigned<SIGNED>::type;
		return (static_cast<UNSIGNED>(x) << 1) ^ static_cast<UNSIGNED>(x >> (sizeof(SIGNED) * 8 - 1));
	}

public:
	template <class UNSIGNED>
	static typename std::enable_if<std::is_unsigned<UNSIGNED>::value, typename std::make_signed<UNSIGNED>::type>::type
	ZigzagToInt(UNSIGNED x) {
		auto integer = ZigzagToIntInternal(x);
		D_ASSERT(x == IntToZigzagInternal(integer)); // test roundtrip
		return integer;
	}

	template <typename SIGNED>
	static typename std::enable_if<std::is_signed<SIGNED>::value, typename std::make_unsigned<SIGNED>::type>::type
	IntToZigzag(SIGNED x) {
		auto zigzag = IntToZigzagInternal(x);
		D_ASSERT(x == ZigzagToIntInternal(zigzag)); // test roundtrip
		return zigzag;
	}

	//===--------------------------------------------------------------------===//
	// Varint
	//===--------------------------------------------------------------------===//
public:
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
