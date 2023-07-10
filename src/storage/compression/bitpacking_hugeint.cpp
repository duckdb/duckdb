#include "duckdb/common/bitpacking.hpp"
#include "duckdb/common/bitpacking_hugeint.hpp"

#include <iostream>

namespace duckdb {
// ! TEMPORARY STREAM OVERLOAD
std::ostream& operator<<(std::ostream& stream, hugeint_t value) {
	stream << value.ToString();
	return stream;
}

static void UnpackSingle(const uint32_t *__restrict &in, hugeint_t *__restrict out, uint16_t delta, uint16_t shr) {
	if (delta + shr < 32) {
		*out = ((static_cast<hugeint_t>(*in)) >> shr) % (hugeint_t(1) << delta);
	}

	else if (delta + shr >= 32 && delta + shr < 64) {
		*out = static_cast<hugeint_t>(*in) >> shr;
		++in;

		if (delta + shr > 32) {
			const uint16_t NEXT_SHR = shr + delta - 32;
		
			*out |= static_cast<hugeint_t>((*in) % (1U << NEXT_SHR)) << (32 - shr);
		}
	}
	
	else if (delta + shr >= 64 && delta + shr < 96) {
		*out = static_cast<hugeint_t>(*in) >> shr;
		++in;

		*out |= static_cast<hugeint_t>(*in) << (32 - shr);
		++in;

		if (delta + shr > 64) {
			const uint16_t NEXT_SHR = delta + shr - 64;
			*out |= static_cast<hugeint_t>((*in) % (1U << NEXT_SHR)) << (64 - shr);
		}
	}

	else if (delta + shr >= 96 && delta + shr < 128) {
		*out = static_cast<hugeint_t>(*in) >> shr;
		++in;

		*out |= static_cast<hugeint_t>(*in) << (32 - shr);
		++in;

		*out |= static_cast<hugeint_t>(*in) << (64 - shr);
		++in;

		if (delta + shr > 96) {
			const uint16_t NEXT_SHR = delta + shr - 96;
			*out |= static_cast<hugeint_t>((*in) % (1U << NEXT_SHR)) << (96 - shr);
		}
	}

	else if (delta + shr >= 128) {
		*out = static_cast<hugeint_t>(*in) >> shr;
		++in;

		*out |= static_cast<hugeint_t>(*in) << (32 - shr);
		++in;

		*out |= static_cast<hugeint_t>(*in) << (64 - shr);
		++in;

		*out |= static_cast<hugeint_t>(*in) << (96 - shr);
		++in;

		if (delta + shr > 128) {
			const uint16_t NEXT_SHR = delta + shr - 128;
			*out |= static_cast<hugeint_t>((*in) % (1U << NEXT_SHR)) << (128 - shr);
		}
	}

}


static void PackSingle(const hugeint_t in, uint32_t *__restrict &out, uint16_t delta, uint16_t shl, hugeint_t mask) {
	if (delta + shl < 32) {

		if (shl == 0) {
			*out = static_cast<uint32_t>(in & mask);
		} else {
			*out |= static_cast<uint32_t>((in & mask) << shl);
		}

	}
	else if  (delta + shl >= 32 && delta + shl < 64) {
	
		if (shl == 0) {
			*out = static_cast<uint32_t>(in & mask);
		} else {
			*out |= static_cast<uint32_t>((in & mask) << shl);
		}

		++out;

		if (delta + shl > 32) {
			*out = static_cast<uint32_t>((in & mask) >> (32 - shl));
		}

	}

	else if (delta + shl >= 64 && delta + shl < 96) {

		if (shl == 0) {
			*out = static_cast<uint32_t>(in & mask);
		} else {
			*out |= static_cast<uint32_t>(in << shl);
		}
		++out;

		*out = static_cast<uint32_t>((in & mask) >> (32 - shl));
		++out;

		if (delta + shl > 64) {
			*out = static_cast<uint32_t>((in & mask) >> (64 - shl));
		}
	}

	else if (delta + shl >= 96 && delta + shl < 128) {
		if (shl == 0) {
			*out = static_cast<uint32_t>(in & mask);
		} else {
			*out |= static_cast<uint32_t>(in << shl);
		}
		++out;

		*out = static_cast<uint32_t>((in & mask) >> (32 - shl));
		++out;

		*out = static_cast<uint32_t>((in & mask) >> (64 - shl));
		++out;

		if (delta + shl > 96) {
			*out = static_cast<uint32_t>((in & mask) >> (96 - shl));
		}
	}

	else if (delta + shl >= 128) {
		if (shl == 0) {
			*out = static_cast<uint32_t>(in & mask);
		} else {
			*out |= static_cast<uint32_t>(in << shl);
		}
		++out;

		*out = static_cast<uint32_t>((in & mask) >> (32 - shl));
		++out;

		*out = static_cast<uint32_t>((in & mask) >> (64 - shl));
		++out;

		*out = static_cast<uint32_t>((in & mask) >> (96 - shl));
		++out;

		if (delta + shl > 128) {
			*out = static_cast<uint32_t>((in & mask) >> (128 - shl));
		}
	}

}

// Custom packing for hugeints
// DELTA = width
// static void UnpackSingle(const uint32_t *__restrict &in, hugeint_t *__restrict out, uint16_t delta, uint16_t oindex) {

// 	std::cout << "Unpacking... with DELTA: " << (uint32_t)delta << ", SHR: "
// 		<< (uint32_t)((delta * oindex) % 32) << ", DELTA+SHR: " << (uint32_t)(delta + (delta * oindex) % 32) << std::endl;

// 	UnpackSingle(in, out + oindex, delta, (delta * oindex) % 32);
// }

// static void PackSingle(const hugeint_t *__restrict in, uint32_t *__restrict &out, uint16_t delta, uint16_t oindex) {

// 	std::cout << "Packing " << in[oindex] << " with DELTA: " << (uint32_t)delta << ", SHL: "
// 		<< (uint32_t)((delta * oindex) % 32) << ", MASK: " << ((hugeint_t(1) << delta) - 1) << ", DELTA+SHL: " << (uint32_t)(delta + (delta * oindex) % 32) << std::endl;

// 	PackSingle(in[oindex], out, delta, (delta * oindex) % 32, (hugeint_t(1) << delta) - 1);
// }

static void UnpackLast(const uint32_t *__restrict &in, hugeint_t *__restrict out, uint16_t delta) {
	uint16_t shift = (delta * 31) % 32;
	out[31] = (*in) >> shift;
	if (delta > 32) {
		++in;
		out[31] |= static_cast<hugeint_t>(*in) << (32 - shift);
	}
	if (delta > 64) {
		++in;
		out[31] |= static_cast<hugeint_t>(*in) << (64 - shift);
	}
	if (delta > 96) {
		++in;
		out[31] |= static_cast<hugeint_t>(*in) << (96 - shift);
	}
}

static void PackLast(const hugeint_t *__restrict in, uint32_t *__restrict out, uint16_t delta) {
	uint16_t shift = (delta * 31) % 32;
	*out |= static_cast<uint32_t>(in[31] << shift);
	if (delta > 32) {
		++out;
		*out = static_cast<uint32_t>(in[31] >> (32 - shift));
	}
	if (delta > 64) {
		++out;
		*out = static_cast<uint32_t>(in[31] >> (64 - shift));
	}
	if (delta > 96) {
		++out;
		*out = static_cast<uint32_t>(in[31] >> (96 - shift));
	}

}

static void PackDelta32(const hugeint_t *__restrict in, uint32_t *__restrict out) {
	for (uint8_t i = 0; i < 32; ++i) {
		out[i] = static_cast<uint32_t>(in[i]);
	}
}

static void PackDelta64(const hugeint_t *__restrict in, uint32_t *__restrict out) {
	for (uint8_t i = 0; i < 32; ++i) {
		out[2 * i] = static_cast<uint32_t>(in[i]);
		out[2 * i + 1] = static_cast<uint32_t>(in[i] >> 32);
	}
}

static void PackDelta96(const hugeint_t *__restrict in, uint32_t *__restrict out) {
	for (uint8_t i = 0; i < 32; ++i) {
		out[3 * i] = static_cast<uint32_t>(in[i]);
		out[3 * i + 1] = static_cast<uint32_t>(in[i] >> 32);
		out[3 * i + 2] = static_cast<uint32_t>(in[i] >> 64);
	}
}

static void PackDelta128(const hugeint_t *__restrict in, uint32_t *__restrict out) {
	for (uint8_t i = 0; i < 32; ++i) {
		out[4 * i] = static_cast<uint32_t>(in[i]);
		out[4 * i + 1] = static_cast<uint32_t>(in[i] >> 32);
		out[4 * i + 2] = static_cast<uint32_t>(in[i] >> 64);
		out[4 * i + 3] = static_cast<uint32_t>(in[i] >> 96);
	}
}

static void UnpackDelta0(const uint32_t *__restrict in, hugeint_t *__restrict out) {
	for (uint8_t i = 0; i < 32; ++i) {
		*(out++) = 0;
	}
}

static void UnpackDelta32(const uint32_t *__restrict in, hugeint_t *__restrict out) {
	for (uint8_t k = 0; k < 32; ++k) {
		out[k] = in[k];
	}
}

static void UnpackDelta64(const uint32_t *__restrict in, hugeint_t *__restrict out) {
	for (uint8_t k = 0; k < 32; ++k) {
		out[k] = in[k * 2];
		out[k] |= hugeint_t(in[k * 2 + 1]) << 32;
	}
}

static void UnpackDelta96(const uint32_t *__restrict in, hugeint_t *__restrict out) {
	for (uint8_t k = 0; k < 32; ++k) {
		out[k] = in[k * 3];
		out[k] |= hugeint_t(in[k * 3 + 1]) << 32;
		out[k] |= hugeint_t(in[k * 3 + 2]) << 64;
	}
}

static void UnpackDelta128(const uint32_t *__restrict in, hugeint_t *__restrict out) {
	for (uint8_t k = 0; k < 32; ++k) {
		out[k] = in[k * 4];
		out[k] |= hugeint_t(static_cast<uint64_t>(in[k * 4 + 1])) << 32;
		out[k] |= hugeint_t(static_cast<uint64_t>(in[k * 4 + 2])) << 64;
		out[k] |= hugeint_t(static_cast<uint64_t>(in[k * 4 + 3])) << 96;
	}
}

void HugeIntPacker::Pack(const hugeint_t *__restrict in, uint32_t *__restrict out, bitpacking_width_t width) {
	// std::cout << "packing with WIDTH: " << (uint32_t)width << std::endl;
	
	switch (width) {
		case 0: return ;
		case 32: PackDelta32(in, out); return ;
		case 64: PackDelta64(in, out); return ;
		case 96: PackDelta96(in, out); return ;
		case 128: PackDelta128(in, out); return ;
		default: break ;
	}	
	for (idx_t oindex = 0; oindex < BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE - 1; ++oindex) {

		// std::cout << "Packing " << in[oindex] << " with DELTA: " << (uint32_t)width << ", SHL: "
		// << (uint32_t)((width * oindex) % 32) << ", MASK: " << ((hugeint_t(1) << width) - 1) << ", width+SHL: " << (uint32_t)(width + (width * oindex) % 32) << std::endl;

		PackSingle(in[oindex], out, width, (width * oindex) % 32, (hugeint_t(1) << width) - 1);

		// std::cout << "Packed " << in[oindex] << std::endl; // STREAM OVERLOAD

	}
	PackLast(in, out, width);
}

void HugeIntPacker::Unpack(const uint32_t *__restrict in, hugeint_t *__restrict out, bitpacking_width_t width) {
	// std::cout << "unpacking with WIDTH: " << (uint32_t)width << std::endl;
	
	switch (width) {
		case 0: UnpackDelta0(in, out); return ;
		case 32: UnpackDelta32(in, out); return ;
		case 64: UnpackDelta64(in, out); return ;
		case 96: UnpackDelta96(in, out); return ;
		case 128: UnpackDelta128(in, out); return ;
		default: break ;
	}
	for (idx_t oindex = 0; oindex < BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE - 1; ++oindex) {
		UnpackSingle(in, out + oindex, width, (width * oindex) % 32);

		// std::cout << "Unpacked " << out[oindex] << std::endl;

	}
	UnpackLast(in, out, width);
}

} // namespace duckdb
