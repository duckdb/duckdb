#include "bitpacking.h"

#include <cstdint>
#include <type_traits>

namespace duckdb_fastpforlib {
namespace internal {

// Used for uint8_t, uint16_t and uint32_t
template <uint8_t DELTA, uint8_t SHR, class TYPE, uint8_t TYPE_SIZE = sizeof(TYPE) * 8>
typename std::enable_if<(DELTA + SHR) < TYPE_SIZE>::type unpack_single_out(const TYPE *__restrict in,
                                                                           TYPE *__restrict out) {
	*out = ((*in) >> SHR) % (1 << DELTA);
}

// Used for uint8_t, uint16_t and uint32_t
template <uint8_t DELTA, uint8_t SHR, class TYPE, uint8_t TYPE_SIZE = sizeof(TYPE) * 8>
typename std::enable_if<(DELTA + SHR) >= TYPE_SIZE>::type unpack_single_out(const TYPE *__restrict &in,
                                                                            TYPE *__restrict out) {
	*out = (*in) >> SHR;
	++in;

	static const TYPE NEXT_SHR = SHR + DELTA - TYPE_SIZE;
	*out |= ((*in) % (1U << NEXT_SHR)) << (TYPE_SIZE - SHR);
}

template <uint8_t DELTA, uint8_t SHR>
typename std::enable_if<(DELTA + SHR) < 32>::type unpack_single_out(const uint32_t *__restrict in,
                                                                    uint64_t *__restrict out) {
	*out = ((static_cast<uint64_t>(*in)) >> SHR) % (1ULL << DELTA);
}

template <uint8_t DELTA, uint8_t SHR>
typename std::enable_if<(DELTA + SHR) >= 32 && (DELTA + SHR) < 64>::type
unpack_single_out(const uint32_t *__restrict &in, uint64_t *__restrict out) {
	*out = static_cast<uint64_t>(*in) >> SHR;
	++in;
	if (DELTA + SHR > 32) {
		static const uint8_t NEXT_SHR = SHR + DELTA - 32;
		*out |= static_cast<uint64_t>((*in) % (1U << NEXT_SHR)) << (32 - SHR);
	}
}

template <uint8_t DELTA, uint8_t SHR>
typename std::enable_if<(DELTA + SHR) >= 64>::type unpack_single_out(const uint32_t *__restrict &in,
                                                                     uint64_t *__restrict out) {
	*out = static_cast<uint64_t>(*in) >> SHR;
	++in;

	*out |= static_cast<uint64_t>(*in) << (32 - SHR);
	++in;

	if (DELTA + SHR > 64) {
		static const uint8_t NEXT_SHR = DELTA + SHR - 64;
		*out |= static_cast<uint64_t>((*in) % (1U << NEXT_SHR)) << (64 - SHR);
	}
}

// Used for uint8_t, uint16_t and uint32_t
template <class TYPE, uint16_t DELTA, uint16_t SHL, TYPE MASK, uint8_t TYPE_SIZE = sizeof(TYPE) * 8>
    typename std::enable_if < DELTA + SHL<TYPE_SIZE>::type pack_single_in(const TYPE in, TYPE *__restrict out) {
	if (SHL == 0) {
		*out = in & MASK;
	} else {
		*out |= (in & MASK) << SHL;
	}
}

// Used for uint8_t, uint16_t and uint32_t
template <class TYPE, uint16_t DELTA, uint16_t SHL, TYPE MASK, uint8_t TYPE_SIZE = sizeof(TYPE) * 8>
typename std::enable_if<DELTA + SHL >= TYPE_SIZE>::type pack_single_in(const TYPE in, TYPE *__restrict &out) {
	*out |= in << SHL;
	++out;

	if (DELTA + SHL > TYPE_SIZE) {
		*out = (in & MASK) >> (TYPE_SIZE - SHL);
	}
}

template <uint16_t DELTA, uint16_t SHL, uint64_t MASK>
    typename std::enable_if < DELTA + SHL<32>::type pack_single_in64(const uint64_t in, uint32_t *__restrict out) {
	if (SHL == 0) {
		*out = static_cast<uint32_t>(in & MASK);
	} else {
		*out |= (in & MASK) << SHL;
	}
}
template <uint16_t DELTA, uint16_t SHL, uint64_t MASK>
        typename std::enable_if < DELTA + SHL >= 32 &&
    DELTA + SHL<64>::type pack_single_in64(const uint64_t in, uint32_t *__restrict &out) {
	if (SHL == 0) {
		*out = static_cast<uint32_t>(in & MASK);
	} else {
		*out |= (in & MASK) << SHL;
	}

	++out;

	if (DELTA + SHL > 32) {
		*out = static_cast<uint32_t>((in & MASK) >> (32 - SHL));
	}
}
template <uint16_t DELTA, uint16_t SHL, uint64_t MASK>
typename std::enable_if<DELTA + SHL >= 64>::type pack_single_in64(const uint64_t in, uint32_t *__restrict &out) {
	*out |= in << SHL;
	++out;

	*out = static_cast<uint32_t>((in & MASK) >> (32 - SHL));
	++out;

	if (DELTA + SHL > 64) {
		*out = (in & MASK) >> (64 - SHL);
	}
}
template <uint16_t DELTA, uint16_t OINDEX = 0>
struct Unroller8 {
	static void Unpack(const uint8_t *__restrict &in, uint8_t *__restrict out) {
		unpack_single_out<DELTA, (DELTA * OINDEX) % 8>(in, out + OINDEX);

		Unroller8<DELTA, OINDEX + 1>::Unpack(in, out);
	}

	static void Pack(const uint8_t *__restrict in, uint8_t *__restrict out) {
		pack_single_in<uint8_t, DELTA, (DELTA * OINDEX) % 8, (1U << DELTA) - 1>(in[OINDEX], out);

		Unroller8<DELTA, OINDEX + 1>::Pack(in, out);
	}

};\
template <uint16_t DELTA>
struct Unroller8<DELTA, 7> {
	enum { SHIFT = (DELTA * 7) % 8 };

	static void Unpack(const uint8_t *__restrict in, uint8_t *__restrict out) {
		out[7] = (*in) >> SHIFT;
	}

	static void Pack(const uint8_t *__restrict in, uint8_t *__restrict out) {
		*out |= (in[7] << SHIFT);
	}
};

template <uint16_t DELTA, uint16_t OINDEX = 0>
struct Unroller16 {
	static void Unpack(const uint16_t *__restrict &in, uint16_t *__restrict out) {
		unpack_single_out<DELTA, (DELTA * OINDEX) % 16>(in, out + OINDEX);

		Unroller16<DELTA, OINDEX + 1>::Unpack(in, out);
	}

	static void Pack(const uint16_t *__restrict in, uint16_t *__restrict out) {
		pack_single_in<uint16_t, DELTA, (DELTA * OINDEX) % 16, (1U << DELTA) - 1>(in[OINDEX], out);

		Unroller16<DELTA, OINDEX + 1>::Pack(in, out);
	}

};

template <uint16_t DELTA>
struct Unroller16<DELTA, 15> {
	enum { SHIFT = (DELTA * 15) % 16 };

	static void Unpack(const uint16_t *__restrict in, uint16_t *__restrict out) {
		out[15] = (*in) >> SHIFT;
	}

	static void Pack(const uint16_t *__restrict in, uint16_t *__restrict out) {
		*out |= (in[15] << SHIFT);
	}
};

template <uint16_t DELTA, uint16_t OINDEX = 0>
struct Unroller {
	static void Unpack(const uint32_t *__restrict &in, uint32_t *__restrict out) {
		unpack_single_out<DELTA, (DELTA * OINDEX) % 32>(in, out + OINDEX);

		Unroller<DELTA, OINDEX + 1>::Unpack(in, out);
	}

	static void Unpack(const uint32_t *__restrict &in, uint64_t *__restrict out) {
		unpack_single_out<DELTA, (DELTA * OINDEX) % 32>(in, out + OINDEX);

		Unroller<DELTA, OINDEX + 1>::Unpack(in, out);
	}

	static void Pack(const uint32_t *__restrict in, uint32_t *__restrict out) {
		pack_single_in<uint32_t, DELTA, (DELTA * OINDEX) % 32, (1U << DELTA) - 1>(in[OINDEX], out);

		Unroller<DELTA, OINDEX + 1>::Pack(in, out);
	}

	static void Pack(const uint64_t *__restrict in, uint32_t *__restrict out) {
		pack_single_in64<DELTA, (DELTA * OINDEX) % 32, (1ULL << DELTA) - 1>(in[OINDEX], out);

		Unroller<DELTA, OINDEX + 1>::Pack(in, out);
	}
};

template <uint16_t DELTA>
struct Unroller<DELTA, 31> {
	enum { SHIFT = (DELTA * 31) % 32 };

	static void Unpack(const uint32_t *__restrict in, uint32_t *__restrict out) {
		out[31] = (*in) >> SHIFT;
	}

	static void Unpack(const uint32_t *__restrict in, uint64_t *__restrict out) {
		out[31] = (*in) >> SHIFT;
		if (DELTA > 32) {
			++in;
			out[31] |= static_cast<uint64_t>(*in) << (32 - SHIFT);
		}
	}

	static void Pack(const uint32_t *__restrict in, uint32_t *__restrict out) {
		*out |= (in[31] << SHIFT);
	}

	static void Pack(const uint64_t *__restrict in, uint32_t *__restrict out) {
		*out |= (in[31] << SHIFT);
		if (DELTA > 32) {
			++out;
			*out = static_cast<uint32_t>(in[31] >> (32 - SHIFT));
		}
	}
};

// Special cases
void __fastunpack0(const uint8_t *__restrict, uint8_t *__restrict out) {
	for (uint8_t i = 0; i < 8; ++i)
		*(out++) = 0;
}

void __fastunpack0(const uint16_t *__restrict, uint16_t *__restrict out) {
	for (uint16_t i = 0; i < 16; ++i)
		*(out++) = 0;
}

void __fastunpack0(const uint32_t *__restrict, uint32_t *__restrict out) {
	for (uint32_t i = 0; i < 32; ++i)
		*(out++) = 0;
}

void __fastunpack0(const uint32_t *__restrict, uint64_t *__restrict out) {
	for (uint32_t i = 0; i < 32; ++i)
		*(out++) = 0;
}

void __fastpack0(const uint8_t *__restrict, uint8_t *__restrict) {
}
void __fastpack0(const uint16_t *__restrict, uint16_t *__restrict) {
}
void __fastpack0(const uint32_t *__restrict, uint32_t *__restrict) {
}
void __fastpack0(const uint64_t *__restrict, uint32_t *__restrict) {
}

// fastunpack for 8 bits
void __fastunpack1(const uint8_t *__restrict in, uint8_t *__restrict out) {
	Unroller8<1>::Unpack(in, out);
}

void __fastunpack2(const uint8_t *__restrict in, uint8_t *__restrict out) {
	Unroller8<2>::Unpack(in, out);
}

void __fastunpack3(const uint8_t *__restrict in, uint8_t *__restrict out) {
	Unroller8<3>::Unpack(in, out);
}

void __fastunpack4(const uint8_t *__restrict in, uint8_t *__restrict out) {
	for (uint8_t outer = 0; outer < 4; ++outer) {
		for (uint8_t inwordpointer = 0; inwordpointer < 8; inwordpointer += 4)
			*(out++) = ((*in) >> inwordpointer) % (1U << 4);
		++in;
	}
}

void __fastunpack5(const uint8_t *__restrict in, uint8_t *__restrict out) {
	Unroller8<5>::Unpack(in, out);
}

void __fastunpack6(const uint8_t *__restrict in, uint8_t *__restrict out) {
	Unroller8<6>::Unpack(in, out);
}

void __fastunpack7(const uint8_t *__restrict in, uint8_t *__restrict out) {
	Unroller8<7>::Unpack(in, out);
}

void __fastunpack8(const uint8_t *__restrict in, uint8_t *__restrict out) {
	for (int k = 0; k < 8; ++k)
		out[k] = in[k];
}


// fastunpack for 16 bits
void __fastunpack1(const uint16_t *__restrict in, uint16_t *__restrict out) {
	Unroller16<1>::Unpack(in, out);
}

void __fastunpack2(const uint16_t *__restrict in, uint16_t *__restrict out) {
	Unroller16<2>::Unpack(in, out);
}

void __fastunpack3(const uint16_t *__restrict in, uint16_t *__restrict out) {
	Unroller16<3>::Unpack(in, out);
}

void __fastunpack4(const uint16_t *__restrict in, uint16_t *__restrict out) {
	for (uint16_t outer = 0; outer < 4; ++outer) {
		for (uint16_t inwordpointer = 0; inwordpointer < 16; inwordpointer += 4)
			*(out++) = ((*in) >> inwordpointer) % (1U << 4);
		++in;
	}
}

void __fastunpack5(const uint16_t *__restrict in, uint16_t *__restrict out) {
	Unroller16<5>::Unpack(in, out);
}

void __fastunpack6(const uint16_t *__restrict in, uint16_t *__restrict out) {
	Unroller16<6>::Unpack(in, out);
}

void __fastunpack7(const uint16_t *__restrict in, uint16_t *__restrict out) {
	Unroller16<7>::Unpack(in, out);
}

void __fastunpack8(const uint16_t *__restrict in, uint16_t *__restrict out) {
	for (uint16_t outer = 0; outer < 8; ++outer) {
		for (uint16_t inwordpointer = 0; inwordpointer < 16; inwordpointer += 8)
			*(out++) = ((*in) >> inwordpointer) % (1U << 8);
		++in;
	}
}

void __fastunpack9(const uint16_t *__restrict in, uint16_t *__restrict out) {
	Unroller16<9>::Unpack(in, out);
}

void __fastunpack10(const uint16_t *__restrict in, uint16_t *__restrict out) {
	Unroller16<10>::Unpack(in, out);
}

void __fastunpack11(const uint16_t *__restrict in, uint16_t *__restrict out) {
	Unroller16<11>::Unpack(in, out);
}

void __fastunpack12(const uint16_t *__restrict in, uint16_t *__restrict out) {
	Unroller16<12>::Unpack(in, out);
}

void __fastunpack13(const uint16_t *__restrict in, uint16_t *__restrict out) {
	Unroller16<13>::Unpack(in, out);
}

void __fastunpack14(const uint16_t *__restrict in, uint16_t *__restrict out) {
	Unroller16<14>::Unpack(in, out);
}

void __fastunpack15(const uint16_t *__restrict in, uint16_t *__restrict out) {
	Unroller16<15>::Unpack(in, out);
}

void __fastunpack16(const uint16_t *__restrict in, uint16_t *__restrict out) {
	for (int k = 0; k < 16; ++k)
		out[k] = in[k];
}

// fastunpack for 32 bits
void __fastunpack1(const uint32_t *__restrict in, uint32_t *__restrict out) {
	Unroller<1>::Unpack(in, out);
}

void __fastunpack2(const uint32_t *__restrict in, uint32_t *__restrict out) {
	Unroller<2>::Unpack(in, out);
}

void __fastunpack3(const uint32_t *__restrict in, uint32_t *__restrict out) {
	Unroller<3>::Unpack(in, out);
}

void __fastunpack4(const uint32_t *__restrict in, uint32_t *__restrict out) {
	for (uint32_t outer = 0; outer < 4; ++outer) {
		for (uint32_t inwordpointer = 0; inwordpointer < 32; inwordpointer += 4)
			*(out++) = ((*in) >> inwordpointer) % (1U << 4);
		++in;
	}
}

void __fastunpack5(const uint32_t *__restrict in, uint32_t *__restrict out) {
	Unroller<5>::Unpack(in, out);
}

void __fastunpack6(const uint32_t *__restrict in, uint32_t *__restrict out) {
	Unroller<6>::Unpack(in, out);
}

void __fastunpack7(const uint32_t *__restrict in, uint32_t *__restrict out) {
	Unroller<7>::Unpack(in, out);
}

void __fastunpack8(const uint32_t *__restrict in, uint32_t *__restrict out) {
	for (uint32_t outer = 0; outer < 8; ++outer) {
		for (uint32_t inwordpointer = 0; inwordpointer < 32; inwordpointer += 8)
			*(out++) = ((*in) >> inwordpointer) % (1U << 8);
		++in;
	}
}

void __fastunpack9(const uint32_t *__restrict in, uint32_t *__restrict out) {
	Unroller<9>::Unpack(in, out);
}

void __fastunpack10(const uint32_t *__restrict in, uint32_t *__restrict out) {
	Unroller<10>::Unpack(in, out);
}

void __fastunpack11(const uint32_t *__restrict in, uint32_t *__restrict out) {
	Unroller<11>::Unpack(in, out);
}

void __fastunpack12(const uint32_t *__restrict in, uint32_t *__restrict out) {
	Unroller<12>::Unpack(in, out);
}

void __fastunpack13(const uint32_t *__restrict in, uint32_t *__restrict out) {
	Unroller<13>::Unpack(in, out);
}

void __fastunpack14(const uint32_t *__restrict in, uint32_t *__restrict out) {
	Unroller<14>::Unpack(in, out);
}

void __fastunpack15(const uint32_t *__restrict in, uint32_t *__restrict out) {
	Unroller<15>::Unpack(in, out);
}

void __fastunpack16(const uint32_t *__restrict in, uint32_t *__restrict out) {
	for (uint32_t outer = 0; outer < 16; ++outer) {
		for (uint32_t inwordpointer = 0; inwordpointer < 32; inwordpointer += 16)
			*(out++) = ((*in) >> inwordpointer) % (1U << 16);
		++in;
	}
}

void __fastunpack17(const uint32_t *__restrict in, uint32_t *__restrict out) {
	Unroller<17>::Unpack(in, out);
}

void __fastunpack18(const uint32_t *__restrict in, uint32_t *__restrict out) {
	Unroller<18>::Unpack(in, out);
}

void __fastunpack19(const uint32_t *__restrict in, uint32_t *__restrict out) {
	Unroller<19>::Unpack(in, out);
}

void __fastunpack20(const uint32_t *__restrict in, uint32_t *__restrict out) {
	Unroller<20>::Unpack(in, out);
}

void __fastunpack21(const uint32_t *__restrict in, uint32_t *__restrict out) {
	Unroller<21>::Unpack(in, out);
}

void __fastunpack22(const uint32_t *__restrict in, uint32_t *__restrict out) {
	Unroller<22>::Unpack(in, out);
}

void __fastunpack23(const uint32_t *__restrict in, uint32_t *__restrict out) {
	Unroller<23>::Unpack(in, out);
}

void __fastunpack24(const uint32_t *__restrict in, uint32_t *__restrict out) {
	Unroller<24>::Unpack(in, out);
}

void __fastunpack25(const uint32_t *__restrict in, uint32_t *__restrict out) {
	Unroller<25>::Unpack(in, out);
}

void __fastunpack26(const uint32_t *__restrict in, uint32_t *__restrict out) {
	Unroller<26>::Unpack(in, out);
}

void __fastunpack27(const uint32_t *__restrict in, uint32_t *__restrict out) {
	Unroller<27>::Unpack(in, out);
}

void __fastunpack28(const uint32_t *__restrict in, uint32_t *__restrict out) {
	Unroller<28>::Unpack(in, out);
}

void __fastunpack29(const uint32_t *__restrict in, uint32_t *__restrict out) {
	Unroller<29>::Unpack(in, out);
}

void __fastunpack30(const uint32_t *__restrict in, uint32_t *__restrict out) {
	Unroller<30>::Unpack(in, out);
}

void __fastunpack31(const uint32_t *__restrict in, uint32_t *__restrict out) {
	Unroller<31>::Unpack(in, out);
}

void __fastunpack32(const uint32_t *__restrict in, uint32_t *__restrict out) {
	for (int k = 0; k < 32; ++k)
		out[k] = in[k];
}

// fastupack for 64 bits
void __fastunpack1(const uint32_t *__restrict in, uint64_t *__restrict out) {
	Unroller<1>::Unpack(in, out);
}

void __fastunpack2(const uint32_t *__restrict in, uint64_t *__restrict out) {
	Unroller<2>::Unpack(in, out);
}

void __fastunpack3(const uint32_t *__restrict in, uint64_t *__restrict out) {
	Unroller<3>::Unpack(in, out);
}

void __fastunpack4(const uint32_t *__restrict in, uint64_t *__restrict out) {
	for (uint32_t outer = 0; outer < 4; ++outer) {
		for (uint32_t inwordpointer = 0; inwordpointer < 32; inwordpointer += 4)
			*(out++) = ((*in) >> inwordpointer) % (1U << 4);
		++in;
	}
}

void __fastunpack5(const uint32_t *__restrict in, uint64_t *__restrict out) {
	Unroller<5>::Unpack(in, out);
}

void __fastunpack6(const uint32_t *__restrict in, uint64_t *__restrict out) {
	Unroller<6>::Unpack(in, out);
}

void __fastunpack7(const uint32_t *__restrict in, uint64_t *__restrict out) {
	Unroller<7>::Unpack(in, out);
}

void __fastunpack8(const uint32_t *__restrict in, uint64_t *__restrict out) {
	for (uint32_t outer = 0; outer < 8; ++outer) {
		for (uint32_t inwordpointer = 0; inwordpointer < 32; inwordpointer += 8) {
			*(out++) = ((*in) >> inwordpointer) % (1U << 8);
		}
		++in;
	}
}

void __fastunpack9(const uint32_t *__restrict in, uint64_t *__restrict out) {
	Unroller<9>::Unpack(in, out);
}

void __fastunpack10(const uint32_t *__restrict in, uint64_t *__restrict out) {
	Unroller<10>::Unpack(in, out);
}

void __fastunpack11(const uint32_t *__restrict in, uint64_t *__restrict out) {
	Unroller<11>::Unpack(in, out);
}

void __fastunpack12(const uint32_t *__restrict in, uint64_t *__restrict out) {
	Unroller<12>::Unpack(in, out);
}

void __fastunpack13(const uint32_t *__restrict in, uint64_t *__restrict out) {
	Unroller<13>::Unpack(in, out);
}

void __fastunpack14(const uint32_t *__restrict in, uint64_t *__restrict out) {
	Unroller<14>::Unpack(in, out);
}

void __fastunpack15(const uint32_t *__restrict in, uint64_t *__restrict out) {
	Unroller<15>::Unpack(in, out);
}

void __fastunpack16(const uint32_t *__restrict in, uint64_t *__restrict out) {
	for (uint32_t outer = 0; outer < 16; ++outer) {
		for (uint32_t inwordpointer = 0; inwordpointer < 32; inwordpointer += 16)
			*(out++) = ((*in) >> inwordpointer) % (1U << 16);
		++in;
	}
}

void __fastunpack17(const uint32_t *__restrict in, uint64_t *__restrict out) {
	Unroller<17>::Unpack(in, out);
}

void __fastunpack18(const uint32_t *__restrict in, uint64_t *__restrict out) {
	Unroller<18>::Unpack(in, out);
}

void __fastunpack19(const uint32_t *__restrict in, uint64_t *__restrict out) {
	Unroller<19>::Unpack(in, out);
}

void __fastunpack20(const uint32_t *__restrict in, uint64_t *__restrict out) {
	Unroller<20>::Unpack(in, out);
}

void __fastunpack21(const uint32_t *__restrict in, uint64_t *__restrict out) {
	Unroller<21>::Unpack(in, out);
}

void __fastunpack22(const uint32_t *__restrict in, uint64_t *__restrict out) {
	Unroller<22>::Unpack(in, out);
}

void __fastunpack23(const uint32_t *__restrict in, uint64_t *__restrict out) {
	Unroller<23>::Unpack(in, out);
}

void __fastunpack24(const uint32_t *__restrict in, uint64_t *__restrict out) {
	Unroller<24>::Unpack(in, out);
}

void __fastunpack25(const uint32_t *__restrict in, uint64_t *__restrict out) {
	Unroller<25>::Unpack(in, out);
}

void __fastunpack26(const uint32_t *__restrict in, uint64_t *__restrict out) {
	Unroller<26>::Unpack(in, out);
}

void __fastunpack27(const uint32_t *__restrict in, uint64_t *__restrict out) {
	Unroller<27>::Unpack(in, out);
}

void __fastunpack28(const uint32_t *__restrict in, uint64_t *__restrict out) {
	Unroller<28>::Unpack(in, out);
}

void __fastunpack29(const uint32_t *__restrict in, uint64_t *__restrict out) {
	Unroller<29>::Unpack(in, out);
}

void __fastunpack30(const uint32_t *__restrict in, uint64_t *__restrict out) {
	Unroller<30>::Unpack(in, out);
}

void __fastunpack31(const uint32_t *__restrict in, uint64_t *__restrict out) {
	Unroller<31>::Unpack(in, out);
}

void __fastunpack32(const uint32_t *__restrict in, uint64_t *__restrict out) {
	for (int k = 0; k < 32; ++k)
		out[k] = in[k];
}

void __fastunpack33(const uint32_t *__restrict in, uint64_t *__restrict out) {
	Unroller<33>::Unpack(in, out);
}

void __fastunpack34(const uint32_t *__restrict in, uint64_t *__restrict out) {
	Unroller<34>::Unpack(in, out);
}

void __fastunpack35(const uint32_t *__restrict in, uint64_t *__restrict out) {
	Unroller<35>::Unpack(in, out);
}

void __fastunpack36(const uint32_t *__restrict in, uint64_t *__restrict out) {
	Unroller<36>::Unpack(in, out);
}

void __fastunpack37(const uint32_t *__restrict in, uint64_t *__restrict out) {
	Unroller<37>::Unpack(in, out);
}

void __fastunpack38(const uint32_t *__restrict in, uint64_t *__restrict out) {
	Unroller<38>::Unpack(in, out);
}

void __fastunpack39(const uint32_t *__restrict in, uint64_t *__restrict out) {
	Unroller<39>::Unpack(in, out);
}

void __fastunpack40(const uint32_t *__restrict in, uint64_t *__restrict out) {
	Unroller<40>::Unpack(in, out);
}

void __fastunpack41(const uint32_t *__restrict in, uint64_t *__restrict out) {
	Unroller<41>::Unpack(in, out);
}

void __fastunpack42(const uint32_t *__restrict in, uint64_t *__restrict out) {
	Unroller<42>::Unpack(in, out);
}

void __fastunpack43(const uint32_t *__restrict in, uint64_t *__restrict out) {
	Unroller<43>::Unpack(in, out);
}

void __fastunpack44(const uint32_t *__restrict in, uint64_t *__restrict out) {
	Unroller<44>::Unpack(in, out);
}

void __fastunpack45(const uint32_t *__restrict in, uint64_t *__restrict out) {
	Unroller<45>::Unpack(in, out);
}

void __fastunpack46(const uint32_t *__restrict in, uint64_t *__restrict out) {
	Unroller<46>::Unpack(in, out);
}

void __fastunpack47(const uint32_t *__restrict in, uint64_t *__restrict out) {
	Unroller<47>::Unpack(in, out);
}

void __fastunpack48(const uint32_t *__restrict in, uint64_t *__restrict out) {
	Unroller<48>::Unpack(in, out);
}

void __fastunpack49(const uint32_t *__restrict in, uint64_t *__restrict out) {
	Unroller<49>::Unpack(in, out);
}

void __fastunpack50(const uint32_t *__restrict in, uint64_t *__restrict out) {
	Unroller<50>::Unpack(in, out);
}

void __fastunpack51(const uint32_t *__restrict in, uint64_t *__restrict out) {
	Unroller<51>::Unpack(in, out);
}

void __fastunpack52(const uint32_t *__restrict in, uint64_t *__restrict out) {
	Unroller<52>::Unpack(in, out);
}

void __fastunpack53(const uint32_t *__restrict in, uint64_t *__restrict out) {
	Unroller<53>::Unpack(in, out);
}

void __fastunpack54(const uint32_t *__restrict in, uint64_t *__restrict out) {
	Unroller<54>::Unpack(in, out);
}

void __fastunpack55(const uint32_t *__restrict in, uint64_t *__restrict out) {
	Unroller<55>::Unpack(in, out);
}

void __fastunpack56(const uint32_t *__restrict in, uint64_t *__restrict out) {
	Unroller<56>::Unpack(in, out);
}

void __fastunpack57(const uint32_t *__restrict in, uint64_t *__restrict out) {
	Unroller<57>::Unpack(in, out);
}

void __fastunpack58(const uint32_t *__restrict in, uint64_t *__restrict out) {
	Unroller<58>::Unpack(in, out);
}

void __fastunpack59(const uint32_t *__restrict in, uint64_t *__restrict out) {
	Unroller<59>::Unpack(in, out);
}

void __fastunpack60(const uint32_t *__restrict in, uint64_t *__restrict out) {
	Unroller<60>::Unpack(in, out);
}

void __fastunpack61(const uint32_t *__restrict in, uint64_t *__restrict out) {
	Unroller<61>::Unpack(in, out);
}

void __fastunpack62(const uint32_t *__restrict in, uint64_t *__restrict out) {
	Unroller<62>::Unpack(in, out);
}

void __fastunpack63(const uint32_t *__restrict in, uint64_t *__restrict out) {
	Unroller<63>::Unpack(in, out);
}

void __fastunpack64(const uint32_t *__restrict in, uint64_t *__restrict out) {
	for (int k = 0; k < 32; ++k) {
		out[k] = in[k * 2];
		out[k] |= static_cast<uint64_t>(in[k * 2 + 1]) << 32;
	}
}

// fastpack for 8 bits

void __fastpack1(const uint8_t *__restrict in, uint8_t *__restrict out) {
	Unroller8<1>::Pack(in, out);
}

void __fastpack2(const uint8_t *__restrict in, uint8_t *__restrict out) {
	Unroller8<2>::Pack(in, out);
}

void __fastpack3(const uint8_t *__restrict in, uint8_t *__restrict out) {
	Unroller8<3>::Pack(in, out);
}

void __fastpack4(const uint8_t *__restrict in, uint8_t *__restrict out) {
	Unroller8<4>::Pack(in, out);
}

void __fastpack5(const uint8_t *__restrict in, uint8_t *__restrict out) {
	Unroller8<5>::Pack(in, out);
}

void __fastpack6(const uint8_t *__restrict in, uint8_t *__restrict out) {
	Unroller8<6>::Pack(in, out);
}

void __fastpack7(const uint8_t *__restrict in, uint8_t *__restrict out) {
	Unroller8<7>::Pack(in, out);
}

void __fastpack8(const uint8_t *__restrict in, uint8_t *__restrict out) {
	for (int k = 0; k < 8; ++k)
		out[k] = in[k];
}

// fastpack for 16 bits

void __fastpack1(const uint16_t *__restrict in, uint16_t *__restrict out) {
	Unroller16<1>::Pack(in, out);
}

void __fastpack2(const uint16_t *__restrict in, uint16_t *__restrict out) {
	Unroller16<2>::Pack(in, out);
}

void __fastpack3(const uint16_t *__restrict in, uint16_t *__restrict out) {
	Unroller16<3>::Pack(in, out);
}

void __fastpack4(const uint16_t *__restrict in, uint16_t *__restrict out) {
	Unroller16<4>::Pack(in, out);
}

void __fastpack5(const uint16_t *__restrict in, uint16_t *__restrict out) {
	Unroller16<5>::Pack(in, out);
}

void __fastpack6(const uint16_t *__restrict in, uint16_t *__restrict out) {
	Unroller16<6>::Pack(in, out);
}

void __fastpack7(const uint16_t *__restrict in, uint16_t *__restrict out) {
	Unroller16<7>::Pack(in, out);
}

void __fastpack8(const uint16_t *__restrict in, uint16_t *__restrict out) {
	Unroller16<8>::Pack(in, out);
}

void __fastpack9(const uint16_t *__restrict in, uint16_t *__restrict out) {
	Unroller16<9>::Pack(in, out);
}

void __fastpack10(const uint16_t *__restrict in, uint16_t *__restrict out) {
	Unroller16<10>::Pack(in, out);
}

void __fastpack11(const uint16_t *__restrict in, uint16_t *__restrict out) {
	Unroller16<11>::Pack(in, out);
}

void __fastpack12(const uint16_t *__restrict in, uint16_t *__restrict out) {
	Unroller16<12>::Pack(in, out);
}

void __fastpack13(const uint16_t *__restrict in, uint16_t *__restrict out) {
	Unroller16<13>::Pack(in, out);
}

void __fastpack14(const uint16_t *__restrict in, uint16_t *__restrict out) {
	Unroller16<14>::Pack(in, out);
}

void __fastpack15(const uint16_t *__restrict in, uint16_t *__restrict out) {
	Unroller16<15>::Pack(in, out);
}

void __fastpack16(const uint16_t *__restrict in, uint16_t *__restrict out) {
	for (int k = 0; k < 16; ++k)
		out[k] = in[k];
}


// fastpack for 32 bits

void __fastpack1(const uint32_t *__restrict in, uint32_t *__restrict out) {
	Unroller<1>::Pack(in, out);
}

void __fastpack2(const uint32_t *__restrict in, uint32_t *__restrict out) {
	Unroller<2>::Pack(in, out);
}

void __fastpack3(const uint32_t *__restrict in, uint32_t *__restrict out) {
	Unroller<3>::Pack(in, out);
}

void __fastpack4(const uint32_t *__restrict in, uint32_t *__restrict out) {
	Unroller<4>::Pack(in, out);
}

void __fastpack5(const uint32_t *__restrict in, uint32_t *__restrict out) {
	Unroller<5>::Pack(in, out);
}

void __fastpack6(const uint32_t *__restrict in, uint32_t *__restrict out) {
	Unroller<6>::Pack(in, out);
}

void __fastpack7(const uint32_t *__restrict in, uint32_t *__restrict out) {
	Unroller<7>::Pack(in, out);
}

void __fastpack8(const uint32_t *__restrict in, uint32_t *__restrict out) {
	Unroller<8>::Pack(in, out);
}

void __fastpack9(const uint32_t *__restrict in, uint32_t *__restrict out) {
	Unroller<9>::Pack(in, out);
}

void __fastpack10(const uint32_t *__restrict in, uint32_t *__restrict out) {
	Unroller<10>::Pack(in, out);
}

void __fastpack11(const uint32_t *__restrict in, uint32_t *__restrict out) {
	Unroller<11>::Pack(in, out);
}

void __fastpack12(const uint32_t *__restrict in, uint32_t *__restrict out) {
	Unroller<12>::Pack(in, out);
}

void __fastpack13(const uint32_t *__restrict in, uint32_t *__restrict out) {
	Unroller<13>::Pack(in, out);
}

void __fastpack14(const uint32_t *__restrict in, uint32_t *__restrict out) {
	Unroller<14>::Pack(in, out);
}

void __fastpack15(const uint32_t *__restrict in, uint32_t *__restrict out) {
	Unroller<15>::Pack(in, out);
}

void __fastpack16(const uint32_t *__restrict in, uint32_t *__restrict out) {
	Unroller<16>::Pack(in, out);
}

void __fastpack17(const uint32_t *__restrict in, uint32_t *__restrict out) {
	Unroller<17>::Pack(in, out);
}

void __fastpack18(const uint32_t *__restrict in, uint32_t *__restrict out) {
	Unroller<18>::Pack(in, out);
}

void __fastpack19(const uint32_t *__restrict in, uint32_t *__restrict out) {
	Unroller<19>::Pack(in, out);
}

void __fastpack20(const uint32_t *__restrict in, uint32_t *__restrict out) {
	Unroller<20>::Pack(in, out);
}

void __fastpack21(const uint32_t *__restrict in, uint32_t *__restrict out) {
	Unroller<21>::Pack(in, out);
}

void __fastpack22(const uint32_t *__restrict in, uint32_t *__restrict out) {
	Unroller<22>::Pack(in, out);
}

void __fastpack23(const uint32_t *__restrict in, uint32_t *__restrict out) {
	Unroller<23>::Pack(in, out);
}

void __fastpack24(const uint32_t *__restrict in, uint32_t *__restrict out) {
	Unroller<24>::Pack(in, out);
}

void __fastpack25(const uint32_t *__restrict in, uint32_t *__restrict out) {
	Unroller<25>::Pack(in, out);
}

void __fastpack26(const uint32_t *__restrict in, uint32_t *__restrict out) {
	Unroller<26>::Pack(in, out);
}

void __fastpack27(const uint32_t *__restrict in, uint32_t *__restrict out) {
	Unroller<27>::Pack(in, out);
}

void __fastpack28(const uint32_t *__restrict in, uint32_t *__restrict out) {
	Unroller<28>::Pack(in, out);
}

void __fastpack29(const uint32_t *__restrict in, uint32_t *__restrict out) {
	Unroller<29>::Pack(in, out);
}

void __fastpack30(const uint32_t *__restrict in, uint32_t *__restrict out) {
	Unroller<30>::Pack(in, out);
}

void __fastpack31(const uint32_t *__restrict in, uint32_t *__restrict out) {
	Unroller<31>::Pack(in, out);
}

void __fastpack32(const uint32_t *__restrict in, uint32_t *__restrict out) {
	for (int k = 0; k < 32; ++k)
		out[k] = in[k];
}

// fastpack for 64 bits

void __fastpack1(const uint64_t *__restrict in, uint32_t *__restrict out) {
	Unroller<1>::Pack(in, out);
}

void __fastpack2(const uint64_t *__restrict in, uint32_t *__restrict out) {
	Unroller<2>::Pack(in, out);
}

void __fastpack3(const uint64_t *__restrict in, uint32_t *__restrict out) {
	Unroller<3>::Pack(in, out);
}

void __fastpack4(const uint64_t *__restrict in, uint32_t *__restrict out) {
	Unroller<4>::Pack(in, out);
}

void __fastpack5(const uint64_t *__restrict in, uint32_t *__restrict out) {
	Unroller<5>::Pack(in, out);
}

void __fastpack6(const uint64_t *__restrict in, uint32_t *__restrict out) {
	Unroller<6>::Pack(in, out);
}

void __fastpack7(const uint64_t *__restrict in, uint32_t *__restrict out) {
	Unroller<7>::Pack(in, out);
}

void __fastpack8(const uint64_t *__restrict in, uint32_t *__restrict out) {
	Unroller<8>::Pack(in, out);
}

void __fastpack9(const uint64_t *__restrict in, uint32_t *__restrict out) {
	Unroller<9>::Pack(in, out);
}

void __fastpack10(const uint64_t *__restrict in, uint32_t *__restrict out) {
	Unroller<10>::Pack(in, out);
}

void __fastpack11(const uint64_t *__restrict in, uint32_t *__restrict out) {
	Unroller<11>::Pack(in, out);
}

void __fastpack12(const uint64_t *__restrict in, uint32_t *__restrict out) {
	Unroller<12>::Pack(in, out);
}

void __fastpack13(const uint64_t *__restrict in, uint32_t *__restrict out) {
	Unroller<13>::Pack(in, out);
}

void __fastpack14(const uint64_t *__restrict in, uint32_t *__restrict out) {
	Unroller<14>::Pack(in, out);
}

void __fastpack15(const uint64_t *__restrict in, uint32_t *__restrict out) {
	Unroller<15>::Pack(in, out);
}

void __fastpack16(const uint64_t *__restrict in, uint32_t *__restrict out) {
	Unroller<16>::Pack(in, out);
}

void __fastpack17(const uint64_t *__restrict in, uint32_t *__restrict out) {
	Unroller<17>::Pack(in, out);
}

void __fastpack18(const uint64_t *__restrict in, uint32_t *__restrict out) {
	Unroller<18>::Pack(in, out);
}

void __fastpack19(const uint64_t *__restrict in, uint32_t *__restrict out) {
	Unroller<19>::Pack(in, out);
}

void __fastpack20(const uint64_t *__restrict in, uint32_t *__restrict out) {
	Unroller<20>::Pack(in, out);
}

void __fastpack21(const uint64_t *__restrict in, uint32_t *__restrict out) {
	Unroller<21>::Pack(in, out);
}

void __fastpack22(const uint64_t *__restrict in, uint32_t *__restrict out) {
	Unroller<22>::Pack(in, out);
}

void __fastpack23(const uint64_t *__restrict in, uint32_t *__restrict out) {
	Unroller<23>::Pack(in, out);
}

void __fastpack24(const uint64_t *__restrict in, uint32_t *__restrict out) {
	Unroller<24>::Pack(in, out);
}

void __fastpack25(const uint64_t *__restrict in, uint32_t *__restrict out) {
	Unroller<25>::Pack(in, out);
}

void __fastpack26(const uint64_t *__restrict in, uint32_t *__restrict out) {
	Unroller<26>::Pack(in, out);
}

void __fastpack27(const uint64_t *__restrict in, uint32_t *__restrict out) {
	Unroller<27>::Pack(in, out);
}

void __fastpack28(const uint64_t *__restrict in, uint32_t *__restrict out) {
	Unroller<28>::Pack(in, out);
}

void __fastpack29(const uint64_t *__restrict in, uint32_t *__restrict out) {
	Unroller<29>::Pack(in, out);
}

void __fastpack30(const uint64_t *__restrict in, uint32_t *__restrict out) {
	Unroller<30>::Pack(in, out);
}

void __fastpack31(const uint64_t *__restrict in, uint32_t *__restrict out) {
	Unroller<31>::Pack(in, out);
}

void __fastpack32(const uint64_t *__restrict in, uint32_t *__restrict out) {
	for (int k = 0; k < 32; ++k) {
		out[k] = static_cast<uint32_t>(in[k]);
	}
}

void __fastpack33(const uint64_t *__restrict in, uint32_t *__restrict out) {
	Unroller<33>::Pack(in, out);
}

void __fastpack34(const uint64_t *__restrict in, uint32_t *__restrict out) {
	Unroller<34>::Pack(in, out);
}

void __fastpack35(const uint64_t *__restrict in, uint32_t *__restrict out) {
	Unroller<35>::Pack(in, out);
}

void __fastpack36(const uint64_t *__restrict in, uint32_t *__restrict out) {
	Unroller<36>::Pack(in, out);
}

void __fastpack37(const uint64_t *__restrict in, uint32_t *__restrict out) {
	Unroller<37>::Pack(in, out);
}

void __fastpack38(const uint64_t *__restrict in, uint32_t *__restrict out) {
	Unroller<38>::Pack(in, out);
}

void __fastpack39(const uint64_t *__restrict in, uint32_t *__restrict out) {
	Unroller<39>::Pack(in, out);
}

void __fastpack40(const uint64_t *__restrict in, uint32_t *__restrict out) {
	Unroller<40>::Pack(in, out);
}

void __fastpack41(const uint64_t *__restrict in, uint32_t *__restrict out) {
	Unroller<41>::Pack(in, out);
}

void __fastpack42(const uint64_t *__restrict in, uint32_t *__restrict out) {
	Unroller<42>::Pack(in, out);
}

void __fastpack43(const uint64_t *__restrict in, uint32_t *__restrict out) {
	Unroller<43>::Pack(in, out);
}

void __fastpack44(const uint64_t *__restrict in, uint32_t *__restrict out) {
	Unroller<44>::Pack(in, out);
}

void __fastpack45(const uint64_t *__restrict in, uint32_t *__restrict out) {
	Unroller<45>::Pack(in, out);
}

void __fastpack46(const uint64_t *__restrict in, uint32_t *__restrict out) {
	Unroller<46>::Pack(in, out);
}

void __fastpack47(const uint64_t *__restrict in, uint32_t *__restrict out) {
	Unroller<47>::Pack(in, out);
}

void __fastpack48(const uint64_t *__restrict in, uint32_t *__restrict out) {
	Unroller<48>::Pack(in, out);
}

void __fastpack49(const uint64_t *__restrict in, uint32_t *__restrict out) {
	Unroller<49>::Pack(in, out);
}

void __fastpack50(const uint64_t *__restrict in, uint32_t *__restrict out) {
	Unroller<50>::Pack(in, out);
}

void __fastpack51(const uint64_t *__restrict in, uint32_t *__restrict out) {
	Unroller<51>::Pack(in, out);
}

void __fastpack52(const uint64_t *__restrict in, uint32_t *__restrict out) {
	Unroller<52>::Pack(in, out);
}

void __fastpack53(const uint64_t *__restrict in, uint32_t *__restrict out) {
	Unroller<53>::Pack(in, out);
}

void __fastpack54(const uint64_t *__restrict in, uint32_t *__restrict out) {
	Unroller<54>::Pack(in, out);
}

void __fastpack55(const uint64_t *__restrict in, uint32_t *__restrict out) {
	Unroller<55>::Pack(in, out);
}

void __fastpack56(const uint64_t *__restrict in, uint32_t *__restrict out) {
	Unroller<56>::Pack(in, out);
}

void __fastpack57(const uint64_t *__restrict in, uint32_t *__restrict out) {
	Unroller<57>::Pack(in, out);
}

void __fastpack58(const uint64_t *__restrict in, uint32_t *__restrict out) {
	Unroller<58>::Pack(in, out);
}

void __fastpack59(const uint64_t *__restrict in, uint32_t *__restrict out) {
	Unroller<59>::Pack(in, out);
}

void __fastpack60(const uint64_t *__restrict in, uint32_t *__restrict out) {
	Unroller<60>::Pack(in, out);
}

void __fastpack61(const uint64_t *__restrict in, uint32_t *__restrict out) {
	Unroller<61>::Pack(in, out);
}

void __fastpack62(const uint64_t *__restrict in, uint32_t *__restrict out) {
	Unroller<62>::Pack(in, out);
}

void __fastpack63(const uint64_t *__restrict in, uint32_t *__restrict out) {
	Unroller<63>::Pack(in, out);
}

void __fastpack64(const uint64_t *__restrict in, uint32_t *__restrict out) {
	for (int i = 0; i < 32; ++i) {
		out[2 * i] = static_cast<uint32_t>(in[i]);
		out[2 * i + 1] = in[i] >> 32;
	}
}
} // namespace internal
} // namespace duckdb_fastpforlib
