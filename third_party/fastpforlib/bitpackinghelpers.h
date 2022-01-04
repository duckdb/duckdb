/**
* This code is released under the
* Apache License Version 2.0 http://www.apache.org/licenses/.
*
* (c) Daniel Lemire, http://lemire.me/en/
*/
#pragma once
#include "bitpacking.h"


#include <stdexcept>

namespace duckdb_fastpforlib {

namespace internal {

// Note that this only packs 8 values
inline void fastunpack_quarter(const uint8_t *__restrict in, uint8_t *__restrict out, const uint32_t bit) {
	// Could have used function pointers instead of switch.
	// Switch calls do offer the compiler more opportunities for optimization in
	// theory. In this case, it makes no difference with a good compiler.
	switch (bit) {
	case 0:
		internal::__fastunpack0(in, out);
		break;
	case 1:
		internal::__fastunpack1(in, out);
		break;
	case 2:
		internal::__fastunpack2(in, out);
		break;
	case 3:
		internal::__fastunpack3(in, out);
		break;
	case 4:
		internal::__fastunpack4(in, out);
		break;
	case 5:
		internal::__fastunpack5(in, out);
		break;
	case 6:
		internal::__fastunpack6(in, out);
		break;
	case 7:
		internal::__fastunpack7(in, out);
		break;
	case 8:
		internal::__fastunpack8(in, out);
		break;
	default:
		throw std::logic_error("Invalid bit width for bitpacking");
	}
}

// Note that this only packs 8 values
inline void fastpack_quarter(const uint8_t *__restrict in, uint8_t *__restrict out, const uint32_t bit) {
	// Could have used function pointers instead of switch.
	// Switch calls do offer the compiler more opportunities for optimization in
	// theory. In this case, it makes no difference with a good compiler.
	switch (bit) {
	case 0:
		internal::__fastpack0(in, out);
		break;
	case 1:
		internal::__fastpack1(in, out);
		break;
	case 2:
		internal::__fastpack2(in, out);
		break;
	case 3:
		internal::__fastpack3(in, out);
		break;
	case 4:
		internal::__fastpack4(in, out);
		break;
	case 5:
		internal::__fastpack5(in, out);
		break;
	case 6:
		internal::__fastpack6(in, out);
		break;
	case 7:
		internal::__fastpack7(in, out);
		break;
	case 8:
		internal::__fastpack8(in, out);
		break;
	default:
		throw std::logic_error("Invalid bit width for bitpacking");
	}
}

// Note that this only packs 16 values
inline void fastunpack_half(const uint16_t *__restrict in, uint16_t *__restrict out, const uint32_t bit) {
	// Could have used function pointers instead of switch.
	// Switch calls do offer the compiler more opportunities for optimization in
	// theory. In this case, it makes no difference with a good compiler.
	switch (bit) {
	case 0:
		internal::__fastunpack0(in, out);
		break;
	case 1:
		internal::__fastunpack1(in, out);
		break;
	case 2:
		internal::__fastunpack2(in, out);
		break;
	case 3:
		internal::__fastunpack3(in, out);
		break;
	case 4:
		internal::__fastunpack4(in, out);
		break;
	case 5:
		internal::__fastunpack5(in, out);
		break;
	case 6:
		internal::__fastunpack6(in, out);
		break;
	case 7:
		internal::__fastunpack7(in, out);
		break;
	case 8:
		internal::__fastunpack8(in, out);
		break;
	case 9:
		internal::__fastunpack9(in, out);
		break;
	case 10:
		internal::__fastunpack10(in, out);
		break;
	case 11:
		internal::__fastunpack11(in, out);
		break;
	case 12:
		internal::__fastunpack12(in, out);
		break;
	case 13:
		internal::__fastunpack13(in, out);
		break;
	case 14:
		internal::__fastunpack14(in, out);
		break;
	case 15:
		internal::__fastunpack15(in, out);
		break;
	case 16:
		internal::__fastunpack16(in, out);
		break;
	default:
		throw std::logic_error("Invalid bit width for bitpacking");
	}
}

// Note that this only packs 16 values
inline void fastpack_half(const uint16_t *__restrict in, uint16_t *__restrict out, const uint32_t bit) {
	// Could have used function pointers instead of switch.
	// Switch calls do offer the compiler more opportunities for optimization in
	// theory. In this case, it makes no difference with a good compiler.
	switch (bit) {
	case 0:
		internal::__fastpack0(in, out);
		break;
	case 1:
		internal::__fastpack1(in, out);
		break;
	case 2:
		internal::__fastpack2(in, out);
		break;
	case 3:
		internal::__fastpack3(in, out);
		break;
	case 4:
		internal::__fastpack4(in, out);
		break;
	case 5:
		internal::__fastpack5(in, out);
		break;
	case 6:
		internal::__fastpack6(in, out);
		break;
	case 7:
		internal::__fastpack7(in, out);
		break;
	case 8:
		internal::__fastpack8(in, out);
		break;
	case 9:
		internal::__fastpack9(in, out);
		break;
	case 10:
		internal::__fastpack10(in, out);
		break;
	case 11:
		internal::__fastpack11(in, out);
		break;
	case 12:
		internal::__fastpack12(in, out);
		break;
	case 13:
		internal::__fastpack13(in, out);
		break;
	case 14:
		internal::__fastpack14(in, out);
		break;
	case 15:
		internal::__fastpack15(in, out);
		break;
	case 16:
		internal::__fastpack16(in, out);
		break;
	default:
		throw std::logic_error("Invalid bit width for bitpacking");
	}
}
}

inline void fastunpack(const uint8_t *__restrict in, uint8_t *__restrict out, const uint32_t bit) {
	for (uint8_t i = 0; i < 4; i++) {
		internal::fastunpack_quarter(in + (i*bit), out+(i*8), bit);
	}
}

inline void fastunpack(const uint16_t *__restrict in, uint16_t *__restrict out, const uint32_t bit) {
	internal::fastunpack_half(in, out, bit);
	internal::fastunpack_half(in + bit, out+16, bit);
}

inline void fastunpack(const uint32_t *__restrict in,
                       uint32_t *__restrict out, const uint32_t bit) {
  // Could have used function pointers instead of switch.
  // Switch calls do offer the compiler more opportunities for optimization in
  // theory. In this case, it makes no difference with a good compiler.
  switch (bit) {
  case 0:
    internal::__fastunpack0(in, out);
    break;
  case 1:
    internal::__fastunpack1(in, out);
    break;
  case 2:
    internal::__fastunpack2(in, out);
    break;
  case 3:
    internal::__fastunpack3(in, out);
    break;
  case 4:
    internal::__fastunpack4(in, out);
    break;
  case 5:
    internal::__fastunpack5(in, out);
    break;
  case 6:
    internal::__fastunpack6(in, out);
    break;
  case 7:
    internal::__fastunpack7(in, out);
    break;
  case 8:
    internal::__fastunpack8(in, out);
    break;
  case 9:
    internal::__fastunpack9(in, out);
    break;
  case 10:
    internal::__fastunpack10(in, out);
    break;
  case 11:
    internal::__fastunpack11(in, out);
    break;
  case 12:
    internal::__fastunpack12(in, out);
    break;
  case 13:
    internal::__fastunpack13(in, out);
    break;
  case 14:
    internal::__fastunpack14(in, out);
    break;
  case 15:
    internal::__fastunpack15(in, out);
    break;
  case 16:
    internal::__fastunpack16(in, out);
    break;
  case 17:
    internal::__fastunpack17(in, out);
    break;
  case 18:
    internal::__fastunpack18(in, out);
    break;
  case 19:
    internal::__fastunpack19(in, out);
    break;
  case 20:
    internal::__fastunpack20(in, out);
    break;
  case 21:
    internal::__fastunpack21(in, out);
    break;
  case 22:
    internal::__fastunpack22(in, out);
    break;
  case 23:
    internal::__fastunpack23(in, out);
    break;
  case 24:
    internal::__fastunpack24(in, out);
    break;
  case 25:
    internal::__fastunpack25(in, out);
    break;
  case 26:
    internal::__fastunpack26(in, out);
    break;
  case 27:
    internal::__fastunpack27(in, out);
    break;
  case 28:
    internal::__fastunpack28(in, out);
    break;
  case 29:
    internal::__fastunpack29(in, out);
    break;
  case 30:
    internal::__fastunpack30(in, out);
    break;
  case 31:
    internal::__fastunpack31(in, out);
    break;
  case 32:
    internal::__fastunpack32(in, out);
    break;
  default:
    throw std::logic_error("Invalid bit width for bitpacking");
  }
}

inline void fastunpack(const uint32_t *__restrict in,
                       uint64_t *__restrict out, const uint32_t bit) {
  // Could have used function pointers instead of switch.
  // Switch calls do offer the compiler more opportunities for optimization in
  // theory. In this case, it makes no difference with a good compiler.
  switch (bit) {
  case 0:
    internal::__fastunpack0(in, out);
    break;
  case 1:
    internal::__fastunpack1(in, out);
    break;
  case 2:
    internal::__fastunpack2(in, out);
    break;
  case 3:
    internal::__fastunpack3(in, out);
    break;
  case 4:
    internal::__fastunpack4(in, out);
    break;
  case 5:
    internal::__fastunpack5(in, out);
    break;
  case 6:
    internal::__fastunpack6(in, out);
    break;
  case 7:
    internal::__fastunpack7(in, out);
    break;
  case 8:
    internal::__fastunpack8(in, out);
    break;
  case 9:
    internal::__fastunpack9(in, out);
    break;
  case 10:
    internal::__fastunpack10(in, out);
    break;
  case 11:
    internal::__fastunpack11(in, out);
    break;
  case 12:
    internal::__fastunpack12(in, out);
    break;
  case 13:
    internal::__fastunpack13(in, out);
    break;
  case 14:
    internal::__fastunpack14(in, out);
    break;
  case 15:
    internal::__fastunpack15(in, out);
    break;
  case 16:
    internal::__fastunpack16(in, out);
    break;
  case 17:
    internal::__fastunpack17(in, out);
    break;
  case 18:
    internal::__fastunpack18(in, out);
    break;
  case 19:
    internal::__fastunpack19(in, out);
    break;
  case 20:
    internal::__fastunpack20(in, out);
    break;
  case 21:
    internal::__fastunpack21(in, out);
    break;
  case 22:
    internal::__fastunpack22(in, out);
    break;
  case 23:
    internal::__fastunpack23(in, out);
    break;
  case 24:
    internal::__fastunpack24(in, out);
    break;
  case 25:
    internal::__fastunpack25(in, out);
    break;
  case 26:
    internal::__fastunpack26(in, out);
    break;
  case 27:
    internal::__fastunpack27(in, out);
    break;
  case 28:
    internal::__fastunpack28(in, out);
    break;
  case 29:
    internal::__fastunpack29(in, out);
    break;
  case 30:
    internal::__fastunpack30(in, out);
    break;
  case 31:
    internal::__fastunpack31(in, out);
    break;
  case 32:
    internal::__fastunpack32(in, out);
    break;
  case 33:
    internal::__fastunpack33(in, out);
    break;
  case 34:
    internal::__fastunpack34(in, out);
    break;
  case 35:
    internal::__fastunpack35(in, out);
    break;
  case 36:
    internal::__fastunpack36(in, out);
    break;
  case 37:
    internal::__fastunpack37(in, out);
    break;
  case 38:
    internal::__fastunpack38(in, out);
    break;
  case 39:
    internal::__fastunpack39(in, out);
    break;
  case 40:
    internal::__fastunpack40(in, out);
    break;
  case 41:
    internal::__fastunpack41(in, out);
    break;
  case 42:
    internal::__fastunpack42(in, out);
    break;
  case 43:
    internal::__fastunpack43(in, out);
    break;
  case 44:
    internal::__fastunpack44(in, out);
    break;
  case 45:
    internal::__fastunpack45(in, out);
    break;
  case 46:
    internal::__fastunpack46(in, out);
    break;
  case 47:
    internal::__fastunpack47(in, out);
    break;
  case 48:
    internal::__fastunpack48(in, out);
    break;
  case 49:
    internal::__fastunpack49(in, out);
    break;
  case 50:
    internal::__fastunpack50(in, out);
    break;
  case 51:
    internal::__fastunpack51(in, out);
    break;
  case 52:
    internal::__fastunpack52(in, out);
    break;
  case 53:
    internal::__fastunpack53(in, out);
    break;
  case 54:
    internal::__fastunpack54(in, out);
    break;
  case 55:
    internal::__fastunpack55(in, out);
    break;
  case 56:
    internal::__fastunpack56(in, out);
    break;
  case 57:
    internal::__fastunpack57(in, out);
    break;
  case 58:
    internal::__fastunpack58(in, out);
    break;
  case 59:
    internal::__fastunpack59(in, out);
    break;
  case 60:
    internal::__fastunpack60(in, out);
    break;
  case 61:
    internal::__fastunpack61(in, out);
    break;
  case 62:
    internal::__fastunpack62(in, out);
    break;
  case 63:
    internal::__fastunpack63(in, out);
    break;
  case 64:
    internal::__fastunpack64(in, out);
    break;
  default:
	throw std::logic_error("Invalid bit width for bitpacking");
  }
}

inline void fastpack(const uint8_t *__restrict in, uint8_t *__restrict out, const uint32_t bit) {

	for (uint8_t i = 0; i < 4; i++) {
		internal::fastpack_quarter(in+(i*8), out + (i*bit), bit);
	}
}

inline void fastpack(const uint16_t *__restrict in, uint16_t *__restrict out, const uint32_t bit) {
	internal::fastpack_half(in, out, bit);
	internal::fastpack_half(in+16, out + bit, bit);
}

inline void fastpack(const uint32_t *__restrict in,
                     uint32_t *__restrict out, const uint32_t bit) {
  // Could have used function pointers instead of switch.
  // Switch calls do offer the compiler more opportunities for optimization in
  // theory. In this case, it makes no difference with a good compiler.
  switch (bit) {
  case 0:
    internal::__fastpack0(in, out);
    break;
  case 1:
    internal::__fastpack1(in, out);
    break;
  case 2:
    internal::__fastpack2(in, out);
    break;
  case 3:
    internal::__fastpack3(in, out);
    break;
  case 4:
    internal::__fastpack4(in, out);
    break;
  case 5:
    internal::__fastpack5(in, out);
    break;
  case 6:
    internal::__fastpack6(in, out);
    break;
  case 7:
    internal::__fastpack7(in, out);
    break;
  case 8:
    internal::__fastpack8(in, out);
    break;
  case 9:
    internal::__fastpack9(in, out);
    break;
  case 10:
    internal::__fastpack10(in, out);
    break;
  case 11:
    internal::__fastpack11(in, out);
    break;
  case 12:
    internal::__fastpack12(in, out);
    break;
  case 13:
    internal::__fastpack13(in, out);
    break;
  case 14:
    internal::__fastpack14(in, out);
    break;
  case 15:
    internal::__fastpack15(in, out);
    break;
  case 16:
    internal::__fastpack16(in, out);
    break;
  case 17:
    internal::__fastpack17(in, out);
    break;
  case 18:
    internal::__fastpack18(in, out);
    break;
  case 19:
    internal::__fastpack19(in, out);
    break;
  case 20:
    internal::__fastpack20(in, out);
    break;
  case 21:
    internal::__fastpack21(in, out);
    break;
  case 22:
    internal::__fastpack22(in, out);
    break;
  case 23:
    internal::__fastpack23(in, out);
    break;
  case 24:
    internal::__fastpack24(in, out);
    break;
  case 25:
    internal::__fastpack25(in, out);
    break;
  case 26:
    internal::__fastpack26(in, out);
    break;
  case 27:
    internal::__fastpack27(in, out);
    break;
  case 28:
    internal::__fastpack28(in, out);
    break;
  case 29:
    internal::__fastpack29(in, out);
    break;
  case 30:
    internal::__fastpack30(in, out);
    break;
  case 31:
    internal::__fastpack31(in, out);
    break;
  case 32:
    internal::__fastpack32(in, out);
    break;
  default:
	throw std::logic_error("Invalid bit width for bitpacking");
  }
}

inline void fastpack(const uint64_t *__restrict in,
                     uint32_t *__restrict out, const uint32_t bit) {
  switch (bit) {
  case 0:
    internal::__fastpack0(in, out);
    break;
  case 1:
    internal::__fastpack1(in, out);
    break;
  case 2:
    internal::__fastpack2(in, out);
    break;
  case 3:
    internal::__fastpack3(in, out);
    break;
  case 4:
    internal::__fastpack4(in, out);
    break;
  case 5:
    internal::__fastpack5(in, out);
    break;
  case 6:
    internal::__fastpack6(in, out);
    break;
  case 7:
    internal::__fastpack7(in, out);
    break;
  case 8:
    internal::__fastpack8(in, out);
    break;
  case 9:
    internal::__fastpack9(in, out);
    break;
  case 10:
    internal::__fastpack10(in, out);
    break;
  case 11:
    internal::__fastpack11(in, out);
    break;
  case 12:
    internal::__fastpack12(in, out);
    break;
  case 13:
    internal::__fastpack13(in, out);
    break;
  case 14:
    internal::__fastpack14(in, out);
    break;
  case 15:
    internal::__fastpack15(in, out);
    break;
  case 16:
    internal::__fastpack16(in, out);
    break;
  case 17:
    internal::__fastpack17(in, out);
    break;
  case 18:
    internal::__fastpack18(in, out);
    break;
  case 19:
    internal::__fastpack19(in, out);
    break;
  case 20:
    internal::__fastpack20(in, out);
    break;
  case 21:
    internal::__fastpack21(in, out);
    break;
  case 22:
    internal::__fastpack22(in, out);
    break;
  case 23:
    internal::__fastpack23(in, out);
    break;
  case 24:
    internal::__fastpack24(in, out);
    break;
  case 25:
    internal::__fastpack25(in, out);
    break;
  case 26:
    internal::__fastpack26(in, out);
    break;
  case 27:
    internal::__fastpack27(in, out);
    break;
  case 28:
    internal::__fastpack28(in, out);
    break;
  case 29:
    internal::__fastpack29(in, out);
    break;
  case 30:
    internal::__fastpack30(in, out);
    break;
  case 31:
    internal::__fastpack31(in, out);
    break;
  case 32:
    internal::__fastpack32(in, out);
    break;
  case 33:
    internal::__fastpack33(in, out);
    break;
  case 34:
    internal::__fastpack34(in, out);
    break;
  case 35:
    internal::__fastpack35(in, out);
    break;
  case 36:
    internal::__fastpack36(in, out);
    break;
  case 37:
    internal::__fastpack37(in, out);
    break;
  case 38:
    internal::__fastpack38(in, out);
    break;
  case 39:
    internal::__fastpack39(in, out);
    break;
  case 40:
    internal::__fastpack40(in, out);
    break;
  case 41:
    internal::__fastpack41(in, out);
    break;
  case 42:
    internal::__fastpack42(in, out);
    break;
  case 43:
    internal::__fastpack43(in, out);
    break;
  case 44:
    internal::__fastpack44(in, out);
    break;
  case 45:
    internal::__fastpack45(in, out);
    break;
  case 46:
    internal::__fastpack46(in, out);
    break;
  case 47:
    internal::__fastpack47(in, out);
    break;
  case 48:
    internal::__fastpack48(in, out);
    break;
  case 49:
    internal::__fastpack49(in, out);
    break;
  case 50:
    internal::__fastpack50(in, out);
    break;
  case 51:
    internal::__fastpack51(in, out);
    break;
  case 52:
    internal::__fastpack52(in, out);
    break;
  case 53:
    internal::__fastpack53(in, out);
    break;
  case 54:
    internal::__fastpack54(in, out);
    break;
  case 55:
    internal::__fastpack55(in, out);
    break;
  case 56:
    internal::__fastpack56(in, out);
    break;
  case 57:
    internal::__fastpack57(in, out);
    break;
  case 58:
    internal::__fastpack58(in, out);
    break;
  case 59:
    internal::__fastpack59(in, out);
    break;
  case 60:
    internal::__fastpack60(in, out);
    break;
  case 61:
    internal::__fastpack61(in, out);
    break;
  case 62:
    internal::__fastpack62(in, out);
    break;
  case 63:
    internal::__fastpack63(in, out);
    break;
  case 64:
    internal::__fastpack64(in, out);
    break;
  default:
	throw std::logic_error("Invalid bit width for bitpacking");
  }
}
} // namespace fastpfor_lib
