//                         DuckDB
//
// duckdb/common/fast_mem.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types.hpp"

template <size_t SIZE>
static inline void MemcpyFixed(void *dest, const void *src) {
	memcpy(dest, src, SIZE);
}

template <size_t SIZE>
static inline int MemcmpFixed(const void *str1, const void *str2) {
	return memcmp(str1, str2, SIZE);
}

namespace duckdb {

//! This templated memcpy is significantly faster than std::memcpy,
//! but only when you are calling memcpy with a const size in a loop.
//! For instance `while (<cond>) { memcpy(<dest>, <src>, const_size); ... }`
static inline void FastMemcpy(void *dest, const void *src, const size_t size) {
	D_ASSERT(size % 8 == 0);
	switch (size) {
	case 0:
		return;
	case 8:
		return MemcpyFixed<8>(dest, src);
	case 16:
		return MemcpyFixed<16>(dest, src);
	case 24:
		return MemcpyFixed<24>(dest, src);
	case 32:
		return MemcpyFixed<32>(dest, src);
	case 40:
		return MemcpyFixed<40>(dest, src);
	case 48:
		return MemcpyFixed<48>(dest, src);
	case 56:
		return MemcpyFixed<56>(dest, src);
	case 64:
		return MemcpyFixed<64>(dest, src);
	case 72:
		return MemcpyFixed<72>(dest, src);
	case 80:
		return MemcpyFixed<80>(dest, src);
	case 88:
		return MemcpyFixed<88>(dest, src);
	case 96:
		return MemcpyFixed<96>(dest, src);
	case 104:
		return MemcpyFixed<104>(dest, src);
	case 112:
		return MemcpyFixed<112>(dest, src);
	case 120:
		return MemcpyFixed<120>(dest, src);
	case 128:
		return MemcpyFixed<128>(dest, src);
	case 136:
		return MemcpyFixed<136>(dest, src);
	case 144:
		return MemcpyFixed<144>(dest, src);
	case 152:
		return MemcpyFixed<152>(dest, src);
	case 160:
		return MemcpyFixed<160>(dest, src);
	case 168:
		return MemcpyFixed<168>(dest, src);
	case 176:
		return MemcpyFixed<176>(dest, src);
	case 184:
		return MemcpyFixed<184>(dest, src);
	case 192:
		return MemcpyFixed<192>(dest, src);
	case 200:
		return MemcpyFixed<200>(dest, src);
	case 208:
		return MemcpyFixed<208>(dest, src);
	case 216:
		return MemcpyFixed<216>(dest, src);
	case 224:
		return MemcpyFixed<224>(dest, src);
	case 232:
		return MemcpyFixed<232>(dest, src);
	case 240:
		return MemcpyFixed<240>(dest, src);
	case 248:
		return MemcpyFixed<248>(dest, src);
	case 256:
		return MemcpyFixed<256>(dest, src);
	default:
		MemcpyFixed<256>(dest, src);
		return FastMemcpy(reinterpret_cast<unsigned char *>(dest) + 256,
		                  reinterpret_cast<const unsigned char *>(src) + 256, size - 256);
	}
}

//! This templated memcmp is significantly faster than std::memcmp,
//! but only when you are calling memcmp with a const size in a loop.
//! For instance `while (<cond>) { memcmp(<str1>, <str2>, const_size); ... }`
static inline int FastMemcmp(const void *str1, const void *str2, const size_t size) {
	switch (size) {
	case 0:
		return 0;
	case 1:
		return MemcmpFixed<1>(str1, str2);
	case 2:
		return MemcmpFixed<2>(str1, str2);
	case 3:
		return MemcmpFixed<3>(str1, str2);
	case 4:
		return MemcmpFixed<4>(str1, str2);
	case 5:
		return MemcmpFixed<5>(str1, str2);
	case 6:
		return MemcmpFixed<6>(str1, str2);
	case 7:
		return MemcmpFixed<7>(str1, str2);
	case 8:
		return MemcmpFixed<8>(str1, str2);
	case 9:
		return MemcmpFixed<9>(str1, str2);
	case 10:
		return MemcmpFixed<10>(str1, str2);
	case 11:
		return MemcmpFixed<11>(str1, str2);
	case 12:
		return MemcmpFixed<12>(str1, str2);
	case 13:
		return MemcmpFixed<13>(str1, str2);
	case 14:
		return MemcmpFixed<14>(str1, str2);
	case 15:
		return MemcmpFixed<15>(str1, str2);
	case 16:
		return MemcmpFixed<16>(str1, str2);
	case 17:
		return MemcmpFixed<17>(str1, str2);
	case 18:
		return MemcmpFixed<18>(str1, str2);
	case 19:
		return MemcmpFixed<19>(str1, str2);
	case 20:
		return MemcmpFixed<20>(str1, str2);
	case 21:
		return MemcmpFixed<21>(str1, str2);
	case 22:
		return MemcmpFixed<22>(str1, str2);
	case 23:
		return MemcmpFixed<23>(str1, str2);
	case 24:
		return MemcmpFixed<24>(str1, str2);
	case 25:
		return MemcmpFixed<25>(str1, str2);
	case 26:
		return MemcmpFixed<26>(str1, str2);
	case 27:
		return MemcmpFixed<27>(str1, str2);
	case 28:
		return MemcmpFixed<28>(str1, str2);
	case 29:
		return MemcmpFixed<29>(str1, str2);
	case 30:
		return MemcmpFixed<30>(str1, str2);
	case 31:
		return MemcmpFixed<31>(str1, str2);
	case 32:
		return MemcmpFixed<32>(str1, str2);
	default:
		return memcmp(str1, str2, size);
	}
}

} // namespace duckdb
