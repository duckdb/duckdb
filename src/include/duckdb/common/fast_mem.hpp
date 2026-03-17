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
inline void MemcpyFixed(void *dest, const void *src) {
	memcpy(dest, src, SIZE);
}

template <size_t SIZE>
inline int MemcmpFixed(const void *str1, const void *str2) {
	return memcmp(str1, str2, SIZE);
}

template <size_t SIZE>
inline void MemsetFixed(void *ptr, int value) {
	memset(ptr, value, SIZE);
}

namespace duckdb {

//! This templated memcpy is significantly faster than std::memcpy,
//! but only when you are calling memcpy with a const size in a loop.
//! For instance `while (<cond>) { memcpy(<dest>, <src>, const_size); ... }`
inline void FastMemcpy(void *dest, const void *src, const size_t size) {
	// LCOV_EXCL_START
	switch (size) {
	case 0:
		return;
	case 1:
		return MemcpyFixed<1>(dest, src);
	case 2:
		return MemcpyFixed<2>(dest, src);
	case 3:
		return MemcpyFixed<3>(dest, src);
	case 4:
		return MemcpyFixed<4>(dest, src);
	case 5:
		return MemcpyFixed<5>(dest, src);
	case 6:
		return MemcpyFixed<6>(dest, src);
	case 7:
		return MemcpyFixed<7>(dest, src);
	case 8:
		return MemcpyFixed<8>(dest, src);
	case 9:
		return MemcpyFixed<9>(dest, src);
	case 10:
		return MemcpyFixed<10>(dest, src);
	case 11:
		return MemcpyFixed<11>(dest, src);
	case 12:
		return MemcpyFixed<12>(dest, src);
	case 13:
		return MemcpyFixed<13>(dest, src);
	case 14:
		return MemcpyFixed<14>(dest, src);
	case 15:
		return MemcpyFixed<15>(dest, src);
	case 16:
		return MemcpyFixed<16>(dest, src);
	case 17:
		return MemcpyFixed<17>(dest, src);
	case 18:
		return MemcpyFixed<18>(dest, src);
	case 19:
		return MemcpyFixed<19>(dest, src);
	case 20:
		return MemcpyFixed<20>(dest, src);
	case 21:
		return MemcpyFixed<21>(dest, src);
	case 22:
		return MemcpyFixed<22>(dest, src);
	case 23:
		return MemcpyFixed<23>(dest, src);
	case 24:
		return MemcpyFixed<24>(dest, src);
	case 25:
		return MemcpyFixed<25>(dest, src);
	case 26:
		return MemcpyFixed<26>(dest, src);
	case 27:
		return MemcpyFixed<27>(dest, src);
	case 28:
		return MemcpyFixed<28>(dest, src);
	case 29:
		return MemcpyFixed<29>(dest, src);
	case 30:
		return MemcpyFixed<30>(dest, src);
	case 31:
		return MemcpyFixed<31>(dest, src);
	case 32:
		return MemcpyFixed<32>(dest, src);
	case 33:
		return MemcpyFixed<33>(dest, src);
	case 34:
		return MemcpyFixed<34>(dest, src);
	case 35:
		return MemcpyFixed<35>(dest, src);
	case 36:
		return MemcpyFixed<36>(dest, src);
	case 37:
		return MemcpyFixed<37>(dest, src);
	case 38:
		return MemcpyFixed<38>(dest, src);
	case 39:
		return MemcpyFixed<39>(dest, src);
	case 40:
		return MemcpyFixed<40>(dest, src);
	case 41:
		return MemcpyFixed<41>(dest, src);
	case 42:
		return MemcpyFixed<42>(dest, src);
	case 43:
		return MemcpyFixed<43>(dest, src);
	case 44:
		return MemcpyFixed<44>(dest, src);
	case 45:
		return MemcpyFixed<45>(dest, src);
	case 46:
		return MemcpyFixed<46>(dest, src);
	case 47:
		return MemcpyFixed<47>(dest, src);
	case 48:
		return MemcpyFixed<48>(dest, src);
	case 49:
		return MemcpyFixed<49>(dest, src);
	case 50:
		return MemcpyFixed<50>(dest, src);
	case 51:
		return MemcpyFixed<51>(dest, src);
	case 52:
		return MemcpyFixed<52>(dest, src);
	case 53:
		return MemcpyFixed<53>(dest, src);
	case 54:
		return MemcpyFixed<54>(dest, src);
	case 55:
		return MemcpyFixed<55>(dest, src);
	case 56:
		return MemcpyFixed<56>(dest, src);
	case 57:
		return MemcpyFixed<57>(dest, src);
	case 58:
		return MemcpyFixed<58>(dest, src);
	case 59:
		return MemcpyFixed<59>(dest, src);
	case 60:
		return MemcpyFixed<60>(dest, src);
	case 61:
		return MemcpyFixed<61>(dest, src);
	case 62:
		return MemcpyFixed<62>(dest, src);
	case 63:
		return MemcpyFixed<63>(dest, src);
	case 64:
		return MemcpyFixed<64>(dest, src);
	case 65:
		return MemcpyFixed<65>(dest, src);
	case 66:
		return MemcpyFixed<66>(dest, src);
	case 67:
		return MemcpyFixed<67>(dest, src);
	case 68:
		return MemcpyFixed<68>(dest, src);
	case 69:
		return MemcpyFixed<69>(dest, src);
	case 70:
		return MemcpyFixed<70>(dest, src);
	case 71:
		return MemcpyFixed<71>(dest, src);
	case 72:
		return MemcpyFixed<72>(dest, src);
	case 73:
		return MemcpyFixed<73>(dest, src);
	case 74:
		return MemcpyFixed<74>(dest, src);
	case 75:
		return MemcpyFixed<75>(dest, src);
	case 76:
		return MemcpyFixed<76>(dest, src);
	case 77:
		return MemcpyFixed<77>(dest, src);
	case 78:
		return MemcpyFixed<78>(dest, src);
	case 79:
		return MemcpyFixed<79>(dest, src);
	case 80:
		return MemcpyFixed<80>(dest, src);
	case 81:
		return MemcpyFixed<81>(dest, src);
	case 82:
		return MemcpyFixed<82>(dest, src);
	case 83:
		return MemcpyFixed<83>(dest, src);
	case 84:
		return MemcpyFixed<84>(dest, src);
	case 85:
		return MemcpyFixed<85>(dest, src);
	case 86:
		return MemcpyFixed<86>(dest, src);
	case 87:
		return MemcpyFixed<87>(dest, src);
	case 88:
		return MemcpyFixed<88>(dest, src);
	case 89:
		return MemcpyFixed<89>(dest, src);
	case 90:
		return MemcpyFixed<90>(dest, src);
	case 91:
		return MemcpyFixed<91>(dest, src);
	case 92:
		return MemcpyFixed<92>(dest, src);
	case 93:
		return MemcpyFixed<93>(dest, src);
	case 94:
		return MemcpyFixed<94>(dest, src);
	case 95:
		return MemcpyFixed<95>(dest, src);
	case 96:
		return MemcpyFixed<96>(dest, src);
	case 97:
		return MemcpyFixed<97>(dest, src);
	case 98:
		return MemcpyFixed<98>(dest, src);
	case 99:
		return MemcpyFixed<99>(dest, src);
	case 100:
		return MemcpyFixed<100>(dest, src);
	case 101:
		return MemcpyFixed<101>(dest, src);
	case 102:
		return MemcpyFixed<102>(dest, src);
	case 103:
		return MemcpyFixed<103>(dest, src);
	case 104:
		return MemcpyFixed<104>(dest, src);
	case 105:
		return MemcpyFixed<105>(dest, src);
	case 106:
		return MemcpyFixed<106>(dest, src);
	case 107:
		return MemcpyFixed<107>(dest, src);
	case 108:
		return MemcpyFixed<108>(dest, src);
	case 109:
		return MemcpyFixed<109>(dest, src);
	case 110:
		return MemcpyFixed<110>(dest, src);
	case 111:
		return MemcpyFixed<111>(dest, src);
	case 112:
		return MemcpyFixed<112>(dest, src);
	case 113:
		return MemcpyFixed<113>(dest, src);
	case 114:
		return MemcpyFixed<114>(dest, src);
	case 115:
		return MemcpyFixed<115>(dest, src);
	case 116:
		return MemcpyFixed<116>(dest, src);
	case 117:
		return MemcpyFixed<117>(dest, src);
	case 118:
		return MemcpyFixed<118>(dest, src);
	case 119:
		return MemcpyFixed<119>(dest, src);
	case 120:
		return MemcpyFixed<120>(dest, src);
	case 121:
		return MemcpyFixed<121>(dest, src);
	case 122:
		return MemcpyFixed<122>(dest, src);
	case 123:
		return MemcpyFixed<123>(dest, src);
	case 124:
		return MemcpyFixed<124>(dest, src);
	case 125:
		return MemcpyFixed<125>(dest, src);
	case 126:
		return MemcpyFixed<126>(dest, src);
	case 127:
		return MemcpyFixed<127>(dest, src);
	case 128:
		return MemcpyFixed<128>(dest, src);
	case 129:
		return MemcpyFixed<129>(dest, src);
	case 130:
		return MemcpyFixed<130>(dest, src);
	case 131:
		return MemcpyFixed<131>(dest, src);
	case 132:
		return MemcpyFixed<132>(dest, src);
	case 133:
		return MemcpyFixed<133>(dest, src);
	case 134:
		return MemcpyFixed<134>(dest, src);
	case 135:
		return MemcpyFixed<135>(dest, src);
	case 136:
		return MemcpyFixed<136>(dest, src);
	case 137:
		return MemcpyFixed<137>(dest, src);
	case 138:
		return MemcpyFixed<138>(dest, src);
	case 139:
		return MemcpyFixed<139>(dest, src);
	case 140:
		return MemcpyFixed<140>(dest, src);
	case 141:
		return MemcpyFixed<141>(dest, src);
	case 142:
		return MemcpyFixed<142>(dest, src);
	case 143:
		return MemcpyFixed<143>(dest, src);
	case 144:
		return MemcpyFixed<144>(dest, src);
	case 145:
		return MemcpyFixed<145>(dest, src);
	case 146:
		return MemcpyFixed<146>(dest, src);
	case 147:
		return MemcpyFixed<147>(dest, src);
	case 148:
		return MemcpyFixed<148>(dest, src);
	case 149:
		return MemcpyFixed<149>(dest, src);
	case 150:
		return MemcpyFixed<150>(dest, src);
	case 151:
		return MemcpyFixed<151>(dest, src);
	case 152:
		return MemcpyFixed<152>(dest, src);
	case 153:
		return MemcpyFixed<153>(dest, src);
	case 154:
		return MemcpyFixed<154>(dest, src);
	case 155:
		return MemcpyFixed<155>(dest, src);
	case 156:
		return MemcpyFixed<156>(dest, src);
	case 157:
		return MemcpyFixed<157>(dest, src);
	case 158:
		return MemcpyFixed<158>(dest, src);
	case 159:
		return MemcpyFixed<159>(dest, src);
	case 160:
		return MemcpyFixed<160>(dest, src);
	case 161:
		return MemcpyFixed<161>(dest, src);
	case 162:
		return MemcpyFixed<162>(dest, src);
	case 163:
		return MemcpyFixed<163>(dest, src);
	case 164:
		return MemcpyFixed<164>(dest, src);
	case 165:
		return MemcpyFixed<165>(dest, src);
	case 166:
		return MemcpyFixed<166>(dest, src);
	case 167:
		return MemcpyFixed<167>(dest, src);
	case 168:
		return MemcpyFixed<168>(dest, src);
	case 169:
		return MemcpyFixed<169>(dest, src);
	case 170:
		return MemcpyFixed<170>(dest, src);
	case 171:
		return MemcpyFixed<171>(dest, src);
	case 172:
		return MemcpyFixed<172>(dest, src);
	case 173:
		return MemcpyFixed<173>(dest, src);
	case 174:
		return MemcpyFixed<174>(dest, src);
	case 175:
		return MemcpyFixed<175>(dest, src);
	case 176:
		return MemcpyFixed<176>(dest, src);
	case 177:
		return MemcpyFixed<177>(dest, src);
	case 178:
		return MemcpyFixed<178>(dest, src);
	case 179:
		return MemcpyFixed<179>(dest, src);
	case 180:
		return MemcpyFixed<180>(dest, src);
	case 181:
		return MemcpyFixed<181>(dest, src);
	case 182:
		return MemcpyFixed<182>(dest, src);
	case 183:
		return MemcpyFixed<183>(dest, src);
	case 184:
		return MemcpyFixed<184>(dest, src);
	case 185:
		return MemcpyFixed<185>(dest, src);
	case 186:
		return MemcpyFixed<186>(dest, src);
	case 187:
		return MemcpyFixed<187>(dest, src);
	case 188:
		return MemcpyFixed<188>(dest, src);
	case 189:
		return MemcpyFixed<189>(dest, src);
	case 190:
		return MemcpyFixed<190>(dest, src);
	case 191:
		return MemcpyFixed<191>(dest, src);
	case 192:
		return MemcpyFixed<192>(dest, src);
	case 193:
		return MemcpyFixed<193>(dest, src);
	case 194:
		return MemcpyFixed<194>(dest, src);
	case 195:
		return MemcpyFixed<195>(dest, src);
	case 196:
		return MemcpyFixed<196>(dest, src);
	case 197:
		return MemcpyFixed<197>(dest, src);
	case 198:
		return MemcpyFixed<198>(dest, src);
	case 199:
		return MemcpyFixed<199>(dest, src);
	case 200:
		return MemcpyFixed<200>(dest, src);
	case 201:
		return MemcpyFixed<201>(dest, src);
	case 202:
		return MemcpyFixed<202>(dest, src);
	case 203:
		return MemcpyFixed<203>(dest, src);
	case 204:
		return MemcpyFixed<204>(dest, src);
	case 205:
		return MemcpyFixed<205>(dest, src);
	case 206:
		return MemcpyFixed<206>(dest, src);
	case 207:
		return MemcpyFixed<207>(dest, src);
	case 208:
		return MemcpyFixed<208>(dest, src);
	case 209:
		return MemcpyFixed<209>(dest, src);
	case 210:
		return MemcpyFixed<210>(dest, src);
	case 211:
		return MemcpyFixed<211>(dest, src);
	case 212:
		return MemcpyFixed<212>(dest, src);
	case 213:
		return MemcpyFixed<213>(dest, src);
	case 214:
		return MemcpyFixed<214>(dest, src);
	case 215:
		return MemcpyFixed<215>(dest, src);
	case 216:
		return MemcpyFixed<216>(dest, src);
	case 217:
		return MemcpyFixed<217>(dest, src);
	case 218:
		return MemcpyFixed<218>(dest, src);
	case 219:
		return MemcpyFixed<219>(dest, src);
	case 220:
		return MemcpyFixed<220>(dest, src);
	case 221:
		return MemcpyFixed<221>(dest, src);
	case 222:
		return MemcpyFixed<222>(dest, src);
	case 223:
		return MemcpyFixed<223>(dest, src);
	case 224:
		return MemcpyFixed<224>(dest, src);
	case 225:
		return MemcpyFixed<225>(dest, src);
	case 226:
		return MemcpyFixed<226>(dest, src);
	case 227:
		return MemcpyFixed<227>(dest, src);
	case 228:
		return MemcpyFixed<228>(dest, src);
	case 229:
		return MemcpyFixed<229>(dest, src);
	case 230:
		return MemcpyFixed<230>(dest, src);
	case 231:
		return MemcpyFixed<231>(dest, src);
	case 232:
		return MemcpyFixed<232>(dest, src);
	case 233:
		return MemcpyFixed<233>(dest, src);
	case 234:
		return MemcpyFixed<234>(dest, src);
	case 235:
		return MemcpyFixed<235>(dest, src);
	case 236:
		return MemcpyFixed<236>(dest, src);
	case 237:
		return MemcpyFixed<237>(dest, src);
	case 238:
		return MemcpyFixed<238>(dest, src);
	case 239:
		return MemcpyFixed<239>(dest, src);
	case 240:
		return MemcpyFixed<240>(dest, src);
	case 241:
		return MemcpyFixed<241>(dest, src);
	case 242:
		return MemcpyFixed<242>(dest, src);
	case 243:
		return MemcpyFixed<243>(dest, src);
	case 244:
		return MemcpyFixed<244>(dest, src);
	case 245:
		return MemcpyFixed<245>(dest, src);
	case 246:
		return MemcpyFixed<246>(dest, src);
	case 247:
		return MemcpyFixed<247>(dest, src);
	case 248:
		return MemcpyFixed<248>(dest, src);
	case 249:
		return MemcpyFixed<249>(dest, src);
	case 250:
		return MemcpyFixed<250>(dest, src);
	case 251:
		return MemcpyFixed<251>(dest, src);
	case 252:
		return MemcpyFixed<252>(dest, src);
	case 253:
		return MemcpyFixed<253>(dest, src);
	case 254:
		return MemcpyFixed<254>(dest, src);
	case 255:
		return MemcpyFixed<255>(dest, src);
	case 256:
		return MemcpyFixed<256>(dest, src);
	default:
		memcpy(dest, src, size);
	}
	// LCOV_EXCL_STOP
}

//! This templated memcmp is significantly faster than std::memcmp,
//! but only when you are calling memcmp with a const size in a loop.
//! For instance `while (<cond>) { memcmp(<str1>, <str2>, const_size); ... }`
inline int FastMemcmp(const void *str1, const void *str2, const size_t size) {
	// LCOV_EXCL_START
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
	case 33:
		return MemcmpFixed<33>(str1, str2);
	case 34:
		return MemcmpFixed<34>(str1, str2);
	case 35:
		return MemcmpFixed<35>(str1, str2);
	case 36:
		return MemcmpFixed<36>(str1, str2);
	case 37:
		return MemcmpFixed<37>(str1, str2);
	case 38:
		return MemcmpFixed<38>(str1, str2);
	case 39:
		return MemcmpFixed<39>(str1, str2);
	case 40:
		return MemcmpFixed<40>(str1, str2);
	case 41:
		return MemcmpFixed<41>(str1, str2);
	case 42:
		return MemcmpFixed<42>(str1, str2);
	case 43:
		return MemcmpFixed<43>(str1, str2);
	case 44:
		return MemcmpFixed<44>(str1, str2);
	case 45:
		return MemcmpFixed<45>(str1, str2);
	case 46:
		return MemcmpFixed<46>(str1, str2);
	case 47:
		return MemcmpFixed<47>(str1, str2);
	case 48:
		return MemcmpFixed<48>(str1, str2);
	case 49:
		return MemcmpFixed<49>(str1, str2);
	case 50:
		return MemcmpFixed<50>(str1, str2);
	case 51:
		return MemcmpFixed<51>(str1, str2);
	case 52:
		return MemcmpFixed<52>(str1, str2);
	case 53:
		return MemcmpFixed<53>(str1, str2);
	case 54:
		return MemcmpFixed<54>(str1, str2);
	case 55:
		return MemcmpFixed<55>(str1, str2);
	case 56:
		return MemcmpFixed<56>(str1, str2);
	case 57:
		return MemcmpFixed<57>(str1, str2);
	case 58:
		return MemcmpFixed<58>(str1, str2);
	case 59:
		return MemcmpFixed<59>(str1, str2);
	case 60:
		return MemcmpFixed<60>(str1, str2);
	case 61:
		return MemcmpFixed<61>(str1, str2);
	case 62:
		return MemcmpFixed<62>(str1, str2);
	case 63:
		return MemcmpFixed<63>(str1, str2);
	case 64:
		return MemcmpFixed<64>(str1, str2);
	default:
		return memcmp(str1, str2, size);
	}
	// LCOV_EXCL_STOP
}

inline void FastMemset(void *ptr, int value, size_t size) {
	// LCOV_EXCL_START
	switch (size) {
	case 0:
		return;
	case 1:
		return MemsetFixed<1>(ptr, value);
	case 2:
		return MemsetFixed<2>(ptr, value);
	case 3:
		return MemsetFixed<3>(ptr, value);
	case 4:
		return MemsetFixed<4>(ptr, value);
	case 5:
		return MemsetFixed<5>(ptr, value);
	case 6:
		return MemsetFixed<6>(ptr, value);
	case 7:
		return MemsetFixed<7>(ptr, value);
	case 8:
		return MemsetFixed<8>(ptr, value);
	case 9:
		return MemsetFixed<9>(ptr, value);
	case 10:
		return MemsetFixed<10>(ptr, value);
	case 11:
		return MemsetFixed<11>(ptr, value);
	case 12:
		return MemsetFixed<12>(ptr, value);
	case 13:
		return MemsetFixed<13>(ptr, value);
	case 14:
		return MemsetFixed<14>(ptr, value);
	case 15:
		return MemsetFixed<15>(ptr, value);
	case 16:
		return MemsetFixed<16>(ptr, value);
	case 17:
		return MemsetFixed<17>(ptr, value);
	case 18:
		return MemsetFixed<18>(ptr, value);
	case 19:
		return MemsetFixed<19>(ptr, value);
	case 20:
		return MemsetFixed<20>(ptr, value);
	case 21:
		return MemsetFixed<21>(ptr, value);
	case 22:
		return MemsetFixed<22>(ptr, value);
	case 23:
		return MemsetFixed<23>(ptr, value);
	case 24:
		return MemsetFixed<24>(ptr, value);
	case 25:
		return MemsetFixed<25>(ptr, value);
	case 26:
		return MemsetFixed<26>(ptr, value);
	case 27:
		return MemsetFixed<27>(ptr, value);
	case 28:
		return MemsetFixed<28>(ptr, value);
	case 29:
		return MemsetFixed<29>(ptr, value);
	case 30:
		return MemsetFixed<30>(ptr, value);
	case 31:
		return MemsetFixed<31>(ptr, value);
	case 32:
		return MemsetFixed<32>(ptr, value);
	case 33:
		return MemsetFixed<33>(ptr, value);
	case 34:
		return MemsetFixed<34>(ptr, value);
	case 35:
		return MemsetFixed<35>(ptr, value);
	case 36:
		return MemsetFixed<36>(ptr, value);
	case 37:
		return MemsetFixed<37>(ptr, value);
	case 38:
		return MemsetFixed<38>(ptr, value);
	case 39:
		return MemsetFixed<39>(ptr, value);
	case 40:
		return MemsetFixed<40>(ptr, value);
	case 41:
		return MemsetFixed<41>(ptr, value);
	case 42:
		return MemsetFixed<42>(ptr, value);
	case 43:
		return MemsetFixed<43>(ptr, value);
	case 44:
		return MemsetFixed<44>(ptr, value);
	case 45:
		return MemsetFixed<45>(ptr, value);
	case 46:
		return MemsetFixed<46>(ptr, value);
	case 47:
		return MemsetFixed<47>(ptr, value);
	case 48:
		return MemsetFixed<48>(ptr, value);
	case 49:
		return MemsetFixed<49>(ptr, value);
	case 50:
		return MemsetFixed<50>(ptr, value);
	case 51:
		return MemsetFixed<51>(ptr, value);
	case 52:
		return MemsetFixed<52>(ptr, value);
	case 53:
		return MemsetFixed<53>(ptr, value);
	case 54:
		return MemsetFixed<54>(ptr, value);
	case 55:
		return MemsetFixed<55>(ptr, value);
	case 56:
		return MemsetFixed<56>(ptr, value);
	case 57:
		return MemsetFixed<57>(ptr, value);
	case 58:
		return MemsetFixed<58>(ptr, value);
	case 59:
		return MemsetFixed<59>(ptr, value);
	case 60:
		return MemsetFixed<60>(ptr, value);
	case 61:
		return MemsetFixed<61>(ptr, value);
	case 62:
		return MemsetFixed<62>(ptr, value);
	case 63:
		return MemsetFixed<63>(ptr, value);
	case 64:
		return MemsetFixed<64>(ptr, value);
	case 65:
		return MemsetFixed<65>(ptr, value);
	case 66:
		return MemsetFixed<66>(ptr, value);
	case 67:
		return MemsetFixed<67>(ptr, value);
	case 68:
		return MemsetFixed<68>(ptr, value);
	case 69:
		return MemsetFixed<69>(ptr, value);
	case 70:
		return MemsetFixed<70>(ptr, value);
	case 71:
		return MemsetFixed<71>(ptr, value);
	case 72:
		return MemsetFixed<72>(ptr, value);
	case 73:
		return MemsetFixed<73>(ptr, value);
	case 74:
		return MemsetFixed<74>(ptr, value);
	case 75:
		return MemsetFixed<75>(ptr, value);
	case 76:
		return MemsetFixed<76>(ptr, value);
	case 77:
		return MemsetFixed<77>(ptr, value);
	case 78:
		return MemsetFixed<78>(ptr, value);
	case 79:
		return MemsetFixed<79>(ptr, value);
	case 80:
		return MemsetFixed<80>(ptr, value);
	case 81:
		return MemsetFixed<81>(ptr, value);
	case 82:
		return MemsetFixed<82>(ptr, value);
	case 83:
		return MemsetFixed<83>(ptr, value);
	case 84:
		return MemsetFixed<84>(ptr, value);
	case 85:
		return MemsetFixed<85>(ptr, value);
	case 86:
		return MemsetFixed<86>(ptr, value);
	case 87:
		return MemsetFixed<87>(ptr, value);
	case 88:
		return MemsetFixed<88>(ptr, value);
	case 89:
		return MemsetFixed<89>(ptr, value);
	case 90:
		return MemsetFixed<90>(ptr, value);
	case 91:
		return MemsetFixed<91>(ptr, value);
	case 92:
		return MemsetFixed<92>(ptr, value);
	case 93:
		return MemsetFixed<93>(ptr, value);
	case 94:
		return MemsetFixed<94>(ptr, value);
	case 95:
		return MemsetFixed<95>(ptr, value);
	case 96:
		return MemsetFixed<96>(ptr, value);
	case 97:
		return MemsetFixed<97>(ptr, value);
	case 98:
		return MemsetFixed<98>(ptr, value);
	case 99:
		return MemsetFixed<99>(ptr, value);
	case 100:
		return MemsetFixed<100>(ptr, value);
	case 101:
		return MemsetFixed<101>(ptr, value);
	case 102:
		return MemsetFixed<102>(ptr, value);
	case 103:
		return MemsetFixed<103>(ptr, value);
	case 104:
		return MemsetFixed<104>(ptr, value);
	case 105:
		return MemsetFixed<105>(ptr, value);
	case 106:
		return MemsetFixed<106>(ptr, value);
	case 107:
		return MemsetFixed<107>(ptr, value);
	case 108:
		return MemsetFixed<108>(ptr, value);
	case 109:
		return MemsetFixed<109>(ptr, value);
	case 110:
		return MemsetFixed<110>(ptr, value);
	case 111:
		return MemsetFixed<111>(ptr, value);
	case 112:
		return MemsetFixed<112>(ptr, value);
	case 113:
		return MemsetFixed<113>(ptr, value);
	case 114:
		return MemsetFixed<114>(ptr, value);
	case 115:
		return MemsetFixed<115>(ptr, value);
	case 116:
		return MemsetFixed<116>(ptr, value);
	case 117:
		return MemsetFixed<117>(ptr, value);
	case 118:
		return MemsetFixed<118>(ptr, value);
	case 119:
		return MemsetFixed<119>(ptr, value);
	case 120:
		return MemsetFixed<120>(ptr, value);
	case 121:
		return MemsetFixed<121>(ptr, value);
	case 122:
		return MemsetFixed<122>(ptr, value);
	case 123:
		return MemsetFixed<123>(ptr, value);
	case 124:
		return MemsetFixed<124>(ptr, value);
	case 125:
		return MemsetFixed<125>(ptr, value);
	case 126:
		return MemsetFixed<126>(ptr, value);
	case 127:
		return MemsetFixed<127>(ptr, value);
	case 128:
		return MemsetFixed<128>(ptr, value);
	case 129:
		return MemsetFixed<129>(ptr, value);
	case 130:
		return MemsetFixed<130>(ptr, value);
	case 131:
		return MemsetFixed<131>(ptr, value);
	case 132:
		return MemsetFixed<132>(ptr, value);
	case 133:
		return MemsetFixed<133>(ptr, value);
	case 134:
		return MemsetFixed<134>(ptr, value);
	case 135:
		return MemsetFixed<135>(ptr, value);
	case 136:
		return MemsetFixed<136>(ptr, value);
	case 137:
		return MemsetFixed<137>(ptr, value);
	case 138:
		return MemsetFixed<138>(ptr, value);
	case 139:
		return MemsetFixed<139>(ptr, value);
	case 140:
		return MemsetFixed<140>(ptr, value);
	case 141:
		return MemsetFixed<141>(ptr, value);
	case 142:
		return MemsetFixed<142>(ptr, value);
	case 143:
		return MemsetFixed<143>(ptr, value);
	case 144:
		return MemsetFixed<144>(ptr, value);
	case 145:
		return MemsetFixed<145>(ptr, value);
	case 146:
		return MemsetFixed<146>(ptr, value);
	case 147:
		return MemsetFixed<147>(ptr, value);
	case 148:
		return MemsetFixed<148>(ptr, value);
	case 149:
		return MemsetFixed<149>(ptr, value);
	case 150:
		return MemsetFixed<150>(ptr, value);
	case 151:
		return MemsetFixed<151>(ptr, value);
	case 152:
		return MemsetFixed<152>(ptr, value);
	case 153:
		return MemsetFixed<153>(ptr, value);
	case 154:
		return MemsetFixed<154>(ptr, value);
	case 155:
		return MemsetFixed<155>(ptr, value);
	case 156:
		return MemsetFixed<156>(ptr, value);
	case 157:
		return MemsetFixed<157>(ptr, value);
	case 158:
		return MemsetFixed<158>(ptr, value);
	case 159:
		return MemsetFixed<159>(ptr, value);
	case 160:
		return MemsetFixed<160>(ptr, value);
	case 161:
		return MemsetFixed<161>(ptr, value);
	case 162:
		return MemsetFixed<162>(ptr, value);
	case 163:
		return MemsetFixed<163>(ptr, value);
	case 164:
		return MemsetFixed<164>(ptr, value);
	case 165:
		return MemsetFixed<165>(ptr, value);
	case 166:
		return MemsetFixed<166>(ptr, value);
	case 167:
		return MemsetFixed<167>(ptr, value);
	case 168:
		return MemsetFixed<168>(ptr, value);
	case 169:
		return MemsetFixed<169>(ptr, value);
	case 170:
		return MemsetFixed<170>(ptr, value);
	case 171:
		return MemsetFixed<171>(ptr, value);
	case 172:
		return MemsetFixed<172>(ptr, value);
	case 173:
		return MemsetFixed<173>(ptr, value);
	case 174:
		return MemsetFixed<174>(ptr, value);
	case 175:
		return MemsetFixed<175>(ptr, value);
	case 176:
		return MemsetFixed<176>(ptr, value);
	case 177:
		return MemsetFixed<177>(ptr, value);
	case 178:
		return MemsetFixed<178>(ptr, value);
	case 179:
		return MemsetFixed<179>(ptr, value);
	case 180:
		return MemsetFixed<180>(ptr, value);
	case 181:
		return MemsetFixed<181>(ptr, value);
	case 182:
		return MemsetFixed<182>(ptr, value);
	case 183:
		return MemsetFixed<183>(ptr, value);
	case 184:
		return MemsetFixed<184>(ptr, value);
	case 185:
		return MemsetFixed<185>(ptr, value);
	case 186:
		return MemsetFixed<186>(ptr, value);
	case 187:
		return MemsetFixed<187>(ptr, value);
	case 188:
		return MemsetFixed<188>(ptr, value);
	case 189:
		return MemsetFixed<189>(ptr, value);
	case 190:
		return MemsetFixed<190>(ptr, value);
	case 191:
		return MemsetFixed<191>(ptr, value);
	case 192:
		return MemsetFixed<192>(ptr, value);
	case 193:
		return MemsetFixed<193>(ptr, value);
	case 194:
		return MemsetFixed<194>(ptr, value);
	case 195:
		return MemsetFixed<195>(ptr, value);
	case 196:
		return MemsetFixed<196>(ptr, value);
	case 197:
		return MemsetFixed<197>(ptr, value);
	case 198:
		return MemsetFixed<198>(ptr, value);
	case 199:
		return MemsetFixed<199>(ptr, value);
	case 200:
		return MemsetFixed<200>(ptr, value);
	case 201:
		return MemsetFixed<201>(ptr, value);
	case 202:
		return MemsetFixed<202>(ptr, value);
	case 203:
		return MemsetFixed<203>(ptr, value);
	case 204:
		return MemsetFixed<204>(ptr, value);
	case 205:
		return MemsetFixed<205>(ptr, value);
	case 206:
		return MemsetFixed<206>(ptr, value);
	case 207:
		return MemsetFixed<207>(ptr, value);
	case 208:
		return MemsetFixed<208>(ptr, value);
	case 209:
		return MemsetFixed<209>(ptr, value);
	case 210:
		return MemsetFixed<210>(ptr, value);
	case 211:
		return MemsetFixed<211>(ptr, value);
	case 212:
		return MemsetFixed<212>(ptr, value);
	case 213:
		return MemsetFixed<213>(ptr, value);
	case 214:
		return MemsetFixed<214>(ptr, value);
	case 215:
		return MemsetFixed<215>(ptr, value);
	case 216:
		return MemsetFixed<216>(ptr, value);
	case 217:
		return MemsetFixed<217>(ptr, value);
	case 218:
		return MemsetFixed<218>(ptr, value);
	case 219:
		return MemsetFixed<219>(ptr, value);
	case 220:
		return MemsetFixed<220>(ptr, value);
	case 221:
		return MemsetFixed<221>(ptr, value);
	case 222:
		return MemsetFixed<222>(ptr, value);
	case 223:
		return MemsetFixed<223>(ptr, value);
	case 224:
		return MemsetFixed<224>(ptr, value);
	case 225:
		return MemsetFixed<225>(ptr, value);
	case 226:
		return MemsetFixed<226>(ptr, value);
	case 227:
		return MemsetFixed<227>(ptr, value);
	case 228:
		return MemsetFixed<228>(ptr, value);
	case 229:
		return MemsetFixed<229>(ptr, value);
	case 230:
		return MemsetFixed<230>(ptr, value);
	case 231:
		return MemsetFixed<231>(ptr, value);
	case 232:
		return MemsetFixed<232>(ptr, value);
	case 233:
		return MemsetFixed<233>(ptr, value);
	case 234:
		return MemsetFixed<234>(ptr, value);
	case 235:
		return MemsetFixed<235>(ptr, value);
	case 236:
		return MemsetFixed<236>(ptr, value);
	case 237:
		return MemsetFixed<237>(ptr, value);
	case 238:
		return MemsetFixed<238>(ptr, value);
	case 239:
		return MemsetFixed<239>(ptr, value);
	case 240:
		return MemsetFixed<240>(ptr, value);
	case 241:
		return MemsetFixed<241>(ptr, value);
	case 242:
		return MemsetFixed<242>(ptr, value);
	case 243:
		return MemsetFixed<243>(ptr, value);
	case 244:
		return MemsetFixed<244>(ptr, value);
	case 245:
		return MemsetFixed<245>(ptr, value);
	case 246:
		return MemsetFixed<246>(ptr, value);
	case 247:
		return MemsetFixed<247>(ptr, value);
	case 248:
		return MemsetFixed<248>(ptr, value);
	case 249:
		return MemsetFixed<249>(ptr, value);
	case 250:
		return MemsetFixed<250>(ptr, value);
	case 251:
		return MemsetFixed<251>(ptr, value);
	case 252:
		return MemsetFixed<252>(ptr, value);
	case 253:
		return MemsetFixed<253>(ptr, value);
	case 254:
		return MemsetFixed<254>(ptr, value);
	case 255:
		return MemsetFixed<255>(ptr, value);
	case 256:
		return MemsetFixed<256>(ptr, value);
	default:
		memset(ptr, value, size);
	}
	// LCOV_EXCL_STOP
}

} // namespace duckdb
