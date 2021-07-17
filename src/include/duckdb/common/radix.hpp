//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/bit_operations.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/string_type.hpp"

namespace duckdb {

#define BSWAP16(x) ((uint16_t)((((uint16_t)(x)&0xff00) >> 8) | (((uint16_t)(x)&0x00ff) << 8)))

#define BSWAP32(x)                                                                                                     \
	((uint32_t)((((uint32_t)(x)&0xff000000) >> 24) | (((uint32_t)(x)&0x00ff0000) >> 8) |                               \
	            (((uint32_t)(x)&0x0000ff00) << 8) | (((uint32_t)(x)&0x000000ff) << 24)))

#define BSWAP64(x)                                                                                                     \
	((uint64_t)((((uint64_t)(x)&0xff00000000000000ull) >> 56) | (((uint64_t)(x)&0x00ff000000000000ull) >> 40) |        \
	            (((uint64_t)(x)&0x0000ff0000000000ull) >> 24) | (((uint64_t)(x)&0x000000ff00000000ull) >> 8) |         \
	            (((uint64_t)(x)&0x00000000ff000000ull) << 8) | (((uint64_t)(x)&0x0000000000ff0000ull) << 24) |         \
	            (((uint64_t)(x)&0x000000000000ff00ull) << 40) | (((uint64_t)(x)&0x00000000000000ffull) << 56)))

bool IsLittleEndian();
uint8_t FlipSign(uint8_t key_byte);
uint32_t EncodeFloat(float x);
uint64_t EncodeDouble(double x);

template <class T>
void EncodeData(data_ptr_t dataptr, T value, bool is_little_endian) {
	throw NotImplementedException("Cannot create data from this type");
}

template <>
void EncodeData(data_ptr_t dataptr, bool value, bool is_little_endian);
template <>
void EncodeData(data_ptr_t dataptr, int8_t value, bool is_little_endian);
template <>
void EncodeData(data_ptr_t dataptr, int16_t value, bool is_little_endian);
template <>
void EncodeData(data_ptr_t dataptr, int32_t value, bool is_little_endian);
template <>
void EncodeData(data_ptr_t dataptr, int64_t value, bool is_little_endian);
template <>
void EncodeData(data_ptr_t dataptr, uint8_t value, bool is_little_endian);
template <>
void EncodeData(data_ptr_t dataptr, uint16_t value, bool is_little_endian);
template <>
void EncodeData(data_ptr_t dataptr, uint32_t value, bool is_little_endian);
template <>
void EncodeData(data_ptr_t dataptr, uint64_t value, bool is_little_endian);
template <>
void EncodeData(data_ptr_t dataptr, hugeint_t value, bool is_little_endian);
template <>
void EncodeData(data_ptr_t dataptr, double value, bool is_little_endian);
template <>
void EncodeData(data_ptr_t dataptr, float value, bool is_little_endian);
template <>
void EncodeData(data_ptr_t dataptr, interval_t value, bool is_little_endian);

void EncodeStringDataPrefix(data_ptr_t dataptr, string_t value, idx_t prefix_len);

} // namespace duckdb
