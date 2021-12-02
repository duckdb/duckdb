// Copyright 2011 Google Inc. All Rights Reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of Google Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
//
// Various stubs for the open-source version of Snappy.

#ifndef THIRD_PARTY_SNAPPY_OPENSOURCE_SNAPPY_STUBS_INTERNAL_H_
#define THIRD_PARTY_SNAPPY_OPENSOURCE_SNAPPY_STUBS_INTERNAL_H_

// #ifdef HAVE_CONFIG_H
// #include "config.h"
// #endif

#include <string>

#include <assert.h>
#include <stdlib.h>
#include <string.h>

#ifdef HAVE_SYS_MMAN_H
#include <sys/mman.h>
#endif

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

#if defined(_MSC_VER)
#include <intrin.h>
#endif  // defined(_MSC_VER)

#ifndef __has_feature
#define __has_feature(x) 0
#endif

#if __has_feature(memory_sanitizer)
#include <sanitizer/msan_interface.h>
#define SNAPPY_ANNOTATE_MEMORY_IS_INITIALIZED(address, size) \
    __msan_unpoison((address), (size))
#else
#define SNAPPY_ANNOTATE_MEMORY_IS_INITIALIZED(address, size) /* empty */
#endif  // __has_feature(memory_sanitizer)

#include "snappy-stubs-public.h"

#if defined(__x86_64__)

// Enable 64-bit optimized versions of some routines.
#define ARCH_K8 1

#elif defined(__ppc64__)

#define ARCH_PPC 1

#elif defined(__aarch64__)

#define ARCH_ARM 1

#endif

// Needed by OS X, among others.
#ifndef MAP_ANONYMOUS
#define MAP_ANONYMOUS MAP_ANON
#endif

// The size of an array, if known at compile-time.
// Will give unexpected results if used on a pointer.
// We undefine it first, since some compilers already have a definition.
#ifdef ARRAYSIZE
#undef ARRAYSIZE
#endif
#define ARRAYSIZE(a) (sizeof(a) / sizeof(*(a)))

// Static prediction hints.
#ifdef HAVE_BUILTIN_EXPECT
#define SNAPPY_PREDICT_FALSE(x) (__builtin_expect(x, 0))
#define SNAPPY_PREDICT_TRUE(x) (__builtin_expect(!!(x), 1))
#else
#define SNAPPY_PREDICT_FALSE(x) x
#define SNAPPY_PREDICT_TRUE(x) x
#endif

// This is only used for recomputing the tag byte table used during
// decompression; for simplicity we just remove it from the open-source
// version (anyone who wants to regenerate it can just do the call
// themselves within main()).
#define DEFINE_bool(flag_name, default_value, description) \
  bool FLAGS_ ## flag_name = default_value
#define DECLARE_bool(flag_name) \
  extern bool FLAGS_ ## flag_name

namespace duckdb_snappy {

//static const uint32 kuint32max = static_cast<uint32>(0xFFFFFFFF);
//static const int64 kint64max = static_cast<int64>(0x7FFFFFFFFFFFFFFFLL);


// HM: Always use aligned load to keep ourselves out of trouble. Sorry.

inline uint16 UNALIGNED_LOAD16(const void *p) {
  uint16 t;
  memcpy(&t, p, sizeof t);
  return t;
}

inline uint32 UNALIGNED_LOAD32(const void *p) {
  uint32 t;
  memcpy(&t, p, sizeof t);
  return t;
}

inline uint64 UNALIGNED_LOAD64(const void *p) {
  uint64 t;
  memcpy(&t, p, sizeof t);
  return t;
}

inline void UNALIGNED_STORE16(void *p, uint16 v) {
  memcpy(p, &v, sizeof v);
}

inline void UNALIGNED_STORE32(void *p, uint32 v) {
  memcpy(p, &v, sizeof v);
}

inline void UNALIGNED_STORE64(void *p, uint64 v) {
  memcpy(p, &v, sizeof v);
}


// The following guarantees declaration of the byte swap functions.
#if defined(SNAPPY_IS_BIG_ENDIAN)

#ifdef HAVE_SYS_BYTEORDER_H
#include <sys/byteorder.h>
#endif

#ifdef HAVE_SYS_ENDIAN_H
#include <sys/endian.h>
#endif

#ifdef _MSC_VER
#include <stdlib.h>
#define bswap_16(x) _byteswap_ushort(x)
#define bswap_32(x) _byteswap_ulong(x)
#define bswap_64(x) _byteswap_uint64(x)

#elif defined(__APPLE__)
// Mac OS X / Darwin features
#include <libkern/OSByteOrder.h>
#define bswap_16(x) OSSwapInt16(x)
#define bswap_32(x) OSSwapInt32(x)
#define bswap_64(x) OSSwapInt64(x)

#elif defined(HAVE_BYTESWAP_H)
#include <byteswap.h>

#elif defined(bswap32)
// FreeBSD defines bswap{16,32,64} in <sys/endian.h> (already #included).
#define bswap_16(x) bswap16(x)
#define bswap_32(x) bswap32(x)
#define bswap_64(x) bswap64(x)

#elif defined(BSWAP_64)
// Solaris 10 defines BSWAP_{16,32,64} in <sys/byteorder.h> (already #included).
#define bswap_16(x) BSWAP_16(x)
#define bswap_32(x) BSWAP_32(x)
#define bswap_64(x) BSWAP_64(x)

#else

inline uint16 bswap_16(uint16 x) {
  return (x << 8) | (x >> 8);
}

inline uint32 bswap_32(uint32 x) {
  x = ((x & 0xff00ff00UL) >> 8) | ((x & 0x00ff00ffUL) << 8);
  return (x >> 16) | (x << 16);
}

inline uint64 bswap_64(uint64 x) {
  x = ((x & 0xff00ff00ff00ff00ULL) >> 8) | ((x & 0x00ff00ff00ff00ffULL) << 8);
  x = ((x & 0xffff0000ffff0000ULL) >> 16) | ((x & 0x0000ffff0000ffffULL) << 16);
  return (x >> 32) | (x << 32);
}

#endif

#endif  // defined(SNAPPY_IS_BIG_ENDIAN)

// Convert to little-endian storage, opposite of network format.
// Convert x from host to little endian: x = LittleEndian.FromHost(x);
// convert x from little endian to host: x = LittleEndian.ToHost(x);
//
//  Store values into unaligned memory converting to little endian order:
//    LittleEndian.Store16(p, x);
//
//  Load unaligned values stored in little endian converting to host order:
//    x = LittleEndian.Load16(p);
class LittleEndian {
 public:
  // Conversion functions.
#if defined(SNAPPY_IS_BIG_ENDIAN)

  static uint16 FromHost16(uint16 x) { return bswap_16(x); }
  static uint16 ToHost16(uint16 x) { return bswap_16(x); }

  static uint32 FromHost32(uint32 x) { return bswap_32(x); }
  static uint32 ToHost32(uint32 x) { return bswap_32(x); }

  static bool IsLittleEndian() { return false; }

#else  // !defined(SNAPPY_IS_BIG_ENDIAN)

  static uint16 FromHost16(uint16 x) { return x; }
  static uint16 ToHost16(uint16 x) { return x; }

  static uint32 FromHost32(uint32 x) { return x; }
  static uint32 ToHost32(uint32 x) { return x; }

  static bool IsLittleEndian() { return true; }

#endif  // !defined(SNAPPY_IS_BIG_ENDIAN)

  // Functions to do unaligned loads and stores in little-endian order.
  static uint16 Load16(const void *p) {
    return ToHost16(UNALIGNED_LOAD16(p));
  }

  static void Store16(void *p, uint16 v) {
    UNALIGNED_STORE16(p, FromHost16(v));
  }

  static uint32 Load32(const void *p) {
    return ToHost32(UNALIGNED_LOAD32(p));
  }

  static void Store32(void *p, uint32 v) {
    UNALIGNED_STORE32(p, FromHost32(v));
  }
};

// Some bit-manipulation functions.
class Bits {
 public:
  // Return floor(log2(n)) for positive integer n.
  static int Log2FloorNonZero(uint32 n);

  // Return floor(log2(n)) for positive integer n.  Returns -1 iff n == 0.
  static int Log2Floor(uint32 n);

  // Return the first set least / most significant bit, 0-indexed.  Returns an
  // undefined value if n == 0.  FindLSBSetNonZero() is similar to ffs() except
  // that it's 0-indexed.
  static int FindLSBSetNonZero(uint32 n);

#if defined(ARCH_K8) || defined(ARCH_PPC) || defined(ARCH_ARM)
  static int FindLSBSetNonZero64(uint64 n);
#endif  // defined(ARCH_K8) || defined(ARCH_PPC) || defined(ARCH_ARM)

 private:
  // No copying
  Bits(const Bits&);
  void operator=(const Bits&);
};

#ifdef HAVE_BUILTIN_CTZ

inline int Bits::Log2FloorNonZero(uint32 n) {
  assert(n != 0);
  // (31 ^ x) is equivalent to (31 - x) for x in [0, 31]. An easy proof
  // represents subtraction in base 2 and observes that there's no carry.
  //
  // GCC and Clang represent __builtin_clz on x86 as 31 ^ _bit_scan_reverse(x).
  // Using "31 ^" here instead of "31 -" allows the optimizer to strip the
  // function body down to _bit_scan_reverse(x).
  return 31 ^ __builtin_clz(n);
}

inline int Bits::Log2Floor(uint32 n) {
  return (n == 0) ? -1 : Bits::Log2FloorNonZero(n);
}

inline int Bits::FindLSBSetNonZero(uint32 n) {
  assert(n != 0);
  return __builtin_ctz(n);
}

#if defined(ARCH_K8) || defined(ARCH_PPC) || defined(ARCH_ARM)
inline int Bits::FindLSBSetNonZero64(uint64 n) {
  assert(n != 0);
  return __builtin_ctzll(n);
}
#endif  // defined(ARCH_K8) || defined(ARCH_PPC) || defined(ARCH_ARM)

#elif defined(_MSC_VER)

inline int Bits::Log2FloorNonZero(uint32 n) {
  assert(n != 0);
  unsigned long where;
  _BitScanReverse(&where, n);
  return static_cast<int>(where);
}

inline int Bits::Log2Floor(uint32 n) {
  unsigned long where;
  if (_BitScanReverse(&where, n))
    return static_cast<int>(where);
  return -1;
}

inline int Bits::FindLSBSetNonZero(uint32 n) {
  assert(n != 0);
  unsigned long where;
  if (_BitScanForward(&where, n))
    return static_cast<int>(where);
  return 32;
}

#if defined(ARCH_K8) || defined(ARCH_PPC) || defined(ARCH_ARM)
inline int Bits::FindLSBSetNonZero64(uint64 n) {
  assert(n != 0);
  unsigned long where;
  if (_BitScanForward64(&where, n))
    return static_cast<int>(where);
  return 64;
}
#endif  // defined(ARCH_K8) || defined(ARCH_PPC) || defined(ARCH_ARM)

#else  // Portable versions.

inline int Bits::Log2FloorNonZero(uint32 n) {
  assert(n != 0);

  int log = 0;
  uint32 value = n;
  for (int i = 4; i >= 0; --i) {
    int shift = (1 << i);
    uint32 x = value >> shift;
    if (x != 0) {
      value = x;
      log += shift;
    }
  }
  assert(value == 1);
  return log;
}

inline int Bits::Log2Floor(uint32 n) {
  return (n == 0) ? -1 : Bits::Log2FloorNonZero(n);
}

inline int Bits::FindLSBSetNonZero(uint32 n) {
  assert(n != 0);

  int rc = 31;
  for (int i = 4, shift = 1 << 4; i >= 0; --i) {
    const uint32 x = n << shift;
    if (x != 0) {
      n = x;
      rc -= shift;
    }
    shift >>= 1;
  }
  return rc;
}

#if defined(ARCH_K8) || defined(ARCH_PPC) || defined(ARCH_ARM)
// FindLSBSetNonZero64() is defined in terms of FindLSBSetNonZero().
inline int Bits::FindLSBSetNonZero64(uint64 n) {
  assert(n != 0);

  const uint32 bottombits = static_cast<uint32>(n);
  if (bottombits == 0) {
    // Bottom bits are zero, so scan in top bits
    return 32 + FindLSBSetNonZero(static_cast<uint32>(n >> 32));
  } else {
    return FindLSBSetNonZero(bottombits);
  }
}
#endif  // defined(ARCH_K8) || defined(ARCH_PPC) || defined(ARCH_ARM)

#endif  // End portable versions.

// Variable-length integer encoding.
class Varint {
 public:
  // Maximum lengths of varint encoding of uint32.
  static const int kMax32 = 5;

  // Attempts to parse a varint32 from a prefix of the bytes in [ptr,limit-1].
  // Never reads a character at or beyond limit.  If a valid/terminated varint32
  // was found in the range, stores it in *OUTPUT and returns a pointer just
  // past the last byte of the varint32. Else returns NULL.  On success,
  // "result <= limit".
  static const char* Parse32WithLimit(const char* ptr, const char* limit,
                                      uint32* OUTPUT);

  // REQUIRES   "ptr" points to a buffer of length sufficient to hold "v".
  // EFFECTS    Encodes "v" into "ptr" and returns a pointer to the
  //            byte just past the last encoded byte.
  static char* Encode32(char* ptr, uint32 v);

  // EFFECTS    Appends the varint representation of "value" to "*s".
  static void Append32(string* s, uint32 value);
};

inline const char* Varint::Parse32WithLimit(const char* p,
                                            const char* l,
                                            uint32* OUTPUT) {
  const unsigned char* ptr = reinterpret_cast<const unsigned char*>(p);
  const unsigned char* limit = reinterpret_cast<const unsigned char*>(l);
  uint32 b, result;
  if (ptr >= limit) return NULL;
  b = *(ptr++); result = b & 127;          if (b < 128) goto done;
  if (ptr >= limit) return NULL;
  b = *(ptr++); result |= (b & 127) <<  7; if (b < 128) goto done;
  if (ptr >= limit) return NULL;
  b = *(ptr++); result |= (b & 127) << 14; if (b < 128) goto done;
  if (ptr >= limit) return NULL;
  b = *(ptr++); result |= (b & 127) << 21; if (b < 128) goto done;
  if (ptr >= limit) return NULL;
  b = *(ptr++); result |= (b & 127) << 28; if (b < 16) goto done;
  return NULL;       // Value is too long to be a varint32
 done:
  *OUTPUT = result;
  return reinterpret_cast<const char*>(ptr);
}

inline char* Varint::Encode32(char* sptr, uint32 v) {
  // Operate on characters as unsigneds
  unsigned char* ptr = reinterpret_cast<unsigned char*>(sptr);
  static const int B = 128;
  if (v < (1<<7)) {
    *(ptr++) = v;
  } else if (v < (1<<14)) {
    *(ptr++) = v | B;
    *(ptr++) = v>>7;
  } else if (v < (1<<21)) {
    *(ptr++) = v | B;
    *(ptr++) = (v>>7) | B;
    *(ptr++) = v>>14;
  } else if (v < (1<<28)) {
    *(ptr++) = v | B;
    *(ptr++) = (v>>7) | B;
    *(ptr++) = (v>>14) | B;
    *(ptr++) = v>>21;
  } else {
    *(ptr++) = v | B;
    *(ptr++) = (v>>7) | B;
    *(ptr++) = (v>>14) | B;
    *(ptr++) = (v>>21) | B;
    *(ptr++) = v>>28;
  }
  return reinterpret_cast<char*>(ptr);
}

// If you know the internal layout of the std::string in use, you can
// replace this function with one that resizes the string without
// filling the new space with zeros (if applicable) --
// it will be non-portable but faster.
inline void STLStringResizeUninitialized(string* s, size_t new_size) {
  s->resize(new_size);
}

// Return a mutable char* pointing to a string's internal buffer,
// which may not be null-terminated. Writing through this pointer will
// modify the string.
//
// string_as_array(&str)[i] is valid for 0 <= i < str.size() until the
// next call to a string method that invalidates iterators.
//
// As of 2006-04, there is no standard-blessed way of getting a
// mutable reference to a string's internal buffer. However, issue 530
// (http://www.open-std.org/JTC1/SC22/WG21/docs/lwg-defects.html#530)
// proposes this as the method. It will officially be part of the standard
// for C++0x. This should already work on all current implementations.
inline char* string_as_array(string* str) {
  return str->empty() ? NULL : &*str->begin();
}

}  // namespace duckdb_snappy

#endif  // THIRD_PARTY_SNAPPY_OPENSOURCE_SNAPPY_STUBS_INTERNAL_H_
