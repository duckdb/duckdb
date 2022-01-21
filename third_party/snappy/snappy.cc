// Copyright 2005 Google Inc. All Rights Reserved.
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

#include "snappy.h"
#include "snappy-internal.h"
#include "snappy-sinksource.h"

#if !defined(SNAPPY_HAVE_SSSE3)
// __SSSE3__ is defined by GCC and Clang. Visual Studio doesn't target SIMD
// support between SSE2 and AVX (so SSSE3 instructions require AVX support), and
// defines __AVX__ when AVX support is available.
#if defined(__SSSE3__) || defined(__AVX__)
#define SNAPPY_HAVE_SSSE3 1
#else
#define SNAPPY_HAVE_SSSE3 0
#endif
#endif  // !defined(SNAPPY_HAVE_SSSE3)

#if !defined(SNAPPY_HAVE_BMI2)
// __BMI2__ is defined by GCC and Clang. Visual Studio doesn't target BMI2
// specifically, but it does define __AVX2__ when AVX2 support is available.
// Fortunately, AVX2 was introduced in Haswell, just like BMI2.
//
// BMI2 is not defined as a subset of AVX2 (unlike SSSE3 and AVX above). So,
// GCC and Clang can build code with AVX2 enabled but BMI2 disabled, in which
// case issuing BMI2 instructions results in a compiler error.
#if defined(__BMI2__) || (defined(_MSC_VER) && defined(__AVX2__))
#define SNAPPY_HAVE_BMI2 1
#else
#define SNAPPY_HAVE_BMI2 0
#endif
#endif  // !defined(SNAPPY_HAVE_BMI2)

#if SNAPPY_HAVE_SSSE3
// Please do not replace with <x86intrin.h>. or with headers that assume more
// advanced SSE versions without checking with all the OWNERS.
#include <tmmintrin.h>
#endif

#if SNAPPY_HAVE_BMI2
// Please do not replace with <x86intrin.h>. or with headers that assume more
// advanced SSE versions without checking with all the OWNERS.
#include <immintrin.h>
#endif

#include <stdio.h>

#include <algorithm>
#include <string>
#include <vector>

namespace duckdb_snappy {

using internal::COPY_1_BYTE_OFFSET;
using internal::COPY_2_BYTE_OFFSET;
using internal::LITERAL;
using internal::char_table;
using internal::kMaximumTagLength;

// Any hash function will produce a valid compressed bitstream, but a good
// hash function reduces the number of collisions and thus yields better
// compression for compressible input, and more speed for incompressible
// input. Of course, it doesn't hurt if the hash function is reasonably fast
// either, as it gets called a lot.
static inline uint32 HashBytes(uint32 bytes, int shift) {
  uint32 kMul = 0x1e35a7bd;
  return (bytes * kMul) >> shift;
}
static inline uint32 Hash(const char* p, int shift) {
  return HashBytes(UNALIGNED_LOAD32(p), shift);
}

size_t MaxCompressedLength(size_t source_len) {
  // Compressed data can be defined as:
  //    compressed := item* literal*
  //    item       := literal* copy
  //
  // The trailing literal sequence has a space blowup of at most 62/60
  // since a literal of length 60 needs one tag byte + one extra byte
  // for length information.
  //
  // Item blowup is trickier to measure.  Suppose the "copy" op copies
  // 4 bytes of data.  Because of a special check in the encoding code,
  // we produce a 4-byte copy only if the offset is < 65536.  Therefore
  // the copy op takes 3 bytes to encode, and this type of item leads
  // to at most the 62/60 blowup for representing literals.
  //
  // Suppose the "copy" op copies 5 bytes of data.  If the offset is big
  // enough, it will take 5 bytes to encode the copy op.  Therefore the
  // worst case here is a one-byte literal followed by a five-byte copy.
  // I.e., 6 bytes of input turn into 7 bytes of "compressed" data.
  //
  // This last factor dominates the blowup, so the final estimate is:
  return 32 + source_len + source_len/6;
}

namespace {

void UnalignedCopy64(const void* src, void* dst) {
  char tmp[8];
  memcpy(tmp, src, 8);
  memcpy(dst, tmp, 8);
}

void UnalignedCopy128(const void* src, void* dst) {
  // memcpy gets vectorized when the appropriate compiler options are used.
  // For example, x86 compilers targeting SSE2+ will optimize to an SSE2 load
  // and store.
  char tmp[16];
  memcpy(tmp, src, 16);
  memcpy(dst, tmp, 16);
}

// Copy [src, src+(op_limit-op)) to [op, (op_limit-op)) a byte at a time. Used
// for handling COPY operations where the input and output regions may overlap.
// For example, suppose:
//    src       == "ab"
//    op        == src + 2
//    op_limit  == op + 20
// After IncrementalCopySlow(src, op, op_limit), the result will have eleven
// copies of "ab"
//    ababababababababababab
// Note that this does not match the semantics of either memcpy() or memmove().
inline char* IncrementalCopySlow(const char* src, char* op,
                                 char* const op_limit) {
  // TODO: Remove pragma when LLVM is aware this
  // function is only called in cold regions and when cold regions don't get
  // vectorized or unrolled.
#ifdef __clang__
#pragma clang loop unroll(disable)
#endif
  while (op < op_limit) {
    *op++ = *src++;
  }
  return op_limit;
}

#if SNAPPY_HAVE_SSSE3

// This is a table of shuffle control masks that can be used as the source
// operand for PSHUFB to permute the contents of the destination XMM register
// into a repeating byte pattern.
alignas(16) const char pshufb_fill_patterns[7][16] = {
  {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
  {0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1},
  {0, 1, 2, 0, 1, 2, 0, 1, 2, 0, 1, 2, 0, 1, 2, 0},
  {0, 1, 2, 3, 0, 1, 2, 3, 0, 1, 2, 3, 0, 1, 2, 3},
  {0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0},
  {0, 1, 2, 3, 4, 5, 0, 1, 2, 3, 4, 5, 0, 1, 2, 3},
  {0, 1, 2, 3, 4, 5, 6, 0, 1, 2, 3, 4, 5, 6, 0, 1},
};

#endif  // SNAPPY_HAVE_SSSE3

// Copy [src, src+(op_limit-op)) to [op, (op_limit-op)) but faster than
// IncrementalCopySlow. buf_limit is the address past the end of the writable
// region of the buffer.
inline char* IncrementalCopy(const char* src, char* op, char* const op_limit,
                             char* const buf_limit) {
  // Terminology:
  //
  // slop = buf_limit - op
  // pat  = op - src
  // len  = limit - op
  assert(src < op);
  assert(op <= op_limit);
  assert(op_limit <= buf_limit);
  // NOTE: The compressor always emits 4 <= len <= 64. It is ok to assume that
  // to optimize this function but we have to also handle other cases in case
  // the input does not satisfy these conditions.

  size_t pattern_size = op - src;
  // The cases are split into different branches to allow the branch predictor,
  // FDO, and static prediction hints to work better. For each input we list the
  // ratio of invocations that match each condition.
  //
  // input        slop < 16   pat < 8  len > 16
  // ------------------------------------------
  // html|html4|cp   0%         1.01%    27.73%
  // urls            0%         0.88%    14.79%
  // jpg             0%        64.29%     7.14%
  // pdf             0%         2.56%    58.06%
  // txt[1-4]        0%         0.23%     0.97%
  // pb              0%         0.96%    13.88%
  // bin             0.01%     22.27%    41.17%
  //
  // It is very rare that we don't have enough slop for doing block copies. It
  // is also rare that we need to expand a pattern. Small patterns are common
  // for incompressible formats and for those we are plenty fast already.
  // Lengths are normally not greater than 16 but they vary depending on the
  // input. In general if we always predict len <= 16 it would be an ok
  // prediction.
  //
  // In order to be fast we want a pattern >= 8 bytes and an unrolled loop
  // copying 2x 8 bytes at a time.

  // Handle the uncommon case where pattern is less than 8 bytes.
  if (SNAPPY_PREDICT_FALSE(pattern_size < 8)) {
#if SNAPPY_HAVE_SSSE3
    // Load the first eight bytes into an 128-bit XMM register, then use PSHUFB
    // to permute the register's contents in-place into a repeating sequence of
    // the first "pattern_size" bytes.
    // For example, suppose:
    //    src       == "abc"
    //    op        == op + 3
    // After _mm_shuffle_epi8(), "pattern" will have five copies of "abc"
    // followed by one byte of slop: abcabcabcabcabca.
    //
    // The non-SSE fallback implementation suffers from store-forwarding stalls
    // because its loads and stores partly overlap. By expanding the pattern
    // in-place, we avoid the penalty.
    if (SNAPPY_PREDICT_TRUE(op <= buf_limit - 16)) {
      const __m128i shuffle_mask = _mm_load_si128(
          reinterpret_cast<const __m128i*>(pshufb_fill_patterns)
          + pattern_size - 1);
      const __m128i pattern = _mm_shuffle_epi8(
          _mm_loadl_epi64(reinterpret_cast<const __m128i*>(src)), shuffle_mask);
      // Uninitialized bytes are masked out by the shuffle mask.
      // TODO: remove annotation and macro defs once MSan is fixed.
      SNAPPY_ANNOTATE_MEMORY_IS_INITIALIZED(&pattern, sizeof(pattern));
      pattern_size *= 16 / pattern_size;
      char* op_end = std::min(op_limit, buf_limit - 15);
      while (op < op_end) {
        _mm_storeu_si128(reinterpret_cast<__m128i*>(op), pattern);
        op += pattern_size;
      }
      if (SNAPPY_PREDICT_TRUE(op >= op_limit)) return op_limit;
    }
    return IncrementalCopySlow(src, op, op_limit);
#else  // !SNAPPY_HAVE_SSSE3
    // If plenty of buffer space remains, expand the pattern to at least 8
    // bytes. The way the following loop is written, we need 8 bytes of buffer
    // space if pattern_size >= 4, 11 bytes if pattern_size is 1 or 3, and 10
    // bytes if pattern_size is 2.  Precisely encoding that is probably not
    // worthwhile; instead, invoke the slow path if we cannot write 11 bytes
    // (because 11 are required in the worst case).
    if (SNAPPY_PREDICT_TRUE(op <= buf_limit - 11)) {
      while (pattern_size < 8) {
        UnalignedCopy64(src, op);
        op += pattern_size;
        pattern_size *= 2;
      }
      if (SNAPPY_PREDICT_TRUE(op >= op_limit)) return op_limit;
    } else {
      return IncrementalCopySlow(src, op, op_limit);
    }
#endif  // SNAPPY_HAVE_SSSE3
  }
  assert(pattern_size >= 8);

  // Copy 2x 8 bytes at a time. Because op - src can be < 16, a single
  // UnalignedCopy128 might overwrite data in op. UnalignedCopy64 is safe
  // because expanding the pattern to at least 8 bytes guarantees that
  // op - src >= 8.
  //
  // Typically, the op_limit is the gating factor so try to simplify the loop
  // based on that.
  if (SNAPPY_PREDICT_TRUE(op_limit <= buf_limit - 16)) {
    // Factor the displacement from op to the source into a variable. This helps
    // simplify the loop below by only varying the op pointer which we need to
    // test for the end. Note that this was done after carefully examining the
    // generated code to allow the addressing modes in the loop below to
    // maximize micro-op fusion where possible on modern Intel processors. The
    // generated code should be checked carefully for new processors or with
    // major changes to the compiler.
    // TODO: Simplify this code when the compiler reliably produces
    // the correct x86 instruction sequence.
    ptrdiff_t op_to_src = src - op;

    // The trip count of this loop is not large and so unrolling will only hurt
    // code size without helping performance.
    //
    // TODO: Replace with loop trip count hint.
#ifdef __clang__
#pragma clang loop unroll(disable)
#endif
    do {
      UnalignedCopy64(op + op_to_src, op);
      UnalignedCopy64(op + op_to_src + 8, op + 8);
      op += 16;
    } while (op < op_limit);
    return op_limit;
  }

  // Fall back to doing as much as we can with the available slop in the
  // buffer. This code path is relatively cold however so we save code size by
  // avoiding unrolling and vectorizing.
  //
  // TODO: Remove pragma when when cold regions don't get vectorized
  // or unrolled.
#ifdef __clang__
#pragma clang loop unroll(disable)
#endif
  for (char *op_end = buf_limit - 16; op < op_end; op += 16, src += 16) {
    UnalignedCopy64(src, op);
    UnalignedCopy64(src + 8, op + 8);
  }
  if (op >= op_limit)
    return op_limit;

  // We only take this branch if we didn't have enough slop and we can do a
  // single 8 byte copy.
  if (SNAPPY_PREDICT_FALSE(op <= buf_limit - 8)) {
    UnalignedCopy64(src, op);
    src += 8;
    op += 8;
  }
  return IncrementalCopySlow(src, op, op_limit);
}

}  // namespace

template <bool allow_fast_path>
static inline char* EmitLiteral(char* op,
                                const char* literal,
                                int len) {
  // The vast majority of copies are below 16 bytes, for which a
  // call to memcpy is overkill. This fast path can sometimes
  // copy up to 15 bytes too much, but that is okay in the
  // main loop, since we have a bit to go on for both sides:
  //
  //   - The input will always have kInputMarginBytes = 15 extra
  //     available bytes, as long as we're in the main loop, and
  //     if not, allow_fast_path = false.
  //   - The output will always have 32 spare bytes (see
  //     MaxCompressedLength).
  assert(len > 0);      // Zero-length literals are disallowed
  int n = len - 1;
  if (allow_fast_path && len <= 16) {
    // Fits in tag byte
    *op++ = LITERAL | (n << 2);

    UnalignedCopy128(literal, op);
    return op + len;
  }

  if (n < 60) {
    // Fits in tag byte
    *op++ = LITERAL | (n << 2);
  } else {
    int count = (Bits::Log2Floor(n) >> 3) + 1;
    assert(count >= 1);
    assert(count <= 4);
    *op++ = LITERAL | ((59 + count) << 2);
    // Encode in upcoming bytes.
    // Write 4 bytes, though we may care about only 1 of them. The output buffer
    // is guaranteed to have at least 3 more spaces left as 'len >= 61' holds
    // here and there is a memcpy of size 'len' below.
    LittleEndian::Store32(op, n);
    op += count;
  }
  memcpy(op, literal, len);
  return op + len;
}

template <bool len_less_than_12>
static inline char* EmitCopyAtMost64(char* op, size_t offset, size_t len) {
  assert(len <= 64);
  assert(len >= 4);
  assert(offset < 65536);
  assert(len_less_than_12 == (len < 12));

  if (len_less_than_12 && SNAPPY_PREDICT_TRUE(offset < 2048)) {
    // offset fits in 11 bits.  The 3 highest go in the top of the first byte,
    // and the rest go in the second byte.
    *op++ = COPY_1_BYTE_OFFSET + ((len - 4) << 2) + ((offset >> 3) & 0xe0);
    *op++ = offset & 0xff;
  } else {
    // Write 4 bytes, though we only care about 3 of them.  The output buffer
    // is required to have some slack, so the extra byte won't overrun it.
    uint32 u = COPY_2_BYTE_OFFSET + ((len - 1) << 2) + (offset << 8);
    LittleEndian::Store32(op, u);
    op += 3;
  }
  return op;
}

template <bool len_less_than_12>
static inline char* EmitCopy(char* op, size_t offset, size_t len) {
  assert(len_less_than_12 == (len < 12));
  if (len_less_than_12) {
    return EmitCopyAtMost64</*len_less_than_12=*/true>(op, offset, len);
  } else {
    // A special case for len <= 64 might help, but so far measurements suggest
    // it's in the noise.

    // Emit 64 byte copies but make sure to keep at least four bytes reserved.
    while (SNAPPY_PREDICT_FALSE(len >= 68)) {
      op = EmitCopyAtMost64</*len_less_than_12=*/false>(op, offset, 64);
      len -= 64;
    }

    // One or two copies will now finish the job.
    if (len > 64) {
      op = EmitCopyAtMost64</*len_less_than_12=*/false>(op, offset, 60);
      len -= 60;
    }

    // Emit remainder.
    if (len < 12) {
      op = EmitCopyAtMost64</*len_less_than_12=*/true>(op, offset, len);
    } else {
      op = EmitCopyAtMost64</*len_less_than_12=*/false>(op, offset, len);
    }
    return op;
  }
}

bool GetUncompressedLength(const char* start, size_t n, size_t* result) {
  uint32 v = 0;
  const char* limit = start + n;
  if (Varint::Parse32WithLimit(start, limit, &v) != NULL) {
    *result = v;
    return true;
  } else {
    return false;
  }
}

namespace {
uint32 CalculateTableSize(uint32 input_size) {
  assert(kMaxHashTableSize >= 256);
  if (input_size > kMaxHashTableSize) {
    return kMaxHashTableSize;
  }
  if (input_size < 256) {
    return 256;
  }
  // This is equivalent to Log2Ceiling(input_size), assuming input_size > 1.
  // 2 << Log2Floor(x - 1) is equivalent to 1 << (1 + Log2Floor(x - 1)).
  return 2u << Bits::Log2Floor(input_size - 1);
}
}  // namespace

namespace internal {
WorkingMemory::WorkingMemory(size_t input_size) {
  const size_t max_fragment_size = std::min(input_size, kBlockSize);
  const size_t table_size = CalculateTableSize(max_fragment_size);
  size_ = table_size * sizeof(*table_) + max_fragment_size +
          MaxCompressedLength(max_fragment_size);
  mem_ = std::allocator<char>().allocate(size_);
  table_ = reinterpret_cast<uint16*>(mem_);
  input_ = mem_ + table_size * sizeof(*table_);
  output_ = input_ + max_fragment_size;
}

WorkingMemory::~WorkingMemory() {
  std::allocator<char>().deallocate(mem_, size_);
}

uint16* WorkingMemory::GetHashTable(size_t fragment_size,
                                    int* table_size) const {
  const size_t htsize = CalculateTableSize(fragment_size);
  memset(table_, 0, htsize * sizeof(*table_));
  *table_size = htsize;
  return table_;
}
}  // end namespace internal

// For 0 <= offset <= 4, GetUint32AtOffset(GetEightBytesAt(p), offset) will
// equal UNALIGNED_LOAD32(p + offset).  Motivation: On x86-64 hardware we have
// empirically found that overlapping loads such as
//  UNALIGNED_LOAD32(p) ... UNALIGNED_LOAD32(p+1) ... UNALIGNED_LOAD32(p+2)
// are slower than UNALIGNED_LOAD64(p) followed by shifts and casts to uint32.
//
// We have different versions for 64- and 32-bit; ideally we would avoid the
// two functions and just inline the UNALIGNED_LOAD64 call into
// GetUint32AtOffset, but GCC (at least not as of 4.6) is seemingly not clever
// enough to avoid loading the value multiple times then. For 64-bit, the load
// is done when GetEightBytesAt() is called, whereas for 32-bit, the load is
// done at GetUint32AtOffset() time.

#ifdef ARCH_K8

typedef uint64 EightBytesReference;

static inline EightBytesReference GetEightBytesAt(const char* ptr) {
  return UNALIGNED_LOAD64(ptr);
}

static inline uint32 GetUint32AtOffset(uint64 v, int offset) {
  assert(offset >= 0);
  assert(offset <= 4);
  return v >> (LittleEndian::IsLittleEndian() ? 8 * offset : 32 - 8 * offset);
}

#else

typedef const char* EightBytesReference;

static inline EightBytesReference GetEightBytesAt(const char* ptr) {
  return ptr;
}

static inline uint32 GetUint32AtOffset(const char* v, int offset) {
  assert(offset >= 0);
  assert(offset <= 4);
  return UNALIGNED_LOAD32(v + offset);
}

#endif

// Flat array compression that does not emit the "uncompressed length"
// prefix. Compresses "input" string to the "*op" buffer.
//
// REQUIRES: "input" is at most "kBlockSize" bytes long.
// REQUIRES: "op" points to an array of memory that is at least
// "MaxCompressedLength(input.size())" in size.
// REQUIRES: All elements in "table[0..table_size-1]" are initialized to zero.
// REQUIRES: "table_size" is a power of two
//
// Returns an "end" pointer into "op" buffer.
// "end - op" is the compressed size of "input".
namespace internal {
char* CompressFragment(const char* input,
                       size_t input_size,
                       char* op,
                       uint16* table,
                       const int table_size) {
  // "ip" is the input pointer, and "op" is the output pointer.
  const char* ip = input;
  assert(input_size <= kBlockSize);
  assert((table_size & (table_size - 1)) == 0);  // table must be power of two
  const int shift = 32 - Bits::Log2Floor(table_size);
  // assert(static_cast<int>(kuint32max >> shift) == table_size - 1);
  const char* ip_end = input + input_size;
  const char* base_ip = ip;
  // Bytes in [next_emit, ip) will be emitted as literal bytes.  Or
  // [next_emit, ip_end) after the main loop.
  const char* next_emit = ip;

  const size_t kInputMarginBytes = 15;
  if (SNAPPY_PREDICT_TRUE(input_size >= kInputMarginBytes)) {
    const char* ip_limit = input + input_size - kInputMarginBytes;

    for (uint32 next_hash = Hash(++ip, shift); ; ) {
      assert(next_emit < ip);
      // The body of this loop calls EmitLiteral once and then EmitCopy one or
      // more times.  (The exception is that when we're close to exhausting
      // the input we goto emit_remainder.)
      //
      // In the first iteration of this loop we're just starting, so
      // there's nothing to copy, so calling EmitLiteral once is
      // necessary.  And we only start a new iteration when the
      // current iteration has determined that a call to EmitLiteral will
      // precede the next call to EmitCopy (if any).
      //
      // Step 1: Scan forward in the input looking for a 4-byte-long match.
      // If we get close to exhausting the input then goto emit_remainder.
      //
      // Heuristic match skipping: If 32 bytes are scanned with no matches
      // found, start looking only at every other byte. If 32 more bytes are
      // scanned (or skipped), look at every third byte, etc.. When a match is
      // found, immediately go back to looking at every byte. This is a small
      // loss (~5% performance, ~0.1% density) for compressible data due to more
      // bookkeeping, but for non-compressible data (such as JPEG) it's a huge
      // win since the compressor quickly "realizes" the data is incompressible
      // and doesn't bother looking for matches everywhere.
      //
      // The "skip" variable keeps track of how many bytes there are since the
      // last match; dividing it by 32 (ie. right-shifting by five) gives the
      // number of bytes to move ahead for each iteration.
      uint32 skip = 32;

      const char* next_ip = ip;
      const char* candidate;
      do {
        ip = next_ip;
        uint32 hash = next_hash;
        assert(hash == Hash(ip, shift));
        uint32 bytes_between_hash_lookups = skip >> 5;
        skip += bytes_between_hash_lookups;
        next_ip = ip + bytes_between_hash_lookups;
        if (SNAPPY_PREDICT_FALSE(next_ip > ip_limit)) {
          goto emit_remainder;
        }
        next_hash = Hash(next_ip, shift);
        candidate = base_ip + table[hash];
        assert(candidate >= base_ip);
        assert(candidate < ip);

        table[hash] = ip - base_ip;
      } while (SNAPPY_PREDICT_TRUE(UNALIGNED_LOAD32(ip) !=
                                 UNALIGNED_LOAD32(candidate)));

      // Step 2: A 4-byte match has been found.  We'll later see if more
      // than 4 bytes match.  But, prior to the match, input
      // bytes [next_emit, ip) are unmatched.  Emit them as "literal bytes."
      assert(next_emit + 16 <= ip_end);
      op = EmitLiteral</*allow_fast_path=*/true>(op, next_emit, ip - next_emit);

      // Step 3: Call EmitCopy, and then see if another EmitCopy could
      // be our next move.  Repeat until we find no match for the
      // input immediately after what was consumed by the last EmitCopy call.
      //
      // If we exit this loop normally then we need to call EmitLiteral next,
      // though we don't yet know how big the literal will be.  We handle that
      // by proceeding to the next iteration of the main loop.  We also can exit
      // this loop via goto if we get close to exhausting the input.
      EightBytesReference input_bytes;
      uint32 candidate_bytes = 0;

      do {
        // We have a 4-byte match at ip, and no need to emit any
        // "literal bytes" prior to ip.
        const char* base = ip;
        std::pair<size_t, bool> p =
            FindMatchLength(candidate + 4, ip + 4, ip_end);
        size_t matched = 4 + p.first;
        ip += matched;
        size_t offset = base - candidate;
        assert(0 == memcmp(base, candidate, matched));
        if (p.second) {
          op = EmitCopy</*len_less_than_12=*/true>(op, offset, matched);
        } else {
          op = EmitCopy</*len_less_than_12=*/false>(op, offset, matched);
        }
        next_emit = ip;
        if (SNAPPY_PREDICT_FALSE(ip >= ip_limit)) {
          goto emit_remainder;
        }
        // We are now looking for a 4-byte match again.  We read
        // table[Hash(ip, shift)] for that.  To improve compression,
        // we also update table[Hash(ip - 1, shift)] and table[Hash(ip, shift)].
        input_bytes = GetEightBytesAt(ip - 1);
        uint32 prev_hash = HashBytes(GetUint32AtOffset(input_bytes, 0), shift);
        table[prev_hash] = ip - base_ip - 1;
        uint32 cur_hash = HashBytes(GetUint32AtOffset(input_bytes, 1), shift);
        candidate = base_ip + table[cur_hash];
        candidate_bytes = UNALIGNED_LOAD32(candidate);
        table[cur_hash] = ip - base_ip;
      } while (GetUint32AtOffset(input_bytes, 1) == candidate_bytes);

      next_hash = HashBytes(GetUint32AtOffset(input_bytes, 2), shift);
      ++ip;
    }
  }

 emit_remainder:
  // Emit the remaining bytes as a literal
  if (next_emit < ip_end) {
    op = EmitLiteral</*allow_fast_path=*/false>(op, next_emit,
                                                ip_end - next_emit);
  }

  return op;
}
}  // end namespace internal

// Called back at avery compression call to trace parameters and sizes.
static inline void Report(const char *algorithm, size_t compressed_size,
                          size_t uncompressed_size) {}

// Signature of output types needed by decompression code.
// The decompression code is templatized on a type that obeys this
// signature so that we do not pay virtual function call overhead in
// the middle of a tight decompression loop.
//
// class DecompressionWriter {
//  public:
//   // Called before decompression
//   void SetExpectedLength(size_t length);
//
//   // Called after decompression
//   bool CheckLength() const;
//
//   // Called repeatedly during decompression
//   bool Append(const char* ip, size_t length);
//   bool AppendFromSelf(uint32 offset, size_t length);
//
//   // The rules for how TryFastAppend differs from Append are somewhat
//   // convoluted:
//   //
//   //  - TryFastAppend is allowed to decline (return false) at any
//   //    time, for any reason -- just "return false" would be
//   //    a perfectly legal implementation of TryFastAppend.
//   //    The intention is for TryFastAppend to allow a fast path
//   //    in the common case of a small append.
//   //  - TryFastAppend is allowed to read up to <available> bytes
//   //    from the input buffer, whereas Append is allowed to read
//   //    <length>. However, if it returns true, it must leave
//   //    at least five (kMaximumTagLength) bytes in the input buffer
//   //    afterwards, so that there is always enough space to read the
//   //    next tag without checking for a refill.
//   //  - TryFastAppend must always return decline (return false)
//   //    if <length> is 61 or more, as in this case the literal length is not
//   //    decoded fully. In practice, this should not be a big problem,
//   //    as it is unlikely that one would implement a fast path accepting
//   //    this much data.
//   //
//   bool TryFastAppend(const char* ip, size_t available, size_t length);
// };

static inline uint32 ExtractLowBytes(uint32 v, int n) {
  assert(n >= 0);
  assert(n <= 4);
#if SNAPPY_HAVE_BMI2
  return _bzhi_u32(v, 8 * n);
#else
  // This needs to be wider than uint32 otherwise `mask << 32` will be
  // undefined.
  uint64 mask = 0xffffffff;
  return v & ~(mask << (8 * n));
#endif
}

static inline bool LeftShiftOverflows(uint8 value, uint32 shift) {
  assert(shift < 32);
  static const uint8 masks[] = {
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,  //
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,  //
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,  //
      0x00, 0x80, 0xc0, 0xe0, 0xf0, 0xf8, 0xfc, 0xfe};
  return (value & masks[shift]) != 0;
}

// Helper class for decompression
class SnappyDecompressor {
 private:
  Source*       reader_;         // Underlying source of bytes to decompress
  const char*   ip_;             // Points to next buffered byte
  const char*   ip_limit_;       // Points just past buffered bytes
  uint32        peeked_;         // Bytes peeked from reader (need to skip)
  bool          eof_;            // Hit end of input without an error?
  char          scratch_[kMaximumTagLength];  // See RefillTag().

  // Ensure that all of the tag metadata for the next tag is available
  // in [ip_..ip_limit_-1].  Also ensures that [ip,ip+4] is readable even
  // if (ip_limit_ - ip_ < 5).
  //
  // Returns true on success, false on error or end of input.
  bool RefillTag();

 public:
  explicit SnappyDecompressor(Source* reader)
      : reader_(reader),
        ip_(NULL),
        ip_limit_(NULL),
        peeked_(0),
        eof_(false) {
  }

  ~SnappyDecompressor() {
    // Advance past any bytes we peeked at from the reader
    reader_->Skip(peeked_);
  }

  // Returns true iff we have hit the end of the input without an error.
  bool eof() const {
    return eof_;
  }

  // Read the uncompressed length stored at the start of the compressed data.
  // On success, stores the length in *result and returns true.
  // On failure, returns false.
  bool ReadUncompressedLength(uint32* result) {
    assert(ip_ == NULL);       // Must not have read anything yet
    // Length is encoded in 1..5 bytes
    *result = 0;
    uint32 shift = 0;
    while (true) {
      if (shift >= 32) return false;
      size_t n;
      const char* ip = reader_->Peek(&n);
      if (n == 0) return false;
      const unsigned char c = *(reinterpret_cast<const unsigned char*>(ip));
      reader_->Skip(1);
      uint32 val = c & 0x7f;
      if (LeftShiftOverflows(static_cast<uint8>(val), shift)) return false;
      *result |= val << shift;
      if (c < 128) {
        break;
      }
      shift += 7;
    }
    return true;
  }

  // Process the next item found in the input.
  // Returns true if successful, false on error or end of input.
  template <class Writer>
#if defined(__GNUC__) && defined(__x86_64__)
  __attribute__((aligned(32)))
#endif
  void DecompressAllTags(Writer* writer) {
    // In x86, pad the function body to start 16 bytes later. This function has
    // a couple of hotspots that are highly sensitive to alignment: we have
    // observed regressions by more than 20% in some metrics just by moving the
    // exact same code to a different position in the benchmark binary.
    //
    // Putting this code on a 32-byte-aligned boundary + 16 bytes makes us hit
    // the "lucky" case consistently. Unfortunately, this is a very brittle
    // workaround, and future differences in code generation may reintroduce
    // this regression. If you experience a big, difficult to explain, benchmark
    // performance regression here, first try removing this hack.
#if defined(__GNUC__) && defined(__x86_64__)
    // Two 8-byte "NOP DWORD ptr [EAX + EAX*1 + 00000000H]" instructions.
    asm(".byte 0x0f, 0x1f, 0x84, 0x00, 0x00, 0x00, 0x00, 0x00");
    asm(".byte 0x0f, 0x1f, 0x84, 0x00, 0x00, 0x00, 0x00, 0x00");
#endif

    const char* ip = ip_;
    // We could have put this refill fragment only at the beginning of the loop.
    // However, duplicating it at the end of each branch gives the compiler more
    // scope to optimize the <ip_limit_ - ip> expression based on the local
    // context, which overall increases speed.
    #define MAYBE_REFILL() \
        if (ip_limit_ - ip < kMaximumTagLength) { \
          ip_ = ip; \
          if (!RefillTag()) return; \
          ip = ip_; \
        }

    MAYBE_REFILL();
    for ( ;; ) {
      const unsigned char c = *(reinterpret_cast<const unsigned char*>(ip++));

      // Ratio of iterations that have LITERAL vs non-LITERAL for different
      // inputs.
      //
      // input          LITERAL  NON_LITERAL
      // -----------------------------------
      // html|html4|cp   23%        77%
      // urls            36%        64%
      // jpg             47%        53%
      // pdf             19%        81%
      // txt[1-4]        25%        75%
      // pb              24%        76%
      // bin             24%        76%
      if (SNAPPY_PREDICT_FALSE((c & 0x3) == LITERAL)) {
        size_t literal_length = (c >> 2) + 1u;
        if (writer->TryFastAppend(ip, ip_limit_ - ip, literal_length)) {
          assert(literal_length < 61);
          ip += literal_length;
          // NOTE: There is no MAYBE_REFILL() here, as TryFastAppend()
          // will not return true unless there's already at least five spare
          // bytes in addition to the literal.
          continue;
        }
        if (SNAPPY_PREDICT_FALSE(literal_length >= 61)) {
          // Long literal.
          const size_t literal_length_length = literal_length - 60;
          literal_length =
              ExtractLowBytes(LittleEndian::Load32(ip), literal_length_length) +
              1;
          ip += literal_length_length;
        }

        size_t avail = ip_limit_ - ip;
        while (avail < literal_length) {
          if (!writer->Append(ip, avail)) return;
          literal_length -= avail;
          reader_->Skip(peeked_);
          size_t n;
          ip = reader_->Peek(&n);
          avail = n;
          peeked_ = avail;
          if (avail == 0) return;  // Premature end of input
          ip_limit_ = ip + avail;
        }
        if (!writer->Append(ip, literal_length)) {
          return;
        }
        ip += literal_length;
        MAYBE_REFILL();
      } else {
        const size_t entry = char_table[c];
        const size_t trailer =
            ExtractLowBytes(LittleEndian::Load32(ip), entry >> 11);
        const size_t length = entry & 0xff;
        ip += entry >> 11;

        // copy_offset/256 is encoded in bits 8..10.  By just fetching
        // those bits, we get copy_offset (since the bit-field starts at
        // bit 8).
        const size_t copy_offset = entry & 0x700;
        if (!writer->AppendFromSelf(copy_offset + trailer, length)) {
          return;
        }
        MAYBE_REFILL();
      }
    }

#undef MAYBE_REFILL
  }
};

bool SnappyDecompressor::RefillTag() {
  const char* ip = ip_;
  if (ip == ip_limit_) {
    // Fetch a new fragment from the reader
    reader_->Skip(peeked_);   // All peeked bytes are used up
    size_t n;
    ip = reader_->Peek(&n);
    peeked_ = n;
    eof_ = (n == 0);
    if (eof_) return false;
    ip_limit_ = ip + n;
  }

  // Read the tag character
  assert(ip < ip_limit_);
  const unsigned char c = *(reinterpret_cast<const unsigned char*>(ip));
  const uint32 entry = char_table[c];
  const uint32 needed = (entry >> 11) + 1;  // +1 byte for 'c'
  assert(needed <= sizeof(scratch_));

  // Read more bytes from reader if needed
  uint32 nbuf = ip_limit_ - ip;
  if (nbuf < needed) {
    // Stitch together bytes from ip and reader to form the word
    // contents.  We store the needed bytes in "scratch_".  They
    // will be consumed immediately by the caller since we do not
    // read more than we need.
    memmove(scratch_, ip, nbuf);
    reader_->Skip(peeked_);  // All peeked bytes are used up
    peeked_ = 0;
    while (nbuf < needed) {
      size_t length;
      const char* src = reader_->Peek(&length);
      if (length == 0) return false;
      uint32 to_add = std::min<uint32>(needed - nbuf, length);
      memcpy(scratch_ + nbuf, src, to_add);
      nbuf += to_add;
      reader_->Skip(to_add);
    }
    assert(nbuf == needed);
    ip_ = scratch_;
    ip_limit_ = scratch_ + needed;
  } else if (nbuf < kMaximumTagLength) {
    // Have enough bytes, but move into scratch_ so that we do not
    // read past end of input
    memmove(scratch_, ip, nbuf);
    reader_->Skip(peeked_);  // All peeked bytes are used up
    peeked_ = 0;
    ip_ = scratch_;
    ip_limit_ = scratch_ + nbuf;
  } else {
    // Pass pointer to buffer returned by reader_.
    ip_ = ip;
  }
  return true;
}

template <typename Writer>
static bool InternalUncompress(Source* r, Writer* writer) {
  // Read the uncompressed length from the front of the compressed input
  SnappyDecompressor decompressor(r);
  uint32 uncompressed_len = 0;
  if (!decompressor.ReadUncompressedLength(&uncompressed_len)) return false;

  return InternalUncompressAllTags(&decompressor, writer, r->Available(),
                                   uncompressed_len);
}

template <typename Writer>
static bool InternalUncompressAllTags(SnappyDecompressor* decompressor,
                                      Writer* writer,
                                      uint32 compressed_len,
                                      uint32 uncompressed_len) {
  Report("snappy_uncompress", compressed_len, uncompressed_len);

  writer->SetExpectedLength(uncompressed_len);

  // Process the entire input
  decompressor->DecompressAllTags(writer);
  writer->Flush();
  return (decompressor->eof() && writer->CheckLength());
}

bool GetUncompressedLength(Source* source, uint32* result) {
  SnappyDecompressor decompressor(source);
  return decompressor.ReadUncompressedLength(result);
}

size_t Compress(Source* reader, Sink* writer) {
  size_t written = 0;
  size_t N = reader->Available();
  const size_t uncompressed_size = N;
  char ulength[Varint::kMax32];
  char* p = Varint::Encode32(ulength, N);
  writer->Append(ulength, p-ulength);
  written += (p - ulength);

  internal::WorkingMemory wmem(N);

  while (N > 0) {
    // Get next block to compress (without copying if possible)
    size_t fragment_size;
    const char* fragment = reader->Peek(&fragment_size);
    assert(fragment_size != 0);  // premature end of input
    const size_t num_to_read = std::min(N, kBlockSize);
    size_t bytes_read = fragment_size;

    size_t pending_advance = 0;
    if (bytes_read >= num_to_read) {
      // Buffer returned by reader is large enough
      pending_advance = num_to_read;
      fragment_size = num_to_read;
    } else {
      char* scratch = wmem.GetScratchInput();
      memcpy(scratch, fragment, bytes_read);
      reader->Skip(bytes_read);

      while (bytes_read < num_to_read) {
        fragment = reader->Peek(&fragment_size);
        size_t n = std::min<size_t>(fragment_size, num_to_read - bytes_read);
        memcpy(scratch + bytes_read, fragment, n);
        bytes_read += n;
        reader->Skip(n);
      }
      assert(bytes_read == num_to_read);
      fragment = scratch;
      fragment_size = num_to_read;
    }
    assert(fragment_size == num_to_read);

    // Get encoding table for compression
    int table_size;
    uint16* table = wmem.GetHashTable(num_to_read, &table_size);

    // Compress input_fragment and append to dest
    const int max_output = MaxCompressedLength(num_to_read);

    // Need a scratch buffer for the output, in case the byte sink doesn't
    // have room for us directly.

    // Since we encode kBlockSize regions followed by a region
    // which is <= kBlockSize in length, a previously allocated
    // scratch_output[] region is big enough for this iteration.
    char* dest = writer->GetAppendBuffer(max_output, wmem.GetScratchOutput());
    char* end = internal::CompressFragment(fragment, fragment_size, dest, table,
                                           table_size);
    writer->Append(dest, end - dest);
    written += (end - dest);

    N -= num_to_read;
    reader->Skip(pending_advance);
  }

  Report("snappy_compress", written, uncompressed_size);

  return written;
}

// -----------------------------------------------------------------------
// IOVec interfaces
// -----------------------------------------------------------------------

// A type that writes to an iovec.
// Note that this is not a "ByteSink", but a type that matches the
// Writer template argument to SnappyDecompressor::DecompressAllTags().
class SnappyIOVecWriter {
 private:
  // output_iov_end_ is set to iov + count and used to determine when
  // the end of the iovs is reached.
  const struct iovec* output_iov_end_;

#if !defined(NDEBUG)
  const struct iovec* output_iov_;
#endif  // !defined(NDEBUG)

  // Current iov that is being written into.
  const struct iovec* curr_iov_;

  // Pointer to current iov's write location.
  char* curr_iov_output_;

  // Remaining bytes to write into curr_iov_output.
  size_t curr_iov_remaining_;

  // Total bytes decompressed into output_iov_ so far.
  size_t total_written_;

  // Maximum number of bytes that will be decompressed into output_iov_.
  size_t output_limit_;

  static inline char* GetIOVecPointer(const struct iovec* iov, size_t offset) {
    return reinterpret_cast<char*>(iov->iov_base) + offset;
  }

 public:
  // Does not take ownership of iov. iov must be valid during the
  // entire lifetime of the SnappyIOVecWriter.
  inline SnappyIOVecWriter(const struct iovec* iov, size_t iov_count)
      : output_iov_end_(iov + iov_count),
#if !defined(NDEBUG)
        output_iov_(iov),
#endif  // !defined(NDEBUG)
        curr_iov_(iov),
        curr_iov_output_(iov_count ? reinterpret_cast<char*>(iov->iov_base)
                                   : nullptr),
        curr_iov_remaining_(iov_count ? iov->iov_len : 0),
        total_written_(0),
        output_limit_(-1) {}

  inline void SetExpectedLength(size_t len) {
    output_limit_ = len;
  }

  inline bool CheckLength() const {
    return total_written_ == output_limit_;
  }

  inline bool Append(const char* ip, size_t len) {
    if (total_written_ + len > output_limit_) {
      return false;
    }

    return AppendNoCheck(ip, len);
  }

  inline bool AppendNoCheck(const char* ip, size_t len) {
    while (len > 0) {
      if (curr_iov_remaining_ == 0) {
        // This iovec is full. Go to the next one.
        if (curr_iov_ + 1 >= output_iov_end_) {
          return false;
        }
        ++curr_iov_;
        curr_iov_output_ = reinterpret_cast<char*>(curr_iov_->iov_base);
        curr_iov_remaining_ = curr_iov_->iov_len;
      }

      const size_t to_write = std::min(len, curr_iov_remaining_);
      memcpy(curr_iov_output_, ip, to_write);
      curr_iov_output_ += to_write;
      curr_iov_remaining_ -= to_write;
      total_written_ += to_write;
      ip += to_write;
      len -= to_write;
    }

    return true;
  }

  inline bool TryFastAppend(const char* ip, size_t available, size_t len) {
    const size_t space_left = output_limit_ - total_written_;
    if (len <= 16 && available >= 16 + kMaximumTagLength && space_left >= 16 &&
        curr_iov_remaining_ >= 16) {
      // Fast path, used for the majority (about 95%) of invocations.
      UnalignedCopy128(ip, curr_iov_output_);
      curr_iov_output_ += len;
      curr_iov_remaining_ -= len;
      total_written_ += len;
      return true;
    }

    return false;
  }

  inline bool AppendFromSelf(size_t offset, size_t len) {
    // See SnappyArrayWriter::AppendFromSelf for an explanation of
    // the "offset - 1u" trick.
    if (offset - 1u >= total_written_) {
      return false;
    }
    const size_t space_left = output_limit_ - total_written_;
    if (len > space_left) {
      return false;
    }

    // Locate the iovec from which we need to start the copy.
    const iovec* from_iov = curr_iov_;
    size_t from_iov_offset = curr_iov_->iov_len - curr_iov_remaining_;
    while (offset > 0) {
      if (from_iov_offset >= offset) {
        from_iov_offset -= offset;
        break;
      }

      offset -= from_iov_offset;
      --from_iov;
#if !defined(NDEBUG)
      assert(from_iov >= output_iov_);
#endif  // !defined(NDEBUG)
      from_iov_offset = from_iov->iov_len;
    }

    // Copy <len> bytes starting from the iovec pointed to by from_iov_index to
    // the current iovec.
    while (len > 0) {
      assert(from_iov <= curr_iov_);
      if (from_iov != curr_iov_) {
        const size_t to_copy =
            std::min((unsigned long)(from_iov->iov_len - from_iov_offset), (unsigned long)len);
        AppendNoCheck(GetIOVecPointer(from_iov, from_iov_offset), to_copy);
        len -= to_copy;
        if (len > 0) {
          ++from_iov;
          from_iov_offset = 0;
        }
      } else {
        size_t to_copy = curr_iov_remaining_;
        if (to_copy == 0) {
          // This iovec is full. Go to the next one.
          if (curr_iov_ + 1 >= output_iov_end_) {
            return false;
          }
          ++curr_iov_;
          curr_iov_output_ = reinterpret_cast<char*>(curr_iov_->iov_base);
          curr_iov_remaining_ = curr_iov_->iov_len;
          continue;
        }
        if (to_copy > len) {
          to_copy = len;
        }

        IncrementalCopy(GetIOVecPointer(from_iov, from_iov_offset),
                        curr_iov_output_, curr_iov_output_ + to_copy,
                        curr_iov_output_ + curr_iov_remaining_);
        curr_iov_output_ += to_copy;
        curr_iov_remaining_ -= to_copy;
        from_iov_offset += to_copy;
        total_written_ += to_copy;
        len -= to_copy;
      }
    }

    return true;
  }

  inline void Flush() {}
};

bool RawUncompressToIOVec(const char* compressed, size_t compressed_length,
                          const struct iovec* iov, size_t iov_cnt) {
  ByteArraySource reader(compressed, compressed_length);
  return RawUncompressToIOVec(&reader, iov, iov_cnt);
}

bool RawUncompressToIOVec(Source* compressed, const struct iovec* iov,
                          size_t iov_cnt) {
  SnappyIOVecWriter output(iov, iov_cnt);
  return InternalUncompress(compressed, &output);
}

// -----------------------------------------------------------------------
// Flat array interfaces
// -----------------------------------------------------------------------

// A type that writes to a flat array.
// Note that this is not a "ByteSink", but a type that matches the
// Writer template argument to SnappyDecompressor::DecompressAllTags().
class SnappyArrayWriter {
 private:
  char* base_;
  char* op_;
  char* op_limit_;

 public:
  inline explicit SnappyArrayWriter(char* dst)
      : base_(dst),
        op_(dst),
        op_limit_(dst) {
  }

  inline void SetExpectedLength(size_t len) {
    op_limit_ = op_ + len;
  }

  inline bool CheckLength() const {
    return op_ == op_limit_;
  }

  inline bool Append(const char* ip, size_t len) {
    char* op = op_;
    const size_t space_left = op_limit_ - op;
    if (space_left < len) {
      return false;
    }
    memcpy(op, ip, len);
    op_ = op + len;
    return true;
  }

  inline bool TryFastAppend(const char* ip, size_t available, size_t len) {
    char* op = op_;
    const size_t space_left = op_limit_ - op;
    if (len <= 16 && available >= 16 + kMaximumTagLength && space_left >= 16) {
      // Fast path, used for the majority (about 95%) of invocations.
      UnalignedCopy128(ip, op);
      op_ = op + len;
      return true;
    } else {
      return false;
    }
  }

  inline bool AppendFromSelf(size_t offset, size_t len) {
    char* const op_end = op_ + len;

    // Check if we try to append from before the start of the buffer.
    // Normally this would just be a check for "produced < offset",
    // but "produced <= offset - 1u" is equivalent for every case
    // except the one where offset==0, where the right side will wrap around
    // to a very big number. This is convenient, as offset==0 is another
    // invalid case that we also want to catch, so that we do not go
    // into an infinite loop.
    if (Produced() <= offset - 1u || op_end > op_limit_) return false;
    op_ = IncrementalCopy(op_ - offset, op_, op_end, op_limit_);

    return true;
  }
  inline size_t Produced() const {
    assert(op_ >= base_);
    return op_ - base_;
  }
  inline void Flush() {}
};

bool RawUncompress(const char* compressed, size_t n, char* uncompressed) {
  ByteArraySource reader(compressed, n);
  return RawUncompress(&reader, uncompressed);
}

bool RawUncompress(Source* compressed, char* uncompressed) {
  SnappyArrayWriter output(uncompressed);
  return InternalUncompress(compressed, &output);
}

bool Uncompress(const char* compressed, size_t n, string* uncompressed) {
  size_t ulength;
  if (!GetUncompressedLength(compressed, n, &ulength)) {
    return false;
  }
  // On 32-bit builds: max_size() < kuint32max.  Check for that instead
  // of crashing (e.g., consider externally specified compressed data).
  if (ulength > uncompressed->max_size()) {
    return false;
  }
  STLStringResizeUninitialized(uncompressed, ulength);
  return RawUncompress(compressed, n, string_as_array(uncompressed));
}

// A Writer that drops everything on the floor and just does validation
class SnappyDecompressionValidator {
 private:
  size_t expected_;
  size_t produced_;

 public:
  inline SnappyDecompressionValidator() : expected_(0), produced_(0) { }
  inline void SetExpectedLength(size_t len) {
    expected_ = len;
  }
  inline bool CheckLength() const {
    return expected_ == produced_;
  }
  inline bool Append(const char* ip, size_t len) {
    produced_ += len;
    return produced_ <= expected_;
  }
  inline bool TryFastAppend(const char* ip, size_t available, size_t length) {
    return false;
  }
  inline bool AppendFromSelf(size_t offset, size_t len) {
    // See SnappyArrayWriter::AppendFromSelf for an explanation of
    // the "offset - 1u" trick.
    if (produced_ <= offset - 1u) return false;
    produced_ += len;
    return produced_ <= expected_;
  }
  inline void Flush() {}
};

bool IsValidCompressedBuffer(const char* compressed, size_t n) {
  ByteArraySource reader(compressed, n);
  SnappyDecompressionValidator writer;
  return InternalUncompress(&reader, &writer);
}

bool IsValidCompressed(Source* compressed) {
  SnappyDecompressionValidator writer;
  return InternalUncompress(compressed, &writer);
}

void RawCompress(const char* input,
                 size_t input_length,
                 char* compressed,
                 size_t* compressed_length) {
  ByteArraySource reader(input, input_length);
  UncheckedByteArraySink writer(compressed);
  Compress(&reader, &writer);

  // Compute how many bytes were added
  *compressed_length = (writer.CurrentDestination() - compressed);
}

size_t Compress(const char* input, size_t input_length, string* compressed) {
  // Pre-grow the buffer to the max length of the compressed output
  STLStringResizeUninitialized(compressed, MaxCompressedLength(input_length));

  size_t compressed_length;
  RawCompress(input, input_length, string_as_array(compressed),
              &compressed_length);
  compressed->resize(compressed_length);
  return compressed_length;
}

// -----------------------------------------------------------------------
// Sink interface
// -----------------------------------------------------------------------

// A type that decompresses into a Sink. The template parameter
// Allocator must export one method "char* Allocate(int size);", which
// allocates a buffer of "size" and appends that to the destination.
template <typename Allocator>
class SnappyScatteredWriter {
  Allocator allocator_;

  // We need random access into the data generated so far.  Therefore
  // we keep track of all of the generated data as an array of blocks.
  // All of the blocks except the last have length kBlockSize.
  std::vector<char*> blocks_;
  size_t expected_;

  // Total size of all fully generated blocks so far
  size_t full_size_;

  // Pointer into current output block
  char* op_base_;       // Base of output block
  char* op_ptr_;        // Pointer to next unfilled byte in block
  char* op_limit_;      // Pointer just past block

  inline size_t Size() const {
    return full_size_ + (op_ptr_ - op_base_);
  }

  bool SlowAppend(const char* ip, size_t len);
  bool SlowAppendFromSelf(size_t offset, size_t len);

 public:
  inline explicit SnappyScatteredWriter(const Allocator& allocator)
      : allocator_(allocator),
        full_size_(0),
        op_base_(NULL),
        op_ptr_(NULL),
        op_limit_(NULL) {
  }

  inline void SetExpectedLength(size_t len) {
    assert(blocks_.empty());
    expected_ = len;
  }

  inline bool CheckLength() const {
    return Size() == expected_;
  }

  // Return the number of bytes actually uncompressed so far
  inline size_t Produced() const {
    return Size();
  }

  inline bool Append(const char* ip, size_t len) {
    size_t avail = op_limit_ - op_ptr_;
    if (len <= avail) {
      // Fast path
      memcpy(op_ptr_, ip, len);
      op_ptr_ += len;
      return true;
    } else {
      return SlowAppend(ip, len);
    }
  }

  inline bool TryFastAppend(const char* ip, size_t available, size_t length) {
    char* op = op_ptr_;
    const int space_left = op_limit_ - op;
    if (length <= 16 && available >= 16 + kMaximumTagLength &&
        space_left >= 16) {
      // Fast path, used for the majority (about 95%) of invocations.
      UnalignedCopy128(ip, op);
      op_ptr_ = op + length;
      return true;
    } else {
      return false;
    }
  }

  inline bool AppendFromSelf(size_t offset, size_t len) {
    char* const op_end = op_ptr_ + len;
    // See SnappyArrayWriter::AppendFromSelf for an explanation of
    // the "offset - 1u" trick.
    if (SNAPPY_PREDICT_TRUE(offset - 1u < (size_t)(op_ptr_ - op_base_) &&
                          op_end <= op_limit_)) {
      // Fast path: src and dst in current block.
      op_ptr_ = IncrementalCopy(op_ptr_ - offset, op_ptr_, op_end, op_limit_);
      return true;
    }
    return SlowAppendFromSelf(offset, len);
  }

  // Called at the end of the decompress. We ask the allocator
  // write all blocks to the sink.
  inline void Flush() { allocator_.Flush(Produced()); }
};

template<typename Allocator>
bool SnappyScatteredWriter<Allocator>::SlowAppend(const char* ip, size_t len) {
  size_t avail = op_limit_ - op_ptr_;
  while (len > avail) {
    // Completely fill this block
    memcpy(op_ptr_, ip, avail);
    op_ptr_ += avail;
    assert(op_limit_ - op_ptr_ == 0);
    full_size_ += (op_ptr_ - op_base_);
    len -= avail;
    ip += avail;

    // Bounds check
    if (full_size_ + len > expected_) {
      return false;
    }

    // Make new block
    size_t bsize = std::min<size_t>(kBlockSize, expected_ - full_size_);
    op_base_ = allocator_.Allocate(bsize);
    op_ptr_ = op_base_;
    op_limit_ = op_base_ + bsize;
    blocks_.push_back(op_base_);
    avail = bsize;
  }

  memcpy(op_ptr_, ip, len);
  op_ptr_ += len;
  return true;
}

template<typename Allocator>
bool SnappyScatteredWriter<Allocator>::SlowAppendFromSelf(size_t offset,
                                                         size_t len) {
  // Overflow check
  // See SnappyArrayWriter::AppendFromSelf for an explanation of
  // the "offset - 1u" trick.
  const size_t cur = Size();
  if (offset - 1u >= cur) return false;
  if (expected_ - cur < len) return false;

  // Currently we shouldn't ever hit this path because Compress() chops the
  // input into blocks and does not create cross-block copies. However, it is
  // nice if we do not rely on that, since we can get better compression if we
  // allow cross-block copies and thus might want to change the compressor in
  // the future.
  size_t src = cur - offset;
  while (len-- > 0) {
    char c = blocks_[src >> kBlockLog][src & (kBlockSize-1)];
    Append(&c, 1);
    src++;
  }
  return true;
}

class SnappySinkAllocator {
 public:
  explicit SnappySinkAllocator(Sink* dest): dest_(dest) {}
  ~SnappySinkAllocator() {}

  char* Allocate(int size) {
    Datablock block(new char[size], size);
    blocks_.push_back(block);
    return block.data;
  }

  // We flush only at the end, because the writer wants
  // random access to the blocks and once we hand the
  // block over to the sink, we can't access it anymore.
  // Also we don't write more than has been actually written
  // to the blocks.
  void Flush(size_t size) {
    size_t size_written = 0;
    size_t block_size;
    for (size_t i = 0; i < blocks_.size(); ++i) {
      block_size = std::min<size_t>(blocks_[i].size, size - size_written);
      dest_->AppendAndTakeOwnership(blocks_[i].data, block_size,
                                    &SnappySinkAllocator::Deleter, NULL);
      size_written += block_size;
    }
    blocks_.clear();
  }

 private:
  struct Datablock {
    char* data;
    size_t size;
    Datablock(char* p, size_t s) : data(p), size(s) {}
  };

  static void Deleter(void* arg, const char* bytes, size_t size) {
    delete[] bytes;
  }

  Sink* dest_;
  std::vector<Datablock> blocks_;

  // Note: copying this object is allowed
};

size_t UncompressAsMuchAsPossible(Source* compressed, Sink* uncompressed) {
  SnappySinkAllocator allocator(uncompressed);
  SnappyScatteredWriter<SnappySinkAllocator> writer(allocator);
  InternalUncompress(compressed, &writer);
  return writer.Produced();
}

bool Uncompress(Source* compressed, Sink* uncompressed) {
  // Read the uncompressed length from the front of the compressed input
  SnappyDecompressor decompressor(compressed);
  uint32 uncompressed_len = 0;
  if (!decompressor.ReadUncompressedLength(&uncompressed_len)) {
    return false;
  }

  char c;
  size_t allocated_size;
  char* buf = uncompressed->GetAppendBufferVariable(
      1, uncompressed_len, &c, 1, &allocated_size);

  const size_t compressed_len = compressed->Available();
  // If we can get a flat buffer, then use it, otherwise do block by block
  // uncompression
  if (allocated_size >= uncompressed_len) {
    SnappyArrayWriter writer(buf);
    bool result = InternalUncompressAllTags(&decompressor, &writer,
                                            compressed_len, uncompressed_len);
    uncompressed->Append(buf, writer.Produced());
    return result;
  } else {
    SnappySinkAllocator allocator(uncompressed);
    SnappyScatteredWriter<SnappySinkAllocator> writer(allocator);
    return InternalUncompressAllTags(&decompressor, &writer, compressed_len,
                                     uncompressed_len);
  }
}

}  // namespace duckdb_snappy
