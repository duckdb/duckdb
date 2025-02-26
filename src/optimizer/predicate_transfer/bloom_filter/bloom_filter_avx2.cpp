#include <immintrin.h>

#include "duckdb/optimizer/predicate_transfer/bloom_filter/bloom_filter.hpp"
#include "arrow/util/bit_util.h"

namespace duckdb {
inline __m256i BlockedBloomFilter::mask_avx2(__m256i hash) const {
  // AVX2 translation of mask() method
  //
  __m256i mask_id =
      _mm256_and_si256(hash, _mm256_set1_epi64x(BloomFilterMasks::kNumMasks - 1));

  auto masks = reinterpret_cast<const arrow::util::int64_for_gather_t*>(masks_.masks_);
  __m256i mask_byte_index = _mm256_srli_epi64(mask_id, 3);
  __m256i result = _mm256_i64gather_epi64(masks, mask_byte_index, 1);
  __m256i mask_bit_in_byte_index = _mm256_and_si256(mask_id, _mm256_set1_epi64x(7));
  result = _mm256_srlv_epi64(result, mask_bit_in_byte_index);
  result = _mm256_and_si256(result, _mm256_set1_epi64x(BloomFilterMasks::kFullMask));

  __m256i rotation = _mm256_and_si256(
      _mm256_srli_epi64(hash, BloomFilterMasks::kLogNumMasks), _mm256_set1_epi64x(63));

  result = _mm256_or_si256(
      _mm256_sllv_epi64(result, rotation),
      _mm256_srlv_epi64(result, _mm256_sub_epi64(_mm256_set1_epi64x(64), rotation)));

  return result;
}

inline __m256i BlockedBloomFilter::block_id_avx2(__m256i hash) const {
  // AVX2 translation of block_id() method
  //
  __m256i result = _mm256_srli_epi64(hash, BloomFilterMasks::kLogNumMasks + 6);
  result = _mm256_and_si256(result, _mm256_set1_epi64x(num_blocks_ - 1));
  return result;
}

int64_t BlockedBloomFilter::FindImp_avx2(int64_t num_rows, const uint64_t* hashes,
                                         uint8_t* result_bit_vector) const {
  constexpr int unroll = 8;

  auto blocks = reinterpret_cast<const arrow::util::int64_for_gather_t*>(blocks_);

  for (int64_t i = 0; i < num_rows / unroll; ++i) {
    __m256i hash_A, hash_B;
    hash_A = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(hashes) + 2 * i + 0);
    hash_B = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(hashes) + 2 * i + 1);
    __m256i mask_A = mask_avx2(hash_A);
    __m256i mask_B = mask_avx2(hash_B);
    __m256i block_id_A = block_id_avx2(hash_A);
    __m256i block_id_B = block_id_avx2(hash_B);
    __m256i block_A = _mm256_i64gather_epi64(blocks, block_id_A, sizeof(uint64_t));
    __m256i block_B = _mm256_i64gather_epi64(blocks, block_id_B, sizeof(uint64_t));
    uint64_t result_bytes = static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_cmpeq_epi64(_mm256_and_si256(block_A, mask_A), mask_A)));
    result_bytes |= static_cast<uint64_t>(_mm256_movemask_epi8(_mm256_cmpeq_epi64(_mm256_and_si256(block_B, mask_B), mask_B))) << 32;
    result_bit_vector[i] = static_cast<uint8_t>(_mm256_movemask_epi8(_mm256_set1_epi64x(result_bytes)));
  }

  return num_rows - (num_rows % unroll);
}

int64_t BlockedBloomFilter::Find_avx2(int64_t num_rows, const uint64_t* hashes,
                                      uint8_t* result_bit_vector) const {
    return FindImp_avx2(num_rows, hashes, result_bit_vector);
}

template <typename T>
int64_t BlockedBloomFilter::InsertImp_avx2(int64_t num_rows, const T* hashes) {
  constexpr int unroll = 4;

  for (int64_t i = 0; i < num_rows / unroll; ++i) {
    __m256i hash;
    if (sizeof(T) == sizeof(uint32_t)) {
      hash = _mm256_cvtepu32_epi64(
          _mm_loadu_si128(reinterpret_cast<const __m128i*>(hashes) + i));
    } else {
      hash = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(hashes) + i);
    }
    __m256i mask = mask_avx2(hash);
    __m256i block_id = block_id_avx2(hash);
    blocks_[_mm256_extract_epi64(block_id, 0)].fetch_or(_mm256_extract_epi64(mask, 0));
    blocks_[_mm256_extract_epi64(block_id, 1)].fetch_or(_mm256_extract_epi64(mask, 1));
    blocks_[_mm256_extract_epi64(block_id, 2)].fetch_or(_mm256_extract_epi64(mask, 2));
    blocks_[_mm256_extract_epi64(block_id, 3)].fetch_or(_mm256_extract_epi64(mask, 3));
  }

  return num_rows - (num_rows % unroll);
}

int64_t BlockedBloomFilter::Insert_avx2(int64_t num_rows, const uint32_t* hashes) {
  return InsertImp_avx2(num_rows, hashes);
}

int64_t BlockedBloomFilter::Insert_avx2(int64_t num_rows, const uint64_t* hashes) {
    return InsertImp_avx2(num_rows, hashes);
}

}  // namespace duckdb