/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// author Kevin Lang, Oath Research

#ifndef CPC_COMPRESSOR_HPP_
#define CPC_COMPRESSOR_HPP_

#include "cpc_common.hpp"

namespace datasketches {

/*
 * This is a very efficient compressor specialized for use by the CPC Sketch.
 * There are two very different compression schemes here: one for the sliding window
 * and another for the table of so-called surprising values.
 * These two compression schemes are designed for specific probability distributions of entries
 * in these data structures and make some compromises for performance. As a result
 * the compression is slightly less effective than theoretically achievable but is very fast.
 */

// forward declarations
template<typename A> class cpc_sketch_alloc;
template<typename A> class cpc_compressor;

// the compressor is not instantiated directly
// the sketch implementation uses this global function to statically allocate and construct on the first use
template<typename A>
inline cpc_compressor<A>& get_compressor();

template<typename A>
class cpc_compressor {
public:
  void compress(const cpc_sketch_alloc<A>& source, compressed_state<A>& target) const;
  void uncompress(const compressed_state<A>& source, uncompressed_state<A>& target, uint8_t lg_k, uint32_t num_coupons) const;

  // methods below are public for testing

  // This returns the number of compressed words that were actually used. It is the caller's
  // responsibility to ensure that the compressed_words array is long enough to prevent over-run.
  uint32_t low_level_compress_bytes(
      const uint8_t* byte_array, // input
      uint32_t num_bytes_to_encode,
      const uint16_t* encoding_table,
      uint32_t* compressed_words  // output
  ) const;

  void low_level_uncompress_bytes(
      uint8_t* byte_array, // output
      uint32_t num_bytes_to_decode,
      const uint16_t* decoding_table,
      const uint32_t* compressed_words,
      uint32_t num_compressed_words // input
  ) const;

  // Here "pairs" refers to row-column pairs that specify
  // the positions of surprising values in the bit matrix.

  // returns the number of compressedWords actually used
  uint32_t low_level_compress_pairs(
      const uint32_t* pair_array, // input
      uint32_t num_pairs_to_encode,
      uint8_t num_base_bits,
      uint32_t* compressed_words // output
  ) const;

  void low_level_uncompress_pairs(
      uint32_t* pair_array, // output
      uint32_t num_pairs_to_decode,
      uint8_t num_base_bits,
      const uint32_t* compressed_words, // input
      uint32_t num_compressed_words // input
  ) const;

private:
  // These decoding tables are created at library startup time by inverting the encoding tables
  uint16_t* decoding_tables_for_high_entropy_byte[22] = {
    // sixteen tables for the steady state (chosen based on the "phase" of C/K)
    NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL,
    // six more tables for the gradual transition between warmup mode and the steady state.
    NULL, NULL, NULL, NULL, NULL, NULL
  };
  uint16_t* length_limited_unary_decoding_table65;
  uint8_t* column_permutations_for_decoding[16] = {
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
  };

  cpc_compressor();
  template<typename T> friend cpc_compressor<T>& get_compressor();
  ~cpc_compressor();

  void make_decoding_tables(); // call this at startup
  void free_decoding_tables(); // call this at the end

  void compress_sparse_flavor(const cpc_sketch_alloc<A>& source, compressed_state<A>& target) const;
  void compress_hybrid_flavor(const cpc_sketch_alloc<A>& source, compressed_state<A>& target) const;
  void compress_pinned_flavor(const cpc_sketch_alloc<A>& source, compressed_state<A>& target) const;
  void compress_sliding_flavor(const cpc_sketch_alloc<A>& source, compressed_state<A>& target) const;

  void uncompress_sparse_flavor(const compressed_state<A>& source, uncompressed_state<A>& target, uint8_t lg_k) const;
  void uncompress_hybrid_flavor(const compressed_state<A>& source, uncompressed_state<A>& target, uint8_t lg_k) const;
  void uncompress_pinned_flavor(const compressed_state<A>& source, uncompressed_state<A>& target, uint8_t lg_k, uint32_t num_coupons) const;
  void uncompress_sliding_flavor(const compressed_state<A>& source, uncompressed_state<A>& target, uint8_t lg_k, uint32_t num_coupons) const;

  uint8_t* make_inverse_permutation(const uint8_t* permu, unsigned length);
  uint16_t* make_decoding_table(const uint16_t* encoding_table, unsigned num_byte_values);
  void validate_decoding_table(const uint16_t* decoding_table, const uint16_t* encoding_table) const;

  void compress_surprising_values(const vector_u32<A>& pairs, uint8_t lg_k, compressed_state<A>& result) const;
  void compress_sliding_window(const uint8_t* window, uint8_t lg_k, uint32_t num_coupons, compressed_state<A>& target) const;

  vector_u32<A> uncompress_surprising_values(const uint32_t* data, uint32_t data_words, uint32_t num_pairs, uint8_t lg_k, const A& allocator) const;
  void uncompress_sliding_window(const uint32_t* data, uint32_t data_words, vector_u8<A>& window, uint8_t lg_k, uint32_t num_coupons) const;

  static size_t safe_length_for_compressed_pair_buf(uint32_t k, uint32_t num_pairs, uint8_t num_base_bits);
  static size_t safe_length_for_compressed_window_buf(uint32_t k);
  static uint8_t determine_pseudo_phase(uint8_t lg_k, uint32_t c);

  static inline vector_u32<A> tricky_get_pairs_from_window(const uint8_t* window, uint32_t k, uint32_t num_pairs_to_get, uint32_t empty_space, const A& allocator);
  static inline uint8_t golomb_choose_number_of_base_bits(uint32_t k, uint64_t count);
};

} /* namespace datasketches */

#include "cpc_compressor_impl.hpp"

#endif
