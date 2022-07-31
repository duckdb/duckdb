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

#ifndef CPC_COMPRESSOR_IMPL_HPP_
#define CPC_COMPRESSOR_IMPL_HPP_

#include <memory>
#include <stdexcept>

#include "compression_data.hpp"
#include "cpc_util.hpp"
#include "cpc_common.hpp"
#include "count_zeros.hpp"

namespace datasketches {

// construct on first use
template<typename A>
cpc_compressor<A>& get_compressor() {
  static cpc_compressor<A>* instance = new cpc_compressor<A>(); // use new for global initialization
  return *instance;
}

template<typename A>
cpc_compressor<A>::cpc_compressor() {
  make_decoding_tables();
}

template<typename A>
cpc_compressor<A>::~cpc_compressor() {
  free_decoding_tables();
}

template<typename A>
uint8_t* cpc_compressor<A>::make_inverse_permutation(const uint8_t* permu, unsigned length) {
  uint8_t* inverse = new uint8_t[length]; // use new for global initialization
  for (unsigned i = 0; i < length; i++) {
    inverse[permu[i]] = static_cast<uint8_t>(i);
  }
  for (unsigned i = 0; i < length; i++) {
    if (permu[inverse[i]] != i) throw std::logic_error("inverse permutation error");
  }
  return inverse;
}

/* Given an encoding table that maps unsigned bytes to codewords
   of length at most 12, this builds a size-4096 decoding table */
// The second argument is typically 256, but can be other values such as 65.
template<typename A>
uint16_t* cpc_compressor<A>::make_decoding_table(const uint16_t* encoding_table, unsigned num_byte_values) {
  uint16_t* decoding_table = new uint16_t[4096]; // use new for global initialization
  for (unsigned byte_value = 0; byte_value < num_byte_values; byte_value++) {
    const uint16_t encoding_entry = encoding_table[byte_value];
    const uint16_t code_value = encoding_entry & 0xfff;
    const uint8_t code_length = encoding_entry >> 12;
    const uint16_t decoding_entry = static_cast<uint16_t>((code_length << 8) | byte_value);
    const uint8_t garbage_length = 12 - code_length;
    const uint32_t num_copies = 1 << garbage_length;
    for (uint32_t garbage_bits = 0; garbage_bits < num_copies; garbage_bits++) {
      const uint16_t extended_code_value = static_cast<uint16_t>(code_value | (garbage_bits << code_length));
      decoding_table[extended_code_value & 0xfff] = decoding_entry;
    }
  }
  return decoding_table;
}

template<typename A>
void cpc_compressor<A>::validate_decoding_table(const uint16_t* decoding_table, const uint16_t* encoding_table) const {
  for (int decode_this = 0; decode_this < 4096; decode_this++) {
    const int tmp_d = decoding_table[decode_this];
    const int decoded_byte = tmp_d & 0xff;
    const int decoded_length = tmp_d >> 8;

    const int tmp_e = encoding_table[decoded_byte];
    const int encoded_bit_pattern = tmp_e & 0xfff;
    const int encoded_length = tmp_e >> 12;

    if (decoded_length != encoded_length) throw std::logic_error("decoded length error");
    if (encoded_bit_pattern != (decode_this & ((1 << decoded_length) - 1))) throw std::logic_error("bit pattern error");
  }
}

template<typename A>
void cpc_compressor<A>::make_decoding_tables() {
  length_limited_unary_decoding_table65 = make_decoding_table(length_limited_unary_encoding_table65, 65);
  validate_decoding_table(
      length_limited_unary_decoding_table65,
      length_limited_unary_encoding_table65
  );

  for (int i = 0; i < (16 + 6); i++) {
    decoding_tables_for_high_entropy_byte[i] = make_decoding_table(encoding_tables_for_high_entropy_byte[i], 256);
    validate_decoding_table(
        decoding_tables_for_high_entropy_byte[i],
        encoding_tables_for_high_entropy_byte[i]
    );
  }

  for (int i = 0; i < 16; i++) {
    column_permutations_for_decoding[i] = make_inverse_permutation(column_permutations_for_encoding[i], 56);
  }
}

template<typename A>
void cpc_compressor<A>::free_decoding_tables() {
  delete[] length_limited_unary_decoding_table65;
  for (int i = 0; i < (16 + 6); i++) {
    delete[] decoding_tables_for_high_entropy_byte[i];
  }
  for (int i = 0; i < 16; i++) {
    delete[] column_permutations_for_decoding[i];
  }
}

template<typename A>
void cpc_compressor<A>::compress(const cpc_sketch_alloc<A>& source, compressed_state<A>& result) const {
  switch (source.determine_flavor()) {
    case cpc_sketch_alloc<A>::flavor::EMPTY:
      break;
    case cpc_sketch_alloc<A>::flavor::SPARSE:
      compress_sparse_flavor(source, result);
      if (result.window_data.size() > 0) throw std::logic_error("window is not expected");
      if (result.table_data.size() == 0) throw std::logic_error("table is expected");
      break;
    case cpc_sketch_alloc<A>::flavor::HYBRID:
      compress_hybrid_flavor(source, result);
      if (result.window_data.size() > 0) throw std::logic_error("window is not expected");
      if (result.table_data.size() == 0) throw std::logic_error("table is expected");
      break;
    case cpc_sketch_alloc<A>::flavor::PINNED:
      compress_pinned_flavor(source, result);
      if (result.window_data.size() == 0) throw std::logic_error("window is not expected");
      break;
    case cpc_sketch_alloc<A>::flavor::SLIDING:
      compress_sliding_flavor(source, result);
      if (result.window_data.size() == 0) throw std::logic_error("window is expected");
      break;
    default: throw std::logic_error("Unknown sketch flavor");
  }
}

template<typename A>
void cpc_compressor<A>::uncompress(const compressed_state<A>& source, uncompressed_state<A>& target, uint8_t lg_k, uint32_t num_coupons) const {
  switch (cpc_sketch_alloc<A>::determine_flavor(lg_k, num_coupons)) {
    case cpc_sketch_alloc<A>::flavor::EMPTY:
      target.table = u32_table<A>(2, 6 + lg_k, source.table_data.get_allocator());
      break;
    case cpc_sketch_alloc<A>::flavor::SPARSE:
      uncompress_sparse_flavor(source, target, lg_k);
      break;
    case cpc_sketch_alloc<A>::flavor::HYBRID:
      uncompress_hybrid_flavor(source, target, lg_k);
      break;
    case cpc_sketch_alloc<A>::flavor::PINNED:
      if (source.window_data.size() == 0) throw std::logic_error("window is expected");
      uncompress_pinned_flavor(source, target, lg_k, num_coupons);
      break;
    case cpc_sketch_alloc<A>::flavor::SLIDING:
      uncompress_sliding_flavor(source, target, lg_k, num_coupons);
      break;
    default: std::logic_error("Unknown sketch flavor");
  }
}

template<typename A>
void cpc_compressor<A>::compress_sparse_flavor(const cpc_sketch_alloc<A>& source, compressed_state<A>& result) const {
  if (source.sliding_window.size() > 0) throw std::logic_error("unexpected sliding window");
  vector_u32<A> pairs = source.surprising_value_table.unwrapping_get_items();
  u32_table<A>::introspective_insertion_sort(pairs.data(), 0, pairs.size());
  compress_surprising_values(pairs, source.get_lg_k(), result);
}

template<typename A>
void cpc_compressor<A>::uncompress_sparse_flavor(const compressed_state<A>& source, uncompressed_state<A>& target, uint8_t lg_k) const {
  if (source.window_data.size() > 0) throw std::logic_error("unexpected sliding window");
  if (source.table_data.size() == 0) throw std::logic_error("table is expected");
  vector_u32<A> pairs = uncompress_surprising_values(source.table_data.data(), source.table_data_words, source.table_num_entries,
      lg_k, source.table_data.get_allocator());
  target.table = u32_table<A>::make_from_pairs(pairs.data(), source.table_num_entries, lg_k, pairs.get_allocator());
}

// This is complicated because it effectively builds a Sparse version
// of a Pinned sketch before compressing it. Hence the name Hybrid.
template<typename A>
void cpc_compressor<A>::compress_hybrid_flavor(const cpc_sketch_alloc<A>& source, compressed_state<A>& result) const {
  if (source.sliding_window.size() == 0) throw std::logic_error("no sliding window");
  if (source.window_offset != 0) throw std::logic_error("window_offset != 0");
  const uint32_t k = 1 << source.get_lg_k();
  vector_u32<A> pairs_from_table = source.surprising_value_table.unwrapping_get_items();
  const uint32_t num_pairs_from_table = static_cast<uint32_t>(pairs_from_table.size());
  if (num_pairs_from_table > 0) u32_table<A>::introspective_insertion_sort(pairs_from_table.data(), 0, num_pairs_from_table);
  const uint32_t num_pairs_from_window = source.get_num_coupons() - num_pairs_from_table; // because the window offset is zero

  vector_u32<A> all_pairs = tricky_get_pairs_from_window(source.sliding_window.data(), k, num_pairs_from_window, num_pairs_from_table, source.get_allocator());

  u32_table<A>::merge(
      pairs_from_table.data(), 0, pairs_from_table.size(),
      all_pairs.data(), num_pairs_from_table, num_pairs_from_window,
      all_pairs.data(), 0
  );  // note the overlapping subarray trick

  compress_surprising_values(all_pairs, source.get_lg_k(), result);
}

template<typename A>
void cpc_compressor<A>::uncompress_hybrid_flavor(const compressed_state<A>& source, uncompressed_state<A>& target, uint8_t lg_k) const {
  if (source.window_data.size() > 0) throw std::logic_error("window is not expected");
  if (source.table_data.size() == 0) throw std::logic_error("table is expected");
  vector_u32<A> pairs = uncompress_surprising_values(source.table_data.data(), source.table_data_words, source.table_num_entries,
      lg_k, source.table_data.get_allocator());

  // In the hybrid flavor, some of these pairs actually
  // belong in the window, so we will separate them out,
  // moving the "true" pairs to the bottom of the array.
  const uint32_t k = 1 << lg_k;
  target.window.resize(k, 0); // important: zero the memory
  uint32_t next_true_pair = 0;
  for (uint32_t i = 0; i < source.table_num_entries; i++) {
    const uint32_t row_col = pairs[i];
    if (row_col == UINT32_MAX) throw std::logic_error("empty marker is not expected");
    const uint8_t col = row_col & 63;
    if (col < 8) {
      const uint32_t row = row_col >> 6;
      target.window[row] |= 1 << col; // set the window bit
    } else {
      pairs[next_true_pair++] = row_col; // move true pair down
    }
  }
  target.table = u32_table<A>::make_from_pairs(pairs.data(), next_true_pair, lg_k, pairs.get_allocator());
}

template<typename A>
void cpc_compressor<A>::compress_pinned_flavor(const cpc_sketch_alloc<A>& source, compressed_state<A>& result) const {
  compress_sliding_window(source.sliding_window.data(), source.get_lg_k(), source.get_num_coupons(), result);
  vector_u32<A> pairs = source.surprising_value_table.unwrapping_get_items();
  if (pairs.size() > 0) {
    // Here we subtract 8 from the column indices. Because they are stored in the low 6 bits
    // of each row_col pair, and because no column index is less than 8 for a "Pinned" sketch,
    // we can simply subtract 8 from the pairs themselves.

    // shift the columns over by 8 positions before compressing (because of the window)
    for (size_t i = 0; i < pairs.size(); i++) {
      if ((pairs[i] & 63) < 8) throw std::logic_error("(pairs[i] & 63) < 8");
      pairs[i] -= 8;
    }

    if (pairs.size() > 0) u32_table<A>::introspective_insertion_sort(pairs.data(), 0, pairs.size());
    compress_surprising_values(pairs, source.get_lg_k(), result);
  }
}

template<typename A>
void cpc_compressor<A>::uncompress_pinned_flavor(const compressed_state<A>& source, uncompressed_state<A>& target,
    uint8_t lg_k, uint32_t num_coupons) const {
  if (source.window_data.size() == 0) throw std::logic_error("window is expected");
  uncompress_sliding_window(source.window_data.data(), source.window_data_words, target.window, lg_k, num_coupons);
  const uint32_t num_pairs = source.table_num_entries;
  if (num_pairs == 0) {
    target.table = u32_table<A>(2, 6 + lg_k, source.table_data.get_allocator());
  } else {
    if (source.table_data.size() == 0) throw std::logic_error("table is expected");
    vector_u32<A> pairs = uncompress_surprising_values(source.table_data.data(), source.table_data_words, num_pairs,
        lg_k, source.table_data.get_allocator());
    // undo the compressor's 8-column shift
    for (uint32_t i = 0; i < num_pairs; i++) {
      if ((pairs[i] & 63) >= 56) throw std::logic_error("(pairs[i] & 63) >= 56");
      pairs[i] += 8;
    }
    target.table = u32_table<A>::make_from_pairs(pairs.data(), num_pairs, lg_k, pairs.get_allocator());
  }
}

template<typename A>
void cpc_compressor<A>::compress_sliding_flavor(const cpc_sketch_alloc<A>& source, compressed_state<A>& result) const {
  compress_sliding_window(source.sliding_window.data(), source.get_lg_k(), source.get_num_coupons(), result);
  vector_u32<A> pairs = source.surprising_value_table.unwrapping_get_items();
  if (pairs.size() > 0) {
    // Here we apply a complicated transformation to the column indices, which
    // changes the implied ordering of the pairs, so we must do it before sorting.

    const uint8_t pseudo_phase = determine_pseudo_phase(source.get_lg_k(), source.get_num_coupons());
    if (pseudo_phase >= 16) throw std::logic_error("unexpected pseudo phase for sliding flavor");
    const uint8_t* permutation = column_permutations_for_encoding[pseudo_phase];

    const uint8_t offset = source.window_offset;
    if (offset > 56) throw std::out_of_range("offset out of range");

    for (size_t i = 0; i < pairs.size(); i++) {
      const uint32_t row_col = pairs[i];
      const uint32_t row = row_col >> 6;
      uint8_t col = row_col & 63;
      // first rotate the columns into a canonical configuration: new = ((old - (offset+8)) + 64) mod 64
      col = (col + 56 - offset) & 63;
      if (col >= 56) throw std::out_of_range("col out of range");
      // then apply the permutation
      col = permutation[col];
      pairs[i] = (row << 6) | col;
    }

    if (pairs.size() > 0) u32_table<A>::introspective_insertion_sort(pairs.data(), 0, pairs.size());
    compress_surprising_values(pairs, source.get_lg_k(), result);
  }
}

template<typename A>
void cpc_compressor<A>::uncompress_sliding_flavor(const compressed_state<A>& source, uncompressed_state<A>& target,
    uint8_t lg_k, uint32_t num_coupons) const {
  if (source.window_data.size() == 0) throw std::logic_error("window is expected");
  uncompress_sliding_window(source.window_data.data(), source.window_data_words, target.window, lg_k, num_coupons);
  const uint32_t num_pairs = source.table_num_entries;
  if (num_pairs == 0) {
    target.table = u32_table<A>(2, 6 + lg_k, source.table_data.get_allocator());
  } else {
    if (source.table_data.size() == 0) throw std::logic_error("table is expected");
    vector_u32<A> pairs = uncompress_surprising_values(source.table_data.data(), source.table_data_words, num_pairs,
        lg_k, source.table_data.get_allocator());

    const uint8_t pseudo_phase = determine_pseudo_phase(lg_k, num_coupons);
    if (pseudo_phase >= 16) throw std::logic_error("unexpected pseudo phase for sliding flavor");
    const uint8_t* permutation = column_permutations_for_decoding[pseudo_phase];

    uint8_t offset = cpc_sketch_alloc<A>::determine_correct_offset(lg_k, num_coupons);
    if (offset > 56) throw std::out_of_range("offset out of range");

    for (uint32_t i = 0; i < num_pairs; i++) {
      const uint32_t row_col = pairs[i];
      const uint32_t row = row_col >> 6;
      uint8_t col = row_col & 63;
      // first undo the permutation
      col = permutation[col];
      // then undo the rotation: old = (new + (offset+8)) mod 64
      col = (col + (offset + 8)) & 63;
      pairs[i] = (row << 6) | col;
    }

    target.table = u32_table<A>::make_from_pairs(pairs.data(), num_pairs, lg_k, pairs.get_allocator());
  }
}

template<typename A>
void cpc_compressor<A>::compress_surprising_values(const vector_u32<A>& pairs, uint8_t lg_k, compressed_state<A>& result) const {
  const uint32_t k = 1 << lg_k;
  const uint32_t num_pairs = static_cast<uint32_t>(pairs.size());
  const uint8_t num_base_bits = golomb_choose_number_of_base_bits(k + num_pairs, num_pairs);
  const uint64_t table_len = safe_length_for_compressed_pair_buf(k, num_pairs, num_base_bits);
  result.table_data.resize(table_len);

  uint32_t csv_length = low_level_compress_pairs(pairs.data(), static_cast<uint32_t>(pairs.size()), num_base_bits, result.table_data.data());

  // At this point we could free the unused portion of the compression output buffer,
  // but it is not necessary if it is temporary
  // Note: realloc caused strange timing spikes for lgK = 11 and 12.

  result.table_data_words = csv_length;
  result.table_num_entries = num_pairs;
}

template<typename A>
vector_u32<A> cpc_compressor<A>::uncompress_surprising_values(const uint32_t* data, uint32_t data_words, uint32_t num_pairs,
    uint8_t lg_k, const A& allocator) const {
  const uint32_t k = 1 << lg_k;
  vector_u32<A> pairs(num_pairs, 0, allocator);
  const uint8_t num_base_bits = golomb_choose_number_of_base_bits(k + num_pairs, num_pairs);
  low_level_uncompress_pairs(pairs.data(), num_pairs, num_base_bits, data, data_words);
  return pairs;
}

template<typename A>
void cpc_compressor<A>::compress_sliding_window(const uint8_t* window, uint8_t lg_k, uint32_t num_coupons, compressed_state<A>& target) const {
  const uint32_t k = 1 << lg_k;
  const size_t window_buf_len = safe_length_for_compressed_window_buf(k);
  target.window_data.resize(window_buf_len);
  const uint8_t pseudo_phase = determine_pseudo_phase(lg_k, num_coupons);
  size_t data_words = low_level_compress_bytes(window, k, encoding_tables_for_high_entropy_byte[pseudo_phase], target.window_data.data());

  // At this point we could free the unused portion of the compression output buffer,
  // but it is not necessary if it is temporary
  // Note: realloc caused strange timing spikes for lgK = 11 and 12.

  target.window_data_words = static_cast<uint32_t>(data_words);
}

template<typename A>
void cpc_compressor<A>::uncompress_sliding_window(const uint32_t* data, uint32_t data_words, vector_u8<A>& window,
    uint8_t lg_k, uint32_t num_coupons) const {
  const uint32_t k = 1 << lg_k;
  window.resize(k); // zeroing not needed here (unlike the Hybrid Flavor)
  const uint8_t pseudo_phase = determine_pseudo_phase(lg_k, num_coupons);
  low_level_uncompress_bytes(window.data(), k, decoding_tables_for_high_entropy_byte[pseudo_phase], data, data_words);
}

template<typename A>
size_t cpc_compressor<A>::safe_length_for_compressed_pair_buf(uint32_t k, uint32_t num_pairs, uint8_t num_base_bits) {
  // Long ybits = k + numPairs; // simpler and safer UB
  // The following tighter UB on ybits is based on page 198
  // of the textbook "Managing Gigabytes" by Witten, Moffat, and Bell.
  // Notice that if numBaseBits == 0 it coincides with (k + numPairs).
  const size_t ybits = num_pairs * (1 + num_base_bits) + (k >> num_base_bits);
  const size_t xbits = 12 * num_pairs;
  const size_t padding = num_base_bits > 10 ? 0 : 10 - num_base_bits;
  return divide_longs_rounding_up(xbits + ybits + padding, 32);
}

// Explanation of padding: we write
// 1) xdelta (huffman, provides at least 1 bit, requires 12-bit lookahead)
// 2) ydeltaGolombHi (unary, provides at least 1 bit, requires 8-bit lookahead)
// 3) ydeltaGolombLo (straight B bits).
// So the 12-bit lookahead is the tight constraint, but there are at least (2 + B) bits emitted,
// so we would be safe with max (0, 10 - B) bits of padding at the end of the bitstream.
template<typename A>
size_t cpc_compressor<A>::safe_length_for_compressed_window_buf(uint32_t k) { // measured in 32-bit words
  const size_t bits = 12 * k + 11; // 11 bits of padding, due to 12-bit lookahead, with 1 bit certainly present.
  return divide_longs_rounding_up(bits, 32);
}

template<typename A>
uint8_t cpc_compressor<A>::determine_pseudo_phase(uint8_t lg_k, uint32_t c) {
  const uint32_t k = 1 << lg_k;
  // This mid-range logic produces pseudo-phases. They are used to select encoding tables.
  // The thresholds were chosen by hand after looking at plots of measured compression.
  if (1000 * c < 2375 * k) {
    if      (   4 * c <    3 * k) return 16 + 0; // mid-range table
    else if (  10 * c <   11 * k) return 16 + 1; // mid-range table
    else if ( 100 * c <  132 * k) return 16 + 2; // mid-range table
    else if (   3 * c <    5 * k) return 16 + 3; // mid-range table
    else if (1000 * c < 1965 * k) return 16 + 4; // mid-range table
    else if (1000 * c < 2275 * k) return 16 + 5; // mid-range table
    else return 6;  // steady-state table employed before its actual phase
  } else { // This steady-state logic produces true phases. They are used to select
         // encoding tables, and also column permutations for the "Sliding" flavor.
    if (lg_k < 4) throw std::logic_error("lgK < 4");
    const size_t tmp = c >> (lg_k - 4);
    const uint8_t phase = tmp & 15;
    if (phase < 0 || phase >= 16) throw std::out_of_range("wrong phase");
    return phase;
  }
}

static inline void maybe_flush_bitbuf(uint64_t& bitbuf, uint8_t& bufbits, uint32_t* wordarr, uint32_t& wordindex) {
  if (bufbits >= 32) {
    wordarr[wordindex++] = bitbuf & 0xffffffff;
    bitbuf = bitbuf >> 32;
    bufbits -= 32;
  }
}

static inline void maybe_fill_bitbuf(uint64_t& bitbuf, uint8_t& bufbits, const uint32_t* wordarr, uint32_t& wordindex, uint8_t minbits) {
  if (bufbits < minbits) {
    bitbuf |= static_cast<uint64_t>(wordarr[wordindex++]) << bufbits;
    bufbits += 32;
  }
}

// This returns the number of compressed words that were actually used.
// It is the caller's responsibility to ensure that the compressed_words array is long enough.
template<typename A>
uint32_t cpc_compressor<A>::low_level_compress_bytes(
    const uint8_t* byte_array, // input
    uint32_t num_bytes_to_encode,
    const uint16_t* encoding_table,
    uint32_t* compressed_words // output
) const {
  uint64_t bitbuf = 0; // bits are packed into this first, then are flushed to compressed_words
  uint8_t bufbits = 0; // number of bits currently in bitbuf; must be between 0 and 31
  uint32_t next_word_index = 0;

  for (uint32_t byte_index = 0; byte_index < num_bytes_to_encode; byte_index++) {
    const uint16_t code_info = encoding_table[byte_array[byte_index]];
    const uint64_t code_val = code_info & 0xfff;
    const uint8_t code_len = code_info >> 12;
    bitbuf |= (code_val << bufbits);
    bufbits += code_len;
    maybe_flush_bitbuf(bitbuf, bufbits, compressed_words, next_word_index);
  }

  // Pad the bitstream with 11 zero-bits so that the decompressor's 12-bit peek can't overrun its input.
  bufbits += 11;
  maybe_flush_bitbuf(bitbuf, bufbits, compressed_words, next_word_index);

  if (bufbits > 0) { // We are done encoding now, so we flush the bit buffer.
    if (bufbits >= 32) throw std::logic_error("bufbits >= 32");
    compressed_words[next_word_index++] = bitbuf & 0xffffffff;
    bitbuf = 0; bufbits = 0; // not really necessary
  }
  return next_word_index;
}

template<typename A>
void cpc_compressor<A>::low_level_uncompress_bytes(
    uint8_t* byte_array, // output
    uint32_t num_bytes_to_decode,
    const uint16_t* decoding_table,
    const uint32_t* compressed_words, // input
    uint32_t num_compressed_words
) const {
  uint32_t word_index = 0;
  uint64_t bitbuf = 0;
  uint8_t bufbits = 0;

  if (byte_array == nullptr) throw std::logic_error("byte_array == NULL");
  if (decoding_table == nullptr) throw std::logic_error("decoding_table == NULL");
  if (compressed_words == nullptr) throw std::logic_error("compressed_words == NULL");

  for (uint32_t byte_index = 0; byte_index < num_bytes_to_decode; byte_index++) {
    maybe_fill_bitbuf(bitbuf, bufbits, compressed_words, word_index, 12); // ensure 12 bits in bit buffer

    const size_t peek12 = bitbuf & 0xfff; // These 12 bits will include an entire Huffman codeword.
    const uint16_t lookup = decoding_table[peek12];
    const uint8_t code_word_length = lookup >> 8;
    const uint8_t decoded_byte = lookup & 0xff;
    byte_array[byte_index] = decoded_byte;
    bitbuf >>= code_word_length;
    bufbits -= code_word_length;
  }
  // Buffer over-run should be impossible unless there is a bug.
  // However, we might as well check here.
  if (word_index > num_compressed_words) throw std::logic_error("word_index > num_compressed_words");
}

static inline uint64_t read_unary(
    const uint32_t* compressed_words,
    uint32_t& next_word_index,
    uint64_t& bitbuf,
    uint8_t& bufbits
);

static inline void write_unary(
    uint32_t* compressed_words,
    uint32_t& next_word_index_ptr,
    uint64_t& bit_buf_ptr,
    uint8_t& buf_bits_ptr,
    uint64_t value
);

// Here "pairs" refers to row/column pairs that specify
// the positions of surprising values in the bit matrix.

// returns the number of compressed_words actually used
template<typename A>
uint32_t cpc_compressor<A>::low_level_compress_pairs(
    const uint32_t* pair_array,  // input
    uint32_t num_pairs_to_encode,
    uint8_t num_base_bits,
    uint32_t* compressed_words // output
) const {
  uint64_t bitbuf = 0;
  uint8_t bufbits = 0;
  uint32_t next_word_index = 0;
  const uint64_t golomb_lo_mask = (1 << num_base_bits) - 1;
  uint32_t predicted_row_index = 0;
  uint8_t predicted_col_index = 0;

  for (uint32_t pair_index = 0; pair_index < num_pairs_to_encode; pair_index++) {
    const uint32_t row_col = pair_array[pair_index];
    const uint32_t row_index = row_col >> 6;
    const uint8_t col_index = row_col & 63;

    if (row_index != predicted_row_index) predicted_col_index = 0;

    if (row_index < predicted_row_index) throw std::logic_error("row_index < predicted_row_index");
    if (col_index < predicted_col_index) throw std::logic_error("col_index < predicted_col_index");

    const uint32_t y_delta = row_index - predicted_row_index;
    const uint8_t x_delta = col_index - predicted_col_index;

    predicted_row_index = row_index;
    predicted_col_index = col_index + 1;

    const uint16_t code_info = length_limited_unary_encoding_table65[x_delta];
    const uint64_t code_val = code_info & 0xfff;
    const uint8_t code_len = static_cast<uint8_t>(code_info >> 12);
    bitbuf |= code_val << bufbits;
    bufbits += code_len;
    maybe_flush_bitbuf(bitbuf, bufbits, compressed_words, next_word_index);

    const uint64_t golomb_lo = y_delta & golomb_lo_mask;
    const uint64_t golomb_hi = y_delta >> num_base_bits;

    write_unary(compressed_words, next_word_index, bitbuf, bufbits, golomb_hi);

    bitbuf |= golomb_lo << bufbits;
    bufbits += num_base_bits;
    maybe_flush_bitbuf(bitbuf, bufbits, compressed_words, next_word_index);
  }

  // Pad the bitstream so that the decompressor's 12-bit peek can't overrun its input.
  const uint8_t padding = (num_base_bits > 10) ? 0 : 10 - num_base_bits;
  bufbits += padding;
  maybe_flush_bitbuf(bitbuf, bufbits, compressed_words, next_word_index);

  if (bufbits > 0) { // We are done encoding now, so we flush the bit buffer
    if (bufbits >= 32) throw std::logic_error("bufbits >= 32");
    compressed_words[next_word_index++] = bitbuf & 0xffffffff;
    bitbuf = 0; bufbits = 0; // not really necessary
  }

  return next_word_index;
}

template<typename A>
void cpc_compressor<A>::low_level_uncompress_pairs(
    uint32_t* pair_array, // output
    uint32_t num_pairs_to_decode,
    uint8_t num_base_bits,
    const uint32_t* compressed_words, // input
    uint32_t num_compressed_words
) const {
  uint32_t word_index = 0;
  uint64_t bitbuf = 0;
  uint8_t bufbits = 0;
  const uint64_t golomb_lo_mask = (1 << num_base_bits) - 1;
  uint32_t predicted_row_index = 0;
  uint8_t predicted_col_index = 0;

  // for each pair we need to read:
  // x_delta (12-bit length-limited unary)
  // y_delta_hi (unary)
  // y_delta_lo (basebits)

  for (uint32_t pair_index = 0; pair_index < num_pairs_to_decode; pair_index++) {
    maybe_fill_bitbuf(bitbuf, bufbits, compressed_words, word_index, 12); // ensure 12 bits in bit buffer
    const size_t peek12 = bitbuf & 0xfff;
    const uint16_t lookup = length_limited_unary_decoding_table65[peek12];
    const uint8_t code_word_length = lookup >> 8;
    const int8_t x_delta = lookup & 0xff;
    bitbuf >>= code_word_length;
    bufbits -= code_word_length;

    const uint64_t golomb_hi = read_unary(compressed_words, word_index, bitbuf, bufbits);

    maybe_fill_bitbuf(bitbuf, bufbits, compressed_words, word_index, num_base_bits); // ensure num_base_bits in bit buffer
    const uint64_t golomb_lo = bitbuf & golomb_lo_mask;
    bitbuf >>= num_base_bits;
    bufbits -= num_base_bits;
    const int64_t y_delta = (golomb_hi << num_base_bits) | golomb_lo;

    // Now that we have x_delta and y_delta, we can compute the pair's row and column
    if (y_delta > 0) predicted_col_index = 0;
    const uint32_t row_index = static_cast<uint32_t>(predicted_row_index + y_delta);
    const uint8_t col_index = predicted_col_index + x_delta;
    const uint32_t row_col = (row_index << 6) | col_index;
    pair_array[pair_index] = row_col;
    predicted_row_index = row_index;
    predicted_col_index = col_index + 1;
  }
  if (word_index > num_compressed_words) throw std::logic_error("word_index > num_compressed_words"); // check for buffer over-run
}

uint64_t read_unary(
    const uint32_t* compressed_words,
    uint32_t& next_word_index,
    uint64_t& bitbuf,
    uint8_t& bufbits
) {
  if (compressed_words == nullptr) throw std::logic_error("compressed_words == NULL");
  size_t subtotal = 0;
  while (true) {
    maybe_fill_bitbuf(bitbuf, bufbits, compressed_words, next_word_index, 8); // ensure 8 bits in bit buffer

    const uint8_t peek8 = bitbuf & 0xff; // These 8 bits include either all or part of the Unary codeword
    const uint8_t trailing_zeros = byte_trailing_zeros_table[peek8];

    if (trailing_zeros > 8) throw std::out_of_range("trailing_zeros out of range");
    if (trailing_zeros < 8) {
      bufbits -= 1 + trailing_zeros;
      bitbuf >>= 1 + trailing_zeros;
      return subtotal + trailing_zeros;
    }
    // The codeword was partial, so read some more
    subtotal += 8;
    bufbits -= 8;
    bitbuf >>= 8;
  }
}

void write_unary(
    uint32_t* compressed_words,
    uint32_t& next_word_index,
    uint64_t& bitbuf,
    uint8_t& bufbits,
    uint64_t value
) {
  if (compressed_words == nullptr) throw std::logic_error("compressed_words == NULL");
  if (bufbits > 31) throw std::out_of_range("bufbits out of range");

  uint64_t remaining = value;

  while (remaining >= 16) {
    remaining -= 16;
    // Here we output 16 zeros, but we don't need to physically write them into bitbuf
    // because it already contains zeros in that region.
    bufbits += 16; // Record the fact that 16 bits of output have occurred.
    maybe_flush_bitbuf(bitbuf, bufbits, compressed_words, next_word_index);
  }

  if (remaining > 15) throw std::out_of_range("remaining out of range");

  const uint64_t the_unary_code = 1ULL << remaining;
  bitbuf |= the_unary_code << bufbits;
  bufbits += static_cast<uint8_t>(remaining + 1);
  maybe_flush_bitbuf(bitbuf, bufbits, compressed_words, next_word_index);
}

// The empty space that this leaves at the beginning of the output array
// will be filled in later by the caller.
template<typename A>
vector_u32<A> cpc_compressor<A>::tricky_get_pairs_from_window(const uint8_t* window, uint32_t k, uint32_t num_pairs_to_get,
    uint32_t empty_space, const A& allocator) {
  const size_t output_length = empty_space + num_pairs_to_get;
  vector_u32<A> pairs(output_length, 0, allocator);
  size_t pair_index = empty_space;
  for (unsigned row_index = 0; row_index < k; row_index++) {
    uint8_t byte = window[row_index];
    while (byte != 0) {
      const uint8_t col_index = byte_trailing_zeros_table[byte];
      byte = byte ^ (1 << col_index); // erase the 1
      pairs[pair_index++] = (row_index << 6) | col_index;
    }
  }
  if (pair_index != output_length) throw std::logic_error("pair_index != output_length");
  return pairs;
}

// returns an integer that is between
// zero and ceiling(log_2(k)) - 1, inclusive
template<typename A>
uint8_t cpc_compressor<A>::golomb_choose_number_of_base_bits(uint32_t k, uint64_t count) {
  if (k < 1) throw std::invalid_argument("golomb_choose_number_of_base_bits: k < 1");
  if (count < 1) throw std::invalid_argument("golomb_choose_number_of_base_bits: count < 1");
  const uint64_t quotient = (k - count) / count; // integer division
  if (quotient == 0) return 0;
  else return floor_log2_of_long(quotient);
}

} /* namespace datasketches */

#endif
