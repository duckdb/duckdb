/* Copyright 2010 Google Inc. All Rights Reserved.

   Distributed under MIT license.
   See file LICENSE for detail or copy at https://opensource.org/licenses/MIT
*/

/* A (forgetful) hash table to the data seen by the compressor, to
   help create backward references to previous data. */

#ifndef BROTLI_ENC_HASH_H_
#define BROTLI_ENC_HASH_H_

#include <stdlib.h>  /* exit */
#include <string.h>  /* memcmp, memset */

#include <brotli/types.h>

#include "../common/brotli_constants.h"
#include "../common/dictionary.h"
#include "../common/brotli_platform.h"
#include "compound_dictionary.h"
#include "encoder_dict.h"
#include "fast_log.h"
#include "find_match_length.h"
#include "memory.h"
#include "quality.h"
#include "static_dict.h"

namespace duckdb_brotli {

typedef struct {
  /**
   * Dynamically allocated areas; regular hasher uses one or two allocations;
   * "composite" hasher uses up to 4 allocations.
   */
  void* extra[4];

  /**
   * False before the fisrt invocation of HasherSetup (where "extra" memory)
   * is allocated.
   */
  BROTLI_BOOL is_setup_;

  size_t dict_num_lookups;
  size_t dict_num_matches;

  BrotliHasherParams params;

  /**
   * False if hasher needs to be "prepared" before use (before the first
   * invocation of HasherSetup or after HasherReset). "preparation" is hasher
   * data initialization (using input ringbuffer).
   */
  BROTLI_BOOL is_prepared_;
} HasherCommon;

#define score_t size_t

static const uint32_t kCutoffTransformsCount = 10;
/*   0,  12,   27,    23,    42,    63,    56,    48,    59,    64 */
/* 0+0, 4+8, 8+19, 12+11, 16+26, 20+43, 24+32, 28+20, 32+27, 36+28 */
static const uint64_t kCutoffTransforms =
    BROTLI_MAKE_UINT64_T(0x071B520A, 0xDA2D3200);

typedef struct HasherSearchResult {
  size_t len;
  size_t distance;
  score_t score;
  int len_code_delta; /* == len_code - len */
} HasherSearchResult;

/* kHashMul32 multiplier has these properties:
   * The multiplier must be odd. Otherwise we may lose the highest bit.
   * No long streaks of ones or zeros.
   * There is no effort to ensure that it is a prime, the oddity is enough
     for this use.
   * The number has been tuned heuristically against compression benchmarks. */
static const uint32_t kHashMul32 = 0x1E35A7BD;
static const uint64_t kHashMul64 =
    BROTLI_MAKE_UINT64_T(0x1FE35A7Bu, 0xD3579BD3u);

static BROTLI_INLINE uint32_t Hash14(const uint8_t* data) {
  uint32_t h = BROTLI_UNALIGNED_LOAD32LE(data) * kHashMul32;
  /* The higher bits contain more mixture from the multiplication,
     so we take our results from there. */
  return h >> (32 - 14);
}

static BROTLI_INLINE void PrepareDistanceCache(
    int* BROTLI_RESTRICT distance_cache, const int num_distances) {
  if (num_distances > 4) {
    int last_distance = distance_cache[0];
    distance_cache[4] = last_distance - 1;
    distance_cache[5] = last_distance + 1;
    distance_cache[6] = last_distance - 2;
    distance_cache[7] = last_distance + 2;
    distance_cache[8] = last_distance - 3;
    distance_cache[9] = last_distance + 3;
    if (num_distances > 10) {
      int next_last_distance = distance_cache[1];
      distance_cache[10] = next_last_distance - 1;
      distance_cache[11] = next_last_distance + 1;
      distance_cache[12] = next_last_distance - 2;
      distance_cache[13] = next_last_distance + 2;
      distance_cache[14] = next_last_distance - 3;
      distance_cache[15] = next_last_distance + 3;
    }
  }
}

#define BROTLI_LITERAL_BYTE_SCORE 135
#define BROTLI_DISTANCE_BIT_PENALTY 30
/* Score must be positive after applying maximal penalty. */
#define BROTLI_SCORE_BASE (BROTLI_DISTANCE_BIT_PENALTY * 8 * sizeof(size_t))

/* Usually, we always choose the longest backward reference. This function
   allows for the exception of that rule.

   If we choose a backward reference that is further away, it will
   usually be coded with more bits. We approximate this by assuming
   log2(distance). If the distance can be expressed in terms of the
   last four distances, we use some heuristic constants to estimate
   the bits cost. For the first up to four literals we use the bit
   cost of the literals from the literal cost model, after that we
   use the average bit cost of the cost model.

   This function is used to sometimes discard a longer backward reference
   when it is not much longer and the bit cost for encoding it is more
   than the saved literals.

   backward_reference_offset MUST be positive. */
static BROTLI_INLINE score_t BackwardReferenceScore(
    size_t copy_length, size_t backward_reference_offset) {
  return BROTLI_SCORE_BASE + BROTLI_LITERAL_BYTE_SCORE * (score_t)copy_length -
      BROTLI_DISTANCE_BIT_PENALTY * Log2FloorNonZero(backward_reference_offset);
}

static BROTLI_INLINE score_t BackwardReferenceScoreUsingLastDistance(
    size_t copy_length) {
  return BROTLI_LITERAL_BYTE_SCORE * (score_t)copy_length +
      BROTLI_SCORE_BASE + 15;
}

static BROTLI_INLINE score_t BackwardReferencePenaltyUsingLastDistance(
    size_t distance_short_code) {
  return (score_t)39 + ((0x1CA10 >> (distance_short_code & 0xE)) & 0xE);
}

static BROTLI_INLINE BROTLI_BOOL TestStaticDictionaryItem(
    const BrotliEncoderDictionary* dictionary, size_t len, size_t word_idx,
    const uint8_t* data, size_t max_length, size_t max_backward,
    size_t max_distance, HasherSearchResult* out) {
  size_t offset;
  size_t matchlen;
  size_t backward;
  score_t score;
  offset = dictionary->words->offsets_by_length[len] + len * word_idx;
  if (len > max_length) {
    return BROTLI_FALSE;
  }

  matchlen =
      FindMatchLengthWithLimit(data, &dictionary->words->data[offset], len);
  if (matchlen + dictionary->cutoffTransformsCount <= len || matchlen == 0) {
    return BROTLI_FALSE;
  }
  {
    size_t cut = len - matchlen;
    size_t transform_id = (cut << 2) +
        (size_t)((dictionary->cutoffTransforms >> (cut * 6)) & 0x3F);
    backward = max_backward + 1 + word_idx +
        (transform_id << dictionary->words->size_bits_by_length[len]);
  }
  if (backward > max_distance) {
    return BROTLI_FALSE;
  }
  score = BackwardReferenceScore(matchlen, backward);
  if (score < out->score) {
    return BROTLI_FALSE;
  }
  out->len = matchlen;
  out->len_code_delta = (int)len - (int)matchlen;
  out->distance = backward;
  out->score = score;
  return BROTLI_TRUE;
}

static BROTLI_INLINE void SearchInStaticDictionary(
    const BrotliEncoderDictionary* dictionary,
    HasherCommon* common, const uint8_t* data, size_t max_length,
    size_t max_backward, size_t max_distance,
    HasherSearchResult* out, BROTLI_BOOL shallow) {
  size_t key;
  size_t i;
  if (common->dict_num_matches < (common->dict_num_lookups >> 7)) {
    return;
  }
  key = Hash14(data) << 1;
  for (i = 0; i < (shallow ? 1u : 2u); ++i, ++key) {
    common->dict_num_lookups++;
    if (dictionary->hash_table_lengths[key] != 0) {
      BROTLI_BOOL item_matches = TestStaticDictionaryItem(
          dictionary, dictionary->hash_table_lengths[key],
          dictionary->hash_table_words[key], data,
          max_length, max_backward, max_distance, out);
      if (item_matches) {
        common->dict_num_matches++;
      }
    }
  }
}

typedef struct BackwardMatch {
  uint32_t distance;
  uint32_t length_and_code;
} BackwardMatch;

static BROTLI_INLINE void InitBackwardMatch(BackwardMatch* self,
    size_t dist, size_t len) {
  self->distance = (uint32_t)dist;
  self->length_and_code = (uint32_t)(len << 5);
}

static BROTLI_INLINE void InitDictionaryBackwardMatch(BackwardMatch* self,
    size_t dist, size_t len, size_t len_code) {
  self->distance = (uint32_t)dist;
  self->length_and_code =
      (uint32_t)((len << 5) | (len == len_code ? 0 : len_code));
}

static BROTLI_INLINE size_t BackwardMatchLength(const BackwardMatch* self) {
  return self->length_and_code >> 5;
}

static BROTLI_INLINE size_t BackwardMatchLengthCode(const BackwardMatch* self) {
  size_t code = self->length_and_code & 31;
  return code ? code : BackwardMatchLength(self);
}

#define EXPAND_CAT(a, b) CAT(a, b)
#define CAT(a, b) a ## b
#define FN(X) EXPAND_CAT(X, HASHER())

#define HASHER() H10
#define BUCKET_BITS 17
#define MAX_TREE_SEARCH_DEPTH 64
#define MAX_TREE_COMP_LENGTH 128
/* NOLINT(build/header_guard) */
/* Copyright 2016 Google Inc. All Rights Reserved.

   Distributed under MIT license.
   See file LICENSE for detail or copy at https://opensource.org/licenses/MIT
*/

/* template parameters: FN, BUCKET_BITS, MAX_TREE_COMP_LENGTH,
                        MAX_TREE_SEARCH_DEPTH */

/* A (forgetful) hash table where each hash bucket contains a binary tree of
   sequences whose first 4 bytes share the same hash code.
   Each sequence is MAX_TREE_COMP_LENGTH long and is identified by its starting
   position in the input data. The binary tree is sorted by the lexicographic
   order of the sequences, and it is also a max-heap with respect to the
   starting positions. */

#define HashToBinaryTree HASHER()

#define BUCKET_SIZE (1 << BUCKET_BITS)

static BROTLI_INLINE size_t FN(HashTypeLength)(void) { return 4; }
static BROTLI_INLINE size_t FN(StoreLookahead)(void) {
  return MAX_TREE_COMP_LENGTH;
}

static uint32_t FN(HashBytes)(const uint8_t* BROTLI_RESTRICT data) {
  uint32_t h = BROTLI_UNALIGNED_LOAD32LE(data) * kHashMul32;
  /* The higher bits contain more mixture from the multiplication,
     so we take our results from there. */
  return h >> (32 - BUCKET_BITS);
}

typedef struct HashToBinaryTree {
  /* The window size minus 1 */
  size_t window_mask_;

  /* Hash table that maps the 4-byte hashes of the sequence to the last
     position where this hash was found, which is the root of the binary
     tree of sequences that share this hash bucket. */
  uint32_t* buckets_;  /* uint32_t[BUCKET_SIZE]; */

  /* A position used to mark a non-existent sequence, i.e. a tree is empty if
     its root is at invalid_pos_ and a node is a leaf if both its children
     are at invalid_pos_. */
  uint32_t invalid_pos_;

  /* --- Dynamic size members --- */

  /* The union of the binary trees of each hash bucket. The root of the tree
     corresponding to a hash is a sequence starting at buckets_[hash] and
     the left and right children of a sequence starting at pos are
     forest_[2 * pos] and forest_[2 * pos + 1]. */
  uint32_t* forest_;  /* uint32_t[2 * num_nodes] */
} HashToBinaryTree;

static void FN(Initialize)(
    HasherCommon* common, HashToBinaryTree* BROTLI_RESTRICT self,
    const BrotliEncoderParams* params) {
  self->buckets_ = (uint32_t*)common->extra[0];
  self->forest_ = (uint32_t*)common->extra[1];

  self->window_mask_ = (1u << params->lgwin) - 1u;
  self->invalid_pos_ = (uint32_t)(0 - self->window_mask_);
}

static void FN(Prepare)
    (HashToBinaryTree* BROTLI_RESTRICT self, BROTLI_BOOL one_shot,
    size_t input_size, const uint8_t* BROTLI_RESTRICT data) {
  uint32_t invalid_pos = self->invalid_pos_;
  uint32_t i;
  uint32_t* BROTLI_RESTRICT buckets = self->buckets_;
  BROTLI_UNUSED(data);
  BROTLI_UNUSED(one_shot);
  BROTLI_UNUSED(input_size);
  for (i = 0; i < BUCKET_SIZE; i++) {
    buckets[i] = invalid_pos;
  }
}

static BROTLI_INLINE void FN(HashMemAllocInBytes)(
    const BrotliEncoderParams* params, BROTLI_BOOL one_shot,
    size_t input_size, size_t* alloc_size) {
  size_t num_nodes = (size_t)1 << params->lgwin;
  if (one_shot && input_size < num_nodes) {
    num_nodes = input_size;
  }
  alloc_size[0] = sizeof(uint32_t) * BUCKET_SIZE;
  alloc_size[1] = 2 * sizeof(uint32_t) * num_nodes;
}

static BROTLI_INLINE size_t FN(LeftChildIndex)(
    HashToBinaryTree* BROTLI_RESTRICT self,
    const size_t pos) {
  return 2 * (pos & self->window_mask_);
}

static BROTLI_INLINE size_t FN(RightChildIndex)(
    HashToBinaryTree* BROTLI_RESTRICT self,
    const size_t pos) {
  return 2 * (pos & self->window_mask_) + 1;
}

/* Stores the hash of the next 4 bytes and in a single tree-traversal, the
   hash bucket's binary tree is searched for matches and is re-rooted at the
   current position.

   If less than MAX_TREE_COMP_LENGTH data is available, the hash bucket of the
   current position is searched for matches, but the state of the hash table
   is not changed, since we can not know the final sorting order of the
   current (incomplete) sequence.

   This function must be called with increasing cur_ix positions. */
static BROTLI_INLINE BackwardMatch* FN(StoreAndFindMatches)(
    HashToBinaryTree* BROTLI_RESTRICT self, const uint8_t* BROTLI_RESTRICT data,
    const size_t cur_ix, const size_t ring_buffer_mask, const size_t max_length,
    const size_t max_backward, size_t* const BROTLI_RESTRICT best_len,
    BackwardMatch* BROTLI_RESTRICT matches) {
  const size_t cur_ix_masked = cur_ix & ring_buffer_mask;
  const size_t max_comp_len =
      BROTLI_MIN(size_t, max_length, MAX_TREE_COMP_LENGTH);
  const BROTLI_BOOL should_reroot_tree =
      TO_BROTLI_BOOL(max_length >= MAX_TREE_COMP_LENGTH);
  const uint32_t key = FN(HashBytes)(&data[cur_ix_masked]);
  uint32_t* BROTLI_RESTRICT buckets = self->buckets_;
  uint32_t* BROTLI_RESTRICT forest = self->forest_;
  size_t prev_ix = buckets[key];
  /* The forest index of the rightmost node of the left subtree of the new
     root, updated as we traverse and re-root the tree of the hash bucket. */
  size_t node_left = FN(LeftChildIndex)(self, cur_ix);
  /* The forest index of the leftmost node of the right subtree of the new
     root, updated as we traverse and re-root the tree of the hash bucket. */
  size_t node_right = FN(RightChildIndex)(self, cur_ix);
  /* The match length of the rightmost node of the left subtree of the new
     root, updated as we traverse and re-root the tree of the hash bucket. */
  size_t best_len_left = 0;
  /* The match length of the leftmost node of the right subtree of the new
     root, updated as we traverse and re-root the tree of the hash bucket. */
  size_t best_len_right = 0;
  size_t depth_remaining;
  if (should_reroot_tree) {
    buckets[key] = (uint32_t)cur_ix;
  }
  for (depth_remaining = MAX_TREE_SEARCH_DEPTH; ; --depth_remaining) {
    const size_t backward = cur_ix - prev_ix;
    const size_t prev_ix_masked = prev_ix & ring_buffer_mask;
    if (backward == 0 || backward > max_backward || depth_remaining == 0) {
      if (should_reroot_tree) {
        forest[node_left] = self->invalid_pos_;
        forest[node_right] = self->invalid_pos_;
      }
      break;
    }
    {
      const size_t cur_len = BROTLI_MIN(size_t, best_len_left, best_len_right);
      size_t len;
      BROTLI_DCHECK(cur_len <= MAX_TREE_COMP_LENGTH);
      len = cur_len +
          FindMatchLengthWithLimit(&data[cur_ix_masked + cur_len],
                                   &data[prev_ix_masked + cur_len],
                                   max_length - cur_len);
      BROTLI_DCHECK(
          0 == memcmp(&data[cur_ix_masked], &data[prev_ix_masked], len));
      if (matches && len > *best_len) {
        *best_len = len;
        InitBackwardMatch(matches++, backward, len);
      }
      if (len >= max_comp_len) {
        if (should_reroot_tree) {
          forest[node_left] = forest[FN(LeftChildIndex)(self, prev_ix)];
          forest[node_right] = forest[FN(RightChildIndex)(self, prev_ix)];
        }
        break;
      }
      if (data[cur_ix_masked + len] > data[prev_ix_masked + len]) {
        best_len_left = len;
        if (should_reroot_tree) {
          forest[node_left] = (uint32_t)prev_ix;
        }
        node_left = FN(RightChildIndex)(self, prev_ix);
        prev_ix = forest[node_left];
      } else {
        best_len_right = len;
        if (should_reroot_tree) {
          forest[node_right] = (uint32_t)prev_ix;
        }
        node_right = FN(LeftChildIndex)(self, prev_ix);
        prev_ix = forest[node_right];
      }
    }
  }
  return matches;
}

/* Finds all backward matches of &data[cur_ix & ring_buffer_mask] up to the
   length of max_length and stores the position cur_ix in the hash table.

   Sets *num_matches to the number of matches found, and stores the found
   matches in matches[0] to matches[*num_matches - 1]. The matches will be
   sorted by strictly increasing length and (non-strictly) increasing
   distance. */
static BROTLI_INLINE size_t FN(FindAllMatches)(
    HashToBinaryTree* BROTLI_RESTRICT self,
    const BrotliEncoderDictionary* dictionary,
    const uint8_t* BROTLI_RESTRICT data,
    const size_t ring_buffer_mask, const size_t cur_ix,
    const size_t max_length, const size_t max_backward,
    const size_t dictionary_distance, const BrotliEncoderParams* params,
    BackwardMatch* matches) {
  BackwardMatch* const orig_matches = matches;
  const size_t cur_ix_masked = cur_ix & ring_buffer_mask;
  size_t best_len = 1;
  const size_t short_match_max_backward =
      params->quality != HQ_ZOPFLIFICATION_QUALITY ? 16 : 64;
  size_t stop = cur_ix - short_match_max_backward;
  uint32_t dict_matches[BROTLI_MAX_STATIC_DICTIONARY_MATCH_LEN + 1];
  size_t i;
  if (cur_ix < short_match_max_backward) { stop = 0; }
  for (i = cur_ix - 1; i > stop && best_len <= 2; --i) {
    size_t prev_ix = i;
    const size_t backward = cur_ix - prev_ix;
    if (BROTLI_PREDICT_FALSE(backward > max_backward)) {
      break;
    }
    prev_ix &= ring_buffer_mask;
    if (data[cur_ix_masked] != data[prev_ix] ||
        data[cur_ix_masked + 1] != data[prev_ix + 1]) {
      continue;
    }
    {
      const size_t len =
          FindMatchLengthWithLimit(&data[prev_ix], &data[cur_ix_masked],
                                   max_length);
      if (len > best_len) {
        best_len = len;
        InitBackwardMatch(matches++, backward, len);
      }
    }
  }
  if (best_len < max_length) {
    matches = FN(StoreAndFindMatches)(self, data, cur_ix,
        ring_buffer_mask, max_length, max_backward, &best_len, matches);
  }
  for (i = 0; i <= BROTLI_MAX_STATIC_DICTIONARY_MATCH_LEN; ++i) {
    dict_matches[i] = kInvalidMatch;
  }
  {
    size_t minlen = BROTLI_MAX(size_t, 4, best_len + 1);
    if (BrotliFindAllStaticDictionaryMatches(dictionary,
        &data[cur_ix_masked], minlen, max_length, &dict_matches[0])) {
      size_t maxlen = BROTLI_MIN(
          size_t, BROTLI_MAX_STATIC_DICTIONARY_MATCH_LEN, max_length);
      size_t l;
      for (l = minlen; l <= maxlen; ++l) {
        uint32_t dict_id = dict_matches[l];
        if (dict_id < kInvalidMatch) {
          size_t distance = dictionary_distance + (dict_id >> 5) + 1;
          if (distance <= params->dist.max_distance) {
            InitDictionaryBackwardMatch(matches++, distance, l, dict_id & 31);
          }
        }
      }
    }
  }
  return (size_t)(matches - orig_matches);
}

/* Stores the hash of the next 4 bytes and re-roots the binary tree at the
   current sequence, without returning any matches.
   REQUIRES: ix + MAX_TREE_COMP_LENGTH <= end-of-current-block */
static BROTLI_INLINE void FN(Store)(HashToBinaryTree* BROTLI_RESTRICT self,
    const uint8_t* BROTLI_RESTRICT data,
    const size_t mask, const size_t ix) {
  /* Maximum distance is window size - 16, see section 9.1. of the spec. */
  const size_t max_backward = self->window_mask_ - BROTLI_WINDOW_GAP + 1;
  FN(StoreAndFindMatches)(self, data, ix, mask, MAX_TREE_COMP_LENGTH,
      max_backward, NULL, NULL);
}

static BROTLI_INLINE void FN(StoreRange)(HashToBinaryTree* BROTLI_RESTRICT self,
    const uint8_t* BROTLI_RESTRICT data, const size_t mask,
    const size_t ix_start, const size_t ix_end) {
  size_t i = ix_start;
  size_t j = ix_start;
  if (ix_start + 63 <= ix_end) {
    i = ix_end - 63;
  }
  if (ix_start + 512 <= i) {
    for (; j < i; j += 8) {
      FN(Store)(self, data, mask, j);
    }
  }
  for (; i < ix_end; ++i) {
    FN(Store)(self, data, mask, i);
  }
}

static BROTLI_INLINE void FN(StitchToPreviousBlock)(
    HashToBinaryTree* BROTLI_RESTRICT self,
    size_t num_bytes, size_t position, const uint8_t* ringbuffer,
    size_t ringbuffer_mask) {
  if (num_bytes >= FN(HashTypeLength)() - 1 &&
      position >= MAX_TREE_COMP_LENGTH) {
    /* Store the last `MAX_TREE_COMP_LENGTH - 1` positions in the hasher.
       These could not be calculated before, since they require knowledge
       of both the previous and the current block. */
    const size_t i_start = position - MAX_TREE_COMP_LENGTH + 1;
    const size_t i_end = BROTLI_MIN(size_t, position, i_start + num_bytes);
    size_t i;
    for (i = i_start; i < i_end; ++i) {
      /* Maximum distance is window size - 16, see section 9.1. of the spec.
         Furthermore, we have to make sure that we don't look further back
         from the start of the next block than the window size, otherwise we
         could access already overwritten areas of the ring-buffer. */
      const size_t max_backward =
          self->window_mask_ - BROTLI_MAX(size_t,
                                          BROTLI_WINDOW_GAP - 1,
                                          position - i);
      /* We know that i + MAX_TREE_COMP_LENGTH <= position + num_bytes, i.e. the
         end of the current block and that we have at least
         MAX_TREE_COMP_LENGTH tail in the ring-buffer. */
      FN(StoreAndFindMatches)(self, ringbuffer, i, ringbuffer_mask,
          MAX_TREE_COMP_LENGTH, max_backward, NULL, NULL);
    }
  }
}

#undef BUCKET_SIZE

#undef HashToBinaryTree
#undef MAX_TREE_SEARCH_DEPTH
#undef MAX_TREE_COMP_LENGTH
#undef BUCKET_BITS
#undef HASHER
/* MAX_NUM_MATCHES == 64 + MAX_TREE_SEARCH_DEPTH */
#define MAX_NUM_MATCHES_H10 128

/* For BUCKET_SWEEP_BITS == 0, enabling the dictionary lookup makes compression
   a little faster (0.5% - 1%) and it compresses 0.15% better on small text
   and HTML inputs. */

#define HASHER() H2
#define BUCKET_BITS 16
#define BUCKET_SWEEP_BITS 0
#define HASH_LEN 5
#define USE_DICTIONARY 1
/* NOLINT(build/header_guard) */
/* Copyright 2010 Google Inc. All Rights Reserved.

   Distributed under MIT license.
   See file LICENSE for detail or copy at https://opensource.org/licenses/MIT
*/

/* template parameters: FN, BUCKET_BITS, BUCKET_SWEEP_BITS, HASH_LEN,
                        USE_DICTIONARY
 */

#define HashLongestMatchQuickly HASHER()

#define BUCKET_SIZE (1 << BUCKET_BITS)
#define BUCKET_MASK (BUCKET_SIZE - 1)
#define BUCKET_SWEEP (1 << BUCKET_SWEEP_BITS)
#define BUCKET_SWEEP_MASK ((BUCKET_SWEEP - 1) << 3)

static BROTLI_INLINE size_t FN(HashTypeLength)(void) { return 8; }
static BROTLI_INLINE size_t FN(StoreLookahead)(void) { return 8; }

/* HashBytes is the function that chooses the bucket to place
   the address in. The HashLongestMatch and HashLongestMatchQuickly
   classes have separate, different implementations of hashing. */
static uint32_t FN(HashBytes)(const uint8_t* data) {
  const uint64_t h = ((BROTLI_UNALIGNED_LOAD64LE(data) << (64 - 8 * HASH_LEN)) *
                      kHashMul64);
  /* The higher bits contain more mixture from the multiplication,
     so we take our results from there. */
  return (uint32_t)(h >> (64 - BUCKET_BITS));
}

/* A (forgetful) hash table to the data seen by the compressor, to
   help create backward references to previous data.

   This is a hash map of fixed size (BUCKET_SIZE). */
typedef struct HashLongestMatchQuickly {
  /* Shortcuts. */
  HasherCommon* common;

  /* --- Dynamic size members --- */

  uint32_t* buckets_;  /* uint32_t[BUCKET_SIZE]; */
} HashLongestMatchQuickly;

static void FN(Initialize)(
    HasherCommon* common, HashLongestMatchQuickly* BROTLI_RESTRICT self,
    const BrotliEncoderParams* params) {
  self->common = common;

  BROTLI_UNUSED(params);
  self->buckets_ = (uint32_t*)common->extra[0];
}

static void FN(Prepare)(
    HashLongestMatchQuickly* BROTLI_RESTRICT self, BROTLI_BOOL one_shot,
    size_t input_size, const uint8_t* BROTLI_RESTRICT data) {
  uint32_t* BROTLI_RESTRICT buckets = self->buckets_;
  /* Partial preparation is 100 times slower (per socket). */
  size_t partial_prepare_threshold = BUCKET_SIZE >> 5;
  if (one_shot && input_size <= partial_prepare_threshold) {
    size_t i;
    for (i = 0; i < input_size; ++i) {
      const uint32_t key = FN(HashBytes)(&data[i]);
      if (BUCKET_SWEEP == 1) {
        buckets[key] = 0;
      } else {
        uint32_t j;
        for (j = 0; j < BUCKET_SWEEP; ++j) {
          buckets[(key + (j << 3)) & BUCKET_MASK] = 0;
        }
      }
    }
  } else {
    /* It is not strictly necessary to fill this buffer here, but
       not filling will make the results of the compression stochastic
       (but correct). This is because random data would cause the
       system to find accidentally good backward references here and there. */
    memset(buckets, 0, sizeof(uint32_t) * BUCKET_SIZE);
  }
}

static BROTLI_INLINE void FN(HashMemAllocInBytes)(
    const BrotliEncoderParams* params, BROTLI_BOOL one_shot,
    size_t input_size, size_t* alloc_size) {
  BROTLI_UNUSED(params);
  BROTLI_UNUSED(one_shot);
  BROTLI_UNUSED(input_size);
  alloc_size[0] = sizeof(uint32_t) * BUCKET_SIZE;
}

/* Look at 5 bytes at &data[ix & mask].
   Compute a hash from these, and store the value somewhere within
   [ix .. ix+3]. */
static BROTLI_INLINE void FN(Store)(
    HashLongestMatchQuickly* BROTLI_RESTRICT self,
    const uint8_t* BROTLI_RESTRICT data, const size_t mask, const size_t ix) {
  const uint32_t key = FN(HashBytes)(&data[ix & mask]);
  if (BUCKET_SWEEP == 1) {
    self->buckets_[key] = (uint32_t)ix;
  } else {
    /* Wiggle the value with the bucket sweep range. */
    const uint32_t off = ix & BUCKET_SWEEP_MASK;
    self->buckets_[(key + off) & BUCKET_MASK] = (uint32_t)ix;
  }
}

static BROTLI_INLINE void FN(StoreRange)(
    HashLongestMatchQuickly* BROTLI_RESTRICT self,
    const uint8_t* BROTLI_RESTRICT data, const size_t mask,
    const size_t ix_start, const size_t ix_end) {
  size_t i;
  for (i = ix_start; i < ix_end; ++i) {
    FN(Store)(self, data, mask, i);
  }
}

static BROTLI_INLINE void FN(StitchToPreviousBlock)(
    HashLongestMatchQuickly* BROTLI_RESTRICT self,
    size_t num_bytes, size_t position,
    const uint8_t* ringbuffer, size_t ringbuffer_mask) {
  if (num_bytes >= FN(HashTypeLength)() - 1 && position >= 3) {
    /* Prepare the hashes for three last bytes of the last write.
       These could not be calculated before, since they require knowledge
       of both the previous and the current block. */
    FN(Store)(self, ringbuffer, ringbuffer_mask, position - 3);
    FN(Store)(self, ringbuffer, ringbuffer_mask, position - 2);
    FN(Store)(self, ringbuffer, ringbuffer_mask, position - 1);
  }
}

static BROTLI_INLINE void FN(PrepareDistanceCache)(
    HashLongestMatchQuickly* BROTLI_RESTRICT self,
    int* BROTLI_RESTRICT distance_cache) {
  BROTLI_UNUSED(self);
  BROTLI_UNUSED(distance_cache);
}

/* Find a longest backward match of &data[cur_ix & ring_buffer_mask]
   up to the length of max_length and stores the position cur_ix in the
   hash table.

   Does not look for matches longer than max_length.
   Does not look for matches further away than max_backward.
   Writes the best match into |out|.
   |out|->score is updated only if a better match is found. */
static BROTLI_INLINE void FN(FindLongestMatch)(
    HashLongestMatchQuickly* BROTLI_RESTRICT self,
    const BrotliEncoderDictionary* dictionary,
    const uint8_t* BROTLI_RESTRICT data,
    const size_t ring_buffer_mask, const int* BROTLI_RESTRICT distance_cache,
    const size_t cur_ix, const size_t max_length, const size_t max_backward,
    const size_t dictionary_distance, const size_t max_distance,
    HasherSearchResult* BROTLI_RESTRICT out) {
  uint32_t* BROTLI_RESTRICT buckets = self->buckets_;
  const size_t best_len_in = out->len;
  const size_t cur_ix_masked = cur_ix & ring_buffer_mask;
  int compare_char = data[cur_ix_masked + best_len_in];
  size_t key = FN(HashBytes)(&data[cur_ix_masked]);
  size_t key_out;
  score_t min_score = out->score;
  score_t best_score = out->score;
  size_t best_len = best_len_in;
  size_t cached_backward = (size_t)distance_cache[0];
  size_t prev_ix = cur_ix - cached_backward;
  out->len_code_delta = 0;
  if (prev_ix < cur_ix) {
    prev_ix &= (uint32_t)ring_buffer_mask;
    if (compare_char == data[prev_ix + best_len]) {
      const size_t len = FindMatchLengthWithLimit(
          &data[prev_ix], &data[cur_ix_masked], max_length);
      if (len >= 4) {
        const score_t score = BackwardReferenceScoreUsingLastDistance(len);
        if (best_score < score) {
          out->len = len;
          out->distance = cached_backward;
          out->score = score;
          if (BUCKET_SWEEP == 1) {
            buckets[key] = (uint32_t)cur_ix;
            return;
          } else {
            best_len = len;
            best_score = score;
            compare_char = data[cur_ix_masked + len];
          }
        }
      }
    }
  }
  if (BUCKET_SWEEP == 1) {
    size_t backward;
    size_t len;
    /* Only one to look for, don't bother to prepare for a loop. */
    prev_ix = buckets[key];
    buckets[key] = (uint32_t)cur_ix;
    backward = cur_ix - prev_ix;
    prev_ix &= (uint32_t)ring_buffer_mask;
    if (compare_char != data[prev_ix + best_len_in]) {
      return;
    }
    if (BROTLI_PREDICT_FALSE(backward == 0 || backward > max_backward)) {
      return;
    }
    len = FindMatchLengthWithLimit(&data[prev_ix],
                                   &data[cur_ix_masked],
                                   max_length);
    if (len >= 4) {
      const score_t score = BackwardReferenceScore(len, backward);
      if (best_score < score) {
        out->len = len;
        out->distance = backward;
        out->score = score;
        return;
      }
    }
  } else {
    size_t keys[BUCKET_SWEEP];
    size_t i;
    for (i = 0; i < BUCKET_SWEEP; ++i) {
      keys[i] = (key + (i << 3)) & BUCKET_MASK;
    }
    key_out = keys[(cur_ix & BUCKET_SWEEP_MASK) >> 3];
    for (i = 0; i < BUCKET_SWEEP; ++i) {
      size_t len;
      size_t backward;
      prev_ix = buckets[keys[i]];
      backward = cur_ix - prev_ix;
      prev_ix &= (uint32_t)ring_buffer_mask;
      if (compare_char != data[prev_ix + best_len]) {
        continue;
      }
      if (BROTLI_PREDICT_FALSE(backward == 0 || backward > max_backward)) {
        continue;
      }
      len = FindMatchLengthWithLimit(&data[prev_ix],
                                     &data[cur_ix_masked],
                                     max_length);
      if (len >= 4) {
        const score_t score = BackwardReferenceScore(len, backward);
        if (best_score < score) {
          best_len = len;
          out->len = len;
          compare_char = data[cur_ix_masked + len];
          best_score = score;
          out->score = score;
          out->distance = backward;
        }
      }
    }
  }
  if (USE_DICTIONARY && min_score == out->score) {
    SearchInStaticDictionary(dictionary,
        self->common, &data[cur_ix_masked], max_length, dictionary_distance,
        max_distance, out, BROTLI_TRUE);
  }
  if (BUCKET_SWEEP != 1) {
    buckets[key_out] = (uint32_t)cur_ix;
  }
}

#undef BUCKET_SWEEP_MASK
#undef BUCKET_SWEEP
#undef BUCKET_MASK
#undef BUCKET_SIZE

#undef HashLongestMatchQuickly
#undef BUCKET_SWEEP_BITS
#undef USE_DICTIONARY
#undef HASHER

#define HASHER() H3
#define BUCKET_SWEEP_BITS 1
#define USE_DICTIONARY 0
/* NOLINT(build/header_guard) */
/* Copyright 2010 Google Inc. All Rights Reserved.

   Distributed under MIT license.
   See file LICENSE for detail or copy at https://opensource.org/licenses/MIT
*/

/* template parameters: FN, BUCKET_BITS, BUCKET_SWEEP_BITS, HASH_LEN,
                        USE_DICTIONARY
 */

#define HashLongestMatchQuickly HASHER()

#define BUCKET_SIZE (1 << BUCKET_BITS)
#define BUCKET_MASK (BUCKET_SIZE - 1)
#define BUCKET_SWEEP (1 << BUCKET_SWEEP_BITS)
#define BUCKET_SWEEP_MASK ((BUCKET_SWEEP - 1) << 3)

static BROTLI_INLINE size_t FN(HashTypeLength)(void) { return 8; }
static BROTLI_INLINE size_t FN(StoreLookahead)(void) { return 8; }

/* HashBytes is the function that chooses the bucket to place
   the address in. The HashLongestMatch and HashLongestMatchQuickly
   classes have separate, different implementations of hashing. */
static uint32_t FN(HashBytes)(const uint8_t* data) {
  const uint64_t h = ((BROTLI_UNALIGNED_LOAD64LE(data) << (64 - 8 * HASH_LEN)) *
                      kHashMul64);
  /* The higher bits contain more mixture from the multiplication,
     so we take our results from there. */
  return (uint32_t)(h >> (64 - BUCKET_BITS));
}

/* A (forgetful) hash table to the data seen by the compressor, to
   help create backward references to previous data.

   This is a hash map of fixed size (BUCKET_SIZE). */
typedef struct HashLongestMatchQuickly {
  /* Shortcuts. */
  HasherCommon* common;

  /* --- Dynamic size members --- */

  uint32_t* buckets_;  /* uint32_t[BUCKET_SIZE]; */
} HashLongestMatchQuickly;

static void FN(Initialize)(
    HasherCommon* common, HashLongestMatchQuickly* BROTLI_RESTRICT self,
    const BrotliEncoderParams* params) {
  self->common = common;

  BROTLI_UNUSED(params);
  self->buckets_ = (uint32_t*)common->extra[0];
}

static void FN(Prepare)(
    HashLongestMatchQuickly* BROTLI_RESTRICT self, BROTLI_BOOL one_shot,
    size_t input_size, const uint8_t* BROTLI_RESTRICT data) {
  uint32_t* BROTLI_RESTRICT buckets = self->buckets_;
  /* Partial preparation is 100 times slower (per socket). */
  size_t partial_prepare_threshold = BUCKET_SIZE >> 5;
  if (one_shot && input_size <= partial_prepare_threshold) {
    size_t i;
    for (i = 0; i < input_size; ++i) {
      const uint32_t key = FN(HashBytes)(&data[i]);
      if (BUCKET_SWEEP == 1) {
        buckets[key] = 0;
      } else {
        uint32_t j;
        for (j = 0; j < BUCKET_SWEEP; ++j) {
          buckets[(key + (j << 3)) & BUCKET_MASK] = 0;
        }
      }
    }
  } else {
    /* It is not strictly necessary to fill this buffer here, but
       not filling will make the results of the compression stochastic
       (but correct). This is because random data would cause the
       system to find accidentally good backward references here and there. */
    memset(buckets, 0, sizeof(uint32_t) * BUCKET_SIZE);
  }
}

static BROTLI_INLINE void FN(HashMemAllocInBytes)(
    const BrotliEncoderParams* params, BROTLI_BOOL one_shot,
    size_t input_size, size_t* alloc_size) {
  BROTLI_UNUSED(params);
  BROTLI_UNUSED(one_shot);
  BROTLI_UNUSED(input_size);
  alloc_size[0] = sizeof(uint32_t) * BUCKET_SIZE;
}

/* Look at 5 bytes at &data[ix & mask].
   Compute a hash from these, and store the value somewhere within
   [ix .. ix+3]. */
static BROTLI_INLINE void FN(Store)(
    HashLongestMatchQuickly* BROTLI_RESTRICT self,
    const uint8_t* BROTLI_RESTRICT data, const size_t mask, const size_t ix) {
  const uint32_t key = FN(HashBytes)(&data[ix & mask]);
  if (BUCKET_SWEEP == 1) {
    self->buckets_[key] = (uint32_t)ix;
  } else {
    /* Wiggle the value with the bucket sweep range. */
    const uint32_t off = ix & BUCKET_SWEEP_MASK;
    self->buckets_[(key + off) & BUCKET_MASK] = (uint32_t)ix;
  }
}

static BROTLI_INLINE void FN(StoreRange)(
    HashLongestMatchQuickly* BROTLI_RESTRICT self,
    const uint8_t* BROTLI_RESTRICT data, const size_t mask,
    const size_t ix_start, const size_t ix_end) {
  size_t i;
  for (i = ix_start; i < ix_end; ++i) {
    FN(Store)(self, data, mask, i);
  }
}

static BROTLI_INLINE void FN(StitchToPreviousBlock)(
    HashLongestMatchQuickly* BROTLI_RESTRICT self,
    size_t num_bytes, size_t position,
    const uint8_t* ringbuffer, size_t ringbuffer_mask) {
  if (num_bytes >= FN(HashTypeLength)() - 1 && position >= 3) {
    /* Prepare the hashes for three last bytes of the last write.
       These could not be calculated before, since they require knowledge
       of both the previous and the current block. */
    FN(Store)(self, ringbuffer, ringbuffer_mask, position - 3);
    FN(Store)(self, ringbuffer, ringbuffer_mask, position - 2);
    FN(Store)(self, ringbuffer, ringbuffer_mask, position - 1);
  }
}

static BROTLI_INLINE void FN(PrepareDistanceCache)(
    HashLongestMatchQuickly* BROTLI_RESTRICT self,
    int* BROTLI_RESTRICT distance_cache) {
  BROTLI_UNUSED(self);
  BROTLI_UNUSED(distance_cache);
}

/* Find a longest backward match of &data[cur_ix & ring_buffer_mask]
   up to the length of max_length and stores the position cur_ix in the
   hash table.

   Does not look for matches longer than max_length.
   Does not look for matches further away than max_backward.
   Writes the best match into |out|.
   |out|->score is updated only if a better match is found. */
static BROTLI_INLINE void FN(FindLongestMatch)(
    HashLongestMatchQuickly* BROTLI_RESTRICT self,
    const BrotliEncoderDictionary* dictionary,
    const uint8_t* BROTLI_RESTRICT data,
    const size_t ring_buffer_mask, const int* BROTLI_RESTRICT distance_cache,
    const size_t cur_ix, const size_t max_length, const size_t max_backward,
    const size_t dictionary_distance, const size_t max_distance,
    HasherSearchResult* BROTLI_RESTRICT out) {
  uint32_t* BROTLI_RESTRICT buckets = self->buckets_;
  const size_t best_len_in = out->len;
  const size_t cur_ix_masked = cur_ix & ring_buffer_mask;
  int compare_char = data[cur_ix_masked + best_len_in];
  size_t key = FN(HashBytes)(&data[cur_ix_masked]);
  size_t key_out;
  score_t min_score = out->score;
  score_t best_score = out->score;
  size_t best_len = best_len_in;
  size_t cached_backward = (size_t)distance_cache[0];
  size_t prev_ix = cur_ix - cached_backward;
  out->len_code_delta = 0;
  if (prev_ix < cur_ix) {
    prev_ix &= (uint32_t)ring_buffer_mask;
    if (compare_char == data[prev_ix + best_len]) {
      const size_t len = FindMatchLengthWithLimit(
          &data[prev_ix], &data[cur_ix_masked], max_length);
      if (len >= 4) {
        const score_t score = BackwardReferenceScoreUsingLastDistance(len);
        if (best_score < score) {
          out->len = len;
          out->distance = cached_backward;
          out->score = score;
          if (BUCKET_SWEEP == 1) {
            buckets[key] = (uint32_t)cur_ix;
            return;
          } else {
            best_len = len;
            best_score = score;
            compare_char = data[cur_ix_masked + len];
          }
        }
      }
    }
  }
  if (BUCKET_SWEEP == 1) {
    size_t backward;
    size_t len;
    /* Only one to look for, don't bother to prepare for a loop. */
    prev_ix = buckets[key];
    buckets[key] = (uint32_t)cur_ix;
    backward = cur_ix - prev_ix;
    prev_ix &= (uint32_t)ring_buffer_mask;
    if (compare_char != data[prev_ix + best_len_in]) {
      return;
    }
    if (BROTLI_PREDICT_FALSE(backward == 0 || backward > max_backward)) {
      return;
    }
    len = FindMatchLengthWithLimit(&data[prev_ix],
                                   &data[cur_ix_masked],
                                   max_length);
    if (len >= 4) {
      const score_t score = BackwardReferenceScore(len, backward);
      if (best_score < score) {
        out->len = len;
        out->distance = backward;
        out->score = score;
        return;
      }
    }
  } else {
    size_t keys[BUCKET_SWEEP];
    size_t i;
    for (i = 0; i < BUCKET_SWEEP; ++i) {
      keys[i] = (key + (i << 3)) & BUCKET_MASK;
    }
    key_out = keys[(cur_ix & BUCKET_SWEEP_MASK) >> 3];
    for (i = 0; i < BUCKET_SWEEP; ++i) {
      size_t len;
      size_t backward;
      prev_ix = buckets[keys[i]];
      backward = cur_ix - prev_ix;
      prev_ix &= (uint32_t)ring_buffer_mask;
      if (compare_char != data[prev_ix + best_len]) {
        continue;
      }
      if (BROTLI_PREDICT_FALSE(backward == 0 || backward > max_backward)) {
        continue;
      }
      len = FindMatchLengthWithLimit(&data[prev_ix],
                                     &data[cur_ix_masked],
                                     max_length);
      if (len >= 4) {
        const score_t score = BackwardReferenceScore(len, backward);
        if (best_score < score) {
          best_len = len;
          out->len = len;
          compare_char = data[cur_ix_masked + len];
          best_score = score;
          out->score = score;
          out->distance = backward;
        }
      }
    }
  }
  if (USE_DICTIONARY && min_score == out->score) {
    SearchInStaticDictionary(dictionary,
        self->common, &data[cur_ix_masked], max_length, dictionary_distance,
        max_distance, out, BROTLI_TRUE);
  }
  if (BUCKET_SWEEP != 1) {
    buckets[key_out] = (uint32_t)cur_ix;
  }
}

#undef BUCKET_SWEEP_MASK
#undef BUCKET_SWEEP
#undef BUCKET_MASK
#undef BUCKET_SIZE

#undef HashLongestMatchQuickly
#undef USE_DICTIONARY
#undef BUCKET_SWEEP_BITS
#undef BUCKET_BITS
#undef HASHER

#define HASHER() H4
#define BUCKET_BITS 17
#define BUCKET_SWEEP_BITS 2
#define USE_DICTIONARY 1
/* NOLINT(build/header_guard) */
/* Copyright 2010 Google Inc. All Rights Reserved.

   Distributed under MIT license.
   See file LICENSE for detail or copy at https://opensource.org/licenses/MIT
*/

/* template parameters: FN, BUCKET_BITS, BUCKET_SWEEP_BITS, HASH_LEN,
                        USE_DICTIONARY
 */

#define HashLongestMatchQuickly HASHER()

#define BUCKET_SIZE (1 << BUCKET_BITS)
#define BUCKET_MASK (BUCKET_SIZE - 1)
#define BUCKET_SWEEP (1 << BUCKET_SWEEP_BITS)
#define BUCKET_SWEEP_MASK ((BUCKET_SWEEP - 1) << 3)

static BROTLI_INLINE size_t FN(HashTypeLength)(void) { return 8; }
static BROTLI_INLINE size_t FN(StoreLookahead)(void) { return 8; }

/* HashBytes is the function that chooses the bucket to place
   the address in. The HashLongestMatch and HashLongestMatchQuickly
   classes have separate, different implementations of hashing. */
static uint32_t FN(HashBytes)(const uint8_t* data) {
  const uint64_t h = ((BROTLI_UNALIGNED_LOAD64LE(data) << (64 - 8 * HASH_LEN)) *
                      kHashMul64);
  /* The higher bits contain more mixture from the multiplication,
     so we take our results from there. */
  return (uint32_t)(h >> (64 - BUCKET_BITS));
}

/* A (forgetful) hash table to the data seen by the compressor, to
   help create backward references to previous data.

   This is a hash map of fixed size (BUCKET_SIZE). */
typedef struct HashLongestMatchQuickly {
  /* Shortcuts. */
  HasherCommon* common;

  /* --- Dynamic size members --- */

  uint32_t* buckets_;  /* uint32_t[BUCKET_SIZE]; */
} HashLongestMatchQuickly;

static void FN(Initialize)(
    HasherCommon* common, HashLongestMatchQuickly* BROTLI_RESTRICT self,
    const BrotliEncoderParams* params) {
  self->common = common;

  BROTLI_UNUSED(params);
  self->buckets_ = (uint32_t*)common->extra[0];
}

static void FN(Prepare)(
    HashLongestMatchQuickly* BROTLI_RESTRICT self, BROTLI_BOOL one_shot,
    size_t input_size, const uint8_t* BROTLI_RESTRICT data) {
  uint32_t* BROTLI_RESTRICT buckets = self->buckets_;
  /* Partial preparation is 100 times slower (per socket). */
  size_t partial_prepare_threshold = BUCKET_SIZE >> 5;
  if (one_shot && input_size <= partial_prepare_threshold) {
    size_t i;
    for (i = 0; i < input_size; ++i) {
      const uint32_t key = FN(HashBytes)(&data[i]);
      if (BUCKET_SWEEP == 1) {
        buckets[key] = 0;
      } else {
        uint32_t j;
        for (j = 0; j < BUCKET_SWEEP; ++j) {
          buckets[(key + (j << 3)) & BUCKET_MASK] = 0;
        }
      }
    }
  } else {
    /* It is not strictly necessary to fill this buffer here, but
       not filling will make the results of the compression stochastic
       (but correct). This is because random data would cause the
       system to find accidentally good backward references here and there. */
    memset(buckets, 0, sizeof(uint32_t) * BUCKET_SIZE);
  }
}

static BROTLI_INLINE void FN(HashMemAllocInBytes)(
    const BrotliEncoderParams* params, BROTLI_BOOL one_shot,
    size_t input_size, size_t* alloc_size) {
  BROTLI_UNUSED(params);
  BROTLI_UNUSED(one_shot);
  BROTLI_UNUSED(input_size);
  alloc_size[0] = sizeof(uint32_t) * BUCKET_SIZE;
}

/* Look at 5 bytes at &data[ix & mask].
   Compute a hash from these, and store the value somewhere within
   [ix .. ix+3]. */
static BROTLI_INLINE void FN(Store)(
    HashLongestMatchQuickly* BROTLI_RESTRICT self,
    const uint8_t* BROTLI_RESTRICT data, const size_t mask, const size_t ix) {
  const uint32_t key = FN(HashBytes)(&data[ix & mask]);
  if (BUCKET_SWEEP == 1) {
    self->buckets_[key] = (uint32_t)ix;
  } else {
    /* Wiggle the value with the bucket sweep range. */
    const uint32_t off = ix & BUCKET_SWEEP_MASK;
    self->buckets_[(key + off) & BUCKET_MASK] = (uint32_t)ix;
  }
}

static BROTLI_INLINE void FN(StoreRange)(
    HashLongestMatchQuickly* BROTLI_RESTRICT self,
    const uint8_t* BROTLI_RESTRICT data, const size_t mask,
    const size_t ix_start, const size_t ix_end) {
  size_t i;
  for (i = ix_start; i < ix_end; ++i) {
    FN(Store)(self, data, mask, i);
  }
}

static BROTLI_INLINE void FN(StitchToPreviousBlock)(
    HashLongestMatchQuickly* BROTLI_RESTRICT self,
    size_t num_bytes, size_t position,
    const uint8_t* ringbuffer, size_t ringbuffer_mask) {
  if (num_bytes >= FN(HashTypeLength)() - 1 && position >= 3) {
    /* Prepare the hashes for three last bytes of the last write.
       These could not be calculated before, since they require knowledge
       of both the previous and the current block. */
    FN(Store)(self, ringbuffer, ringbuffer_mask, position - 3);
    FN(Store)(self, ringbuffer, ringbuffer_mask, position - 2);
    FN(Store)(self, ringbuffer, ringbuffer_mask, position - 1);
  }
}

static BROTLI_INLINE void FN(PrepareDistanceCache)(
    HashLongestMatchQuickly* BROTLI_RESTRICT self,
    int* BROTLI_RESTRICT distance_cache) {
  BROTLI_UNUSED(self);
  BROTLI_UNUSED(distance_cache);
}

/* Find a longest backward match of &data[cur_ix & ring_buffer_mask]
   up to the length of max_length and stores the position cur_ix in the
   hash table.

   Does not look for matches longer than max_length.
   Does not look for matches further away than max_backward.
   Writes the best match into |out|.
   |out|->score is updated only if a better match is found. */
static BROTLI_INLINE void FN(FindLongestMatch)(
    HashLongestMatchQuickly* BROTLI_RESTRICT self,
    const BrotliEncoderDictionary* dictionary,
    const uint8_t* BROTLI_RESTRICT data,
    const size_t ring_buffer_mask, const int* BROTLI_RESTRICT distance_cache,
    const size_t cur_ix, const size_t max_length, const size_t max_backward,
    const size_t dictionary_distance, const size_t max_distance,
    HasherSearchResult* BROTLI_RESTRICT out) {
  uint32_t* BROTLI_RESTRICT buckets = self->buckets_;
  const size_t best_len_in = out->len;
  const size_t cur_ix_masked = cur_ix & ring_buffer_mask;
  int compare_char = data[cur_ix_masked + best_len_in];
  size_t key = FN(HashBytes)(&data[cur_ix_masked]);
  size_t key_out;
  score_t min_score = out->score;
  score_t best_score = out->score;
  size_t best_len = best_len_in;
  size_t cached_backward = (size_t)distance_cache[0];
  size_t prev_ix = cur_ix - cached_backward;
  out->len_code_delta = 0;
  if (prev_ix < cur_ix) {
    prev_ix &= (uint32_t)ring_buffer_mask;
    if (compare_char == data[prev_ix + best_len]) {
      const size_t len = FindMatchLengthWithLimit(
          &data[prev_ix], &data[cur_ix_masked], max_length);
      if (len >= 4) {
        const score_t score = BackwardReferenceScoreUsingLastDistance(len);
        if (best_score < score) {
          out->len = len;
          out->distance = cached_backward;
          out->score = score;
          if (BUCKET_SWEEP == 1) {
            buckets[key] = (uint32_t)cur_ix;
            return;
          } else {
            best_len = len;
            best_score = score;
            compare_char = data[cur_ix_masked + len];
          }
        }
      }
    }
  }
  if (BUCKET_SWEEP == 1) {
    size_t backward;
    size_t len;
    /* Only one to look for, don't bother to prepare for a loop. */
    prev_ix = buckets[key];
    buckets[key] = (uint32_t)cur_ix;
    backward = cur_ix - prev_ix;
    prev_ix &= (uint32_t)ring_buffer_mask;
    if (compare_char != data[prev_ix + best_len_in]) {
      return;
    }
    if (BROTLI_PREDICT_FALSE(backward == 0 || backward > max_backward)) {
      return;
    }
    len = FindMatchLengthWithLimit(&data[prev_ix],
                                   &data[cur_ix_masked],
                                   max_length);
    if (len >= 4) {
      const score_t score = BackwardReferenceScore(len, backward);
      if (best_score < score) {
        out->len = len;
        out->distance = backward;
        out->score = score;
        return;
      }
    }
  } else {
    size_t keys[BUCKET_SWEEP];
    size_t i;
    for (i = 0; i < BUCKET_SWEEP; ++i) {
      keys[i] = (key + (i << 3)) & BUCKET_MASK;
    }
    key_out = keys[(cur_ix & BUCKET_SWEEP_MASK) >> 3];
    for (i = 0; i < BUCKET_SWEEP; ++i) {
      size_t len;
      size_t backward;
      prev_ix = buckets[keys[i]];
      backward = cur_ix - prev_ix;
      prev_ix &= (uint32_t)ring_buffer_mask;
      if (compare_char != data[prev_ix + best_len]) {
        continue;
      }
      if (BROTLI_PREDICT_FALSE(backward == 0 || backward > max_backward)) {
        continue;
      }
      len = FindMatchLengthWithLimit(&data[prev_ix],
                                     &data[cur_ix_masked],
                                     max_length);
      if (len >= 4) {
        const score_t score = BackwardReferenceScore(len, backward);
        if (best_score < score) {
          best_len = len;
          out->len = len;
          compare_char = data[cur_ix_masked + len];
          best_score = score;
          out->score = score;
          out->distance = backward;
        }
      }
    }
  }
  if (USE_DICTIONARY && min_score == out->score) {
    SearchInStaticDictionary(dictionary,
        self->common, &data[cur_ix_masked], max_length, dictionary_distance,
        max_distance, out, BROTLI_TRUE);
  }
  if (BUCKET_SWEEP != 1) {
    buckets[key_out] = (uint32_t)cur_ix;
  }
}

#undef BUCKET_SWEEP_MASK
#undef BUCKET_SWEEP
#undef BUCKET_MASK
#undef BUCKET_SIZE

#undef HashLongestMatchQuickly
#undef USE_DICTIONARY
#undef HASH_LEN
#undef BUCKET_SWEEP_BITS
#undef BUCKET_BITS
#undef HASHER

#define HASHER() H5
/* NOLINT(build/header_guard) */
/* Copyright 2010 Google Inc. All Rights Reserved.

   Distributed under MIT license.
   See file LICENSE for detail or copy at https://opensource.org/licenses/MIT
*/

/* template parameters: FN */

/* A (forgetful) hash table to the data seen by the compressor, to
   help create backward references to previous data.

   This is a hash map of fixed size (bucket_size_) to a ring buffer of
   fixed size (block_size_). The ring buffer contains the last block_size_
   index positions of the given hash key in the compressed data. */

#define HashLongestMatch HASHER()

static BROTLI_INLINE size_t FN(HashTypeLength)(void) { return 4; }
static BROTLI_INLINE size_t FN(StoreLookahead)(void) { return 4; }

/* HashBytes is the function that chooses the bucket to place the address in. */
static uint32_t FN(HashBytes)(
    const uint8_t* BROTLI_RESTRICT data, const int shift) {
  uint32_t h = BROTLI_UNALIGNED_LOAD32LE(data) * kHashMul32;
  /* The higher bits contain more mixture from the multiplication,
     so we take our results from there. */
  return (uint32_t)(h >> shift);
}

typedef struct HashLongestMatch {
  /* Number of hash buckets. */
  size_t bucket_size_;
  /* Only block_size_ newest backward references are kept,
     and the older are forgotten. */
  size_t block_size_;
  /* Left-shift for computing hash bucket index from hash value. */
  int hash_shift_;
  /* Mask for accessing entries in a block (in a ring-buffer manner). */
  uint32_t block_mask_;

  int block_bits_;
  int num_last_distances_to_check_;

  /* Shortcuts. */
  HasherCommon* common_;

  /* --- Dynamic size members --- */

  /* Number of entries in a particular bucket. */
  uint16_t* num_;  /* uint16_t[bucket_size]; */

  /* Buckets containing block_size_ of backward references. */
  uint32_t* buckets_;  /* uint32_t[bucket_size * block_size]; */
} HashLongestMatch;

static void FN(Initialize)(
    HasherCommon* common, HashLongestMatch* BROTLI_RESTRICT self,
    const BrotliEncoderParams* params) {
  self->common_ = common;

  BROTLI_UNUSED(params);
  self->hash_shift_ = 32 - common->params.bucket_bits;
  self->bucket_size_ = (size_t)1 << common->params.bucket_bits;
  self->block_size_ = (size_t)1 << common->params.block_bits;
  self->block_mask_ = (uint32_t)(self->block_size_ - 1);
  self->num_ = (uint16_t*)common->extra[0];
  self->buckets_ = (uint32_t*)common->extra[1];
  self->block_bits_ = common->params.block_bits;
  self->num_last_distances_to_check_ =
      common->params.num_last_distances_to_check;
}

static void FN(Prepare)(
    HashLongestMatch* BROTLI_RESTRICT self, BROTLI_BOOL one_shot,
    size_t input_size, const uint8_t* BROTLI_RESTRICT data) {
  uint16_t* BROTLI_RESTRICT num = self->num_;
  /* Partial preparation is 100 times slower (per socket). */
  size_t partial_prepare_threshold = self->bucket_size_ >> 6;
  if (one_shot && input_size <= partial_prepare_threshold) {
    size_t i;
    for (i = 0; i < input_size; ++i) {
      const uint32_t key = FN(HashBytes)(&data[i], self->hash_shift_);
      num[key] = 0;
    }
  } else {
    memset(num, 0, self->bucket_size_ * sizeof(num[0]));
  }
}

static BROTLI_INLINE void FN(HashMemAllocInBytes)(
    const BrotliEncoderParams* params, BROTLI_BOOL one_shot,
    size_t input_size, size_t* alloc_size) {
  size_t bucket_size = (size_t)1 << params->hasher.bucket_bits;
  size_t block_size = (size_t)1 << params->hasher.block_bits;
  BROTLI_UNUSED(one_shot);
  BROTLI_UNUSED(input_size);
  alloc_size[0] = sizeof(uint16_t) * bucket_size;
  alloc_size[1] = sizeof(uint32_t) * bucket_size * block_size;
}

/* Look at 4 bytes at &data[ix & mask].
   Compute a hash from these, and store the value of ix at that position. */
static BROTLI_INLINE void FN(Store)(
    HashLongestMatch* BROTLI_RESTRICT self, const uint8_t* BROTLI_RESTRICT data,
    const size_t mask, const size_t ix) {
  const uint32_t key = FN(HashBytes)(&data[ix & mask], self->hash_shift_);
  const size_t minor_ix = self->num_[key] & self->block_mask_;
  const size_t offset = minor_ix + (key << self->block_bits_);
  self->buckets_[offset] = (uint32_t)ix;
  ++self->num_[key];
}

static BROTLI_INLINE void FN(StoreRange)(HashLongestMatch* BROTLI_RESTRICT self,
    const uint8_t* BROTLI_RESTRICT data, const size_t mask,
    const size_t ix_start, const size_t ix_end) {
  size_t i;
  for (i = ix_start; i < ix_end; ++i) {
    FN(Store)(self, data, mask, i);
  }
}

static BROTLI_INLINE void FN(StitchToPreviousBlock)(
    HashLongestMatch* BROTLI_RESTRICT self,
    size_t num_bytes, size_t position, const uint8_t* ringbuffer,
    size_t ringbuffer_mask) {
  if (num_bytes >= FN(HashTypeLength)() - 1 && position >= 3) {
    /* Prepare the hashes for three last bytes of the last write.
       These could not be calculated before, since they require knowledge
       of both the previous and the current block. */
    FN(Store)(self, ringbuffer, ringbuffer_mask, position - 3);
    FN(Store)(self, ringbuffer, ringbuffer_mask, position - 2);
    FN(Store)(self, ringbuffer, ringbuffer_mask, position - 1);
  }
}

static BROTLI_INLINE void FN(PrepareDistanceCache)(
    HashLongestMatch* BROTLI_RESTRICT self,
    int* BROTLI_RESTRICT distance_cache) {
  PrepareDistanceCache(distance_cache, self->num_last_distances_to_check_);
}

/* Find a longest backward match of &data[cur_ix] up to the length of
   max_length and stores the position cur_ix in the hash table.

   REQUIRES: FN(PrepareDistanceCache) must be invoked for current distance cache
             values; if this method is invoked repeatedly with the same distance
             cache values, it is enough to invoke FN(PrepareDistanceCache) once.

   Does not look for matches longer than max_length.
   Does not look for matches further away than max_backward.
   Writes the best match into |out|.
   |out|->score is updated only if a better match is found. */
static BROTLI_INLINE void FN(FindLongestMatch)(
    HashLongestMatch* BROTLI_RESTRICT self,
    const BrotliEncoderDictionary* dictionary,
    const uint8_t* BROTLI_RESTRICT data, const size_t ring_buffer_mask,
    const int* BROTLI_RESTRICT distance_cache, const size_t cur_ix,
    const size_t max_length, const size_t max_backward,
    const size_t dictionary_distance, const size_t max_distance,
    HasherSearchResult* BROTLI_RESTRICT out) {
  uint16_t* BROTLI_RESTRICT num = self->num_;
  uint32_t* BROTLI_RESTRICT buckets = self->buckets_;
  const size_t cur_ix_masked = cur_ix & ring_buffer_mask;
  /* Don't accept a short copy from far away. */
  score_t min_score = out->score;
  score_t best_score = out->score;
  size_t best_len = out->len;
  size_t i;
  out->len = 0;
  out->len_code_delta = 0;
  /* Try last distance first. */
  for (i = 0; i < (size_t)self->num_last_distances_to_check_; ++i) {
    const size_t backward = (size_t)distance_cache[i];
    size_t prev_ix = (size_t)(cur_ix - backward);
    if (prev_ix >= cur_ix) {
      continue;
    }
    if (BROTLI_PREDICT_FALSE(backward > max_backward)) {
      continue;
    }
    prev_ix &= ring_buffer_mask;

    if (cur_ix_masked + best_len > ring_buffer_mask ||
        prev_ix + best_len > ring_buffer_mask ||
        data[cur_ix_masked + best_len] != data[prev_ix + best_len]) {
      continue;
    }
    {
      const size_t len = FindMatchLengthWithLimit(&data[prev_ix],
                                                  &data[cur_ix_masked],
                                                  max_length);
      if (len >= 3 || (len == 2 && i < 2)) {
        /* Comparing for >= 2 does not change the semantics, but just saves for
           a few unnecessary binary logarithms in backward reference score,
           since we are not interested in such short matches. */
        score_t score = BackwardReferenceScoreUsingLastDistance(len);
        if (best_score < score) {
          if (i != 0) score -= BackwardReferencePenaltyUsingLastDistance(i);
          if (best_score < score) {
            best_score = score;
            best_len = len;
            out->len = best_len;
            out->distance = backward;
            out->score = best_score;
          }
        }
      }
    }
  }
  {
    const uint32_t key =
        FN(HashBytes)(&data[cur_ix_masked], self->hash_shift_);
    uint32_t* BROTLI_RESTRICT bucket = &buckets[key << self->block_bits_];
    const size_t down =
        (num[key] > self->block_size_) ? (num[key] - self->block_size_) : 0u;
    for (i = num[key]; i > down;) {
      size_t prev_ix = bucket[--i & self->block_mask_];
      const size_t backward = cur_ix - prev_ix;
      if (BROTLI_PREDICT_FALSE(backward > max_backward)) {
        break;
      }
      prev_ix &= ring_buffer_mask;
      if (cur_ix_masked + best_len > ring_buffer_mask ||
          prev_ix + best_len > ring_buffer_mask ||
          data[cur_ix_masked + best_len] != data[prev_ix + best_len]) {
        continue;
      }
      {
        const size_t len = FindMatchLengthWithLimit(&data[prev_ix],
                                                    &data[cur_ix_masked],
                                                    max_length);
        if (len >= 4) {
          /* Comparing for >= 3 does not change the semantics, but just saves
             for a few unnecessary binary logarithms in backward reference
             score, since we are not interested in such short matches. */
          score_t score = BackwardReferenceScore(len, backward);
          if (best_score < score) {
            best_score = score;
            best_len = len;
            out->len = best_len;
            out->distance = backward;
            out->score = best_score;
          }
        }
      }
    }
    bucket[num[key] & self->block_mask_] = (uint32_t)cur_ix;
    ++num[key];
  }
  if (min_score == out->score) {
    SearchInStaticDictionary(dictionary,
        self->common_, &data[cur_ix_masked], max_length, dictionary_distance,
        max_distance, out, BROTLI_FALSE);
  }
}

#undef HashLongestMatch
#undef HASHER

#define HASHER() H6
/* NOLINT(build/header_guard) */
/* Copyright 2010 Google Inc. All Rights Reserved.

   Distributed under MIT license.
   See file LICENSE for detail or copy at https://opensource.org/licenses/MIT
*/

/* template parameters: FN */

/* A (forgetful) hash table to the data seen by the compressor, to
   help create backward references to previous data.

   This is a hash map of fixed size (bucket_size_) to a ring buffer of
   fixed size (block_size_). The ring buffer contains the last block_size_
   index positions of the given hash key in the compressed data. */

#define HashLongestMatch HASHER()

static BROTLI_INLINE size_t FN(HashTypeLength)(void) { return 8; }
static BROTLI_INLINE size_t FN(StoreLookahead)(void) { return 8; }

/* HashBytes is the function that chooses the bucket to place the address in. */
static BROTLI_INLINE size_t FN(HashBytes)(const uint8_t* BROTLI_RESTRICT data,
                                          uint64_t hash_mul) {
  const uint64_t h = BROTLI_UNALIGNED_LOAD64LE(data) * hash_mul;
  /* The higher bits contain more mixture from the multiplication,
     so we take our results from there. */
  return (size_t)(h >> (64 - 15));
}

typedef struct HashLongestMatch {
  /* Number of hash buckets. */
  size_t bucket_size_;
  /* Only block_size_ newest backward references are kept,
     and the older are forgotten. */
  size_t block_size_;
  /* Hash multiplier tuned to match length. */
  uint64_t hash_mul_;
  /* Mask for accessing entries in a block (in a ring-buffer manner). */
  uint32_t block_mask_;

  int block_bits_;
  int num_last_distances_to_check_;

  /* Shortcuts. */
  HasherCommon* common_;

  /* --- Dynamic size members --- */

  /* Number of entries in a particular bucket. */
  uint16_t* num_;  /* uint16_t[bucket_size]; */

  /* Buckets containing block_size_ of backward references. */
  uint32_t* buckets_;  /* uint32_t[bucket_size * block_size]; */
} HashLongestMatch;

static void FN(Initialize)(
    HasherCommon* common, HashLongestMatch* BROTLI_RESTRICT self,
    const BrotliEncoderParams* params) {
  self->common_ = common;

  BROTLI_UNUSED(params);
  self->hash_mul_ = kHashMul64 << (64 - 5 * 8);
  BROTLI_DCHECK(common->params.bucket_bits == 15);
  self->bucket_size_ = (size_t)1 << common->params.bucket_bits;
  self->block_bits_ = common->params.block_bits;
  self->block_size_ = (size_t)1 << common->params.block_bits;
  self->block_mask_ = (uint32_t)(self->block_size_ - 1);
  self->num_last_distances_to_check_ =
      common->params.num_last_distances_to_check;
  self->num_ = (uint16_t*)common->extra[0];
  self->buckets_ = (uint32_t*)common->extra[1];
}

static void FN(Prepare)(
    HashLongestMatch* BROTLI_RESTRICT self, BROTLI_BOOL one_shot,
    size_t input_size, const uint8_t* BROTLI_RESTRICT data) {
  uint16_t* BROTLI_RESTRICT num = self->num_;
  /* Partial preparation is 100 times slower (per socket). */
  size_t partial_prepare_threshold = self->bucket_size_ >> 6;
  if (one_shot && input_size <= partial_prepare_threshold) {
    size_t i;
    for (i = 0; i < input_size; ++i) {
      const size_t key = FN(HashBytes)(&data[i], self->hash_mul_);
      num[key] = 0;
    }
  } else {
    memset(num, 0, self->bucket_size_ * sizeof(num[0]));
  }
}

static BROTLI_INLINE void FN(HashMemAllocInBytes)(
    const BrotliEncoderParams* params, BROTLI_BOOL one_shot,
    size_t input_size, size_t* alloc_size) {
  size_t bucket_size = (size_t)1 << params->hasher.bucket_bits;
  size_t block_size = (size_t)1 << params->hasher.block_bits;
  BROTLI_UNUSED(one_shot);
  BROTLI_UNUSED(input_size);
  alloc_size[0] = sizeof(uint16_t) * bucket_size;
  alloc_size[1] = sizeof(uint32_t) * bucket_size * block_size;
}

/* Look at 4 bytes at &data[ix & mask].
   Compute a hash from these, and store the value of ix at that position. */
static BROTLI_INLINE void FN(Store)(
    HashLongestMatch* BROTLI_RESTRICT self, const uint8_t* BROTLI_RESTRICT data,
    const size_t mask, const size_t ix) {
  uint16_t* BROTLI_RESTRICT num = self->num_;
  uint32_t* BROTLI_RESTRICT buckets = self->buckets_;
  const size_t key = FN(HashBytes)(&data[ix & mask], self->hash_mul_);
  const size_t minor_ix = num[key] & self->block_mask_;
  const size_t offset = minor_ix + (key << self->block_bits_);
  ++num[key];
  buckets[offset] = (uint32_t)ix;
}

static BROTLI_INLINE void FN(StoreRange)(HashLongestMatch* BROTLI_RESTRICT self,
    const uint8_t* BROTLI_RESTRICT data, const size_t mask,
    const size_t ix_start, const size_t ix_end) {
  size_t i;
  for (i = ix_start; i < ix_end; ++i) {
    FN(Store)(self, data, mask, i);
  }
}

static BROTLI_INLINE void FN(StitchToPreviousBlock)(
    HashLongestMatch* BROTLI_RESTRICT self,
    size_t num_bytes, size_t position, const uint8_t* ringbuffer,
    size_t ringbuffer_mask) {
  if (num_bytes >= FN(HashTypeLength)() - 1 && position >= 3) {
    /* Prepare the hashes for three last bytes of the last write.
       These could not be calculated before, since they require knowledge
       of both the previous and the current block. */
    FN(Store)(self, ringbuffer, ringbuffer_mask, position - 3);
    FN(Store)(self, ringbuffer, ringbuffer_mask, position - 2);
    FN(Store)(self, ringbuffer, ringbuffer_mask, position - 1);
  }
}

static BROTLI_INLINE void FN(PrepareDistanceCache)(
    HashLongestMatch* BROTLI_RESTRICT self,
    int* BROTLI_RESTRICT distance_cache) {
  PrepareDistanceCache(distance_cache, self->num_last_distances_to_check_);
}

/* Find a longest backward match of &data[cur_ix] up to the length of
   max_length and stores the position cur_ix in the hash table.

   REQUIRES: FN(PrepareDistanceCache) must be invoked for current distance cache
             values; if this method is invoked repeatedly with the same distance
             cache values, it is enough to invoke FN(PrepareDistanceCache) once.

   Does not look for matches longer than max_length.
   Does not look for matches further away than max_backward.
   Writes the best match into |out|.
   |out|->score is updated only if a better match is found. */
static BROTLI_INLINE void FN(FindLongestMatch)(
    HashLongestMatch* BROTLI_RESTRICT self,
    const BrotliEncoderDictionary* dictionary,
    const uint8_t* BROTLI_RESTRICT data, const size_t ring_buffer_mask,
    const int* BROTLI_RESTRICT distance_cache, const size_t cur_ix,
    const size_t max_length, const size_t max_backward,
    const size_t dictionary_distance, const size_t max_distance,
    HasherSearchResult* BROTLI_RESTRICT out) {
  uint16_t* BROTLI_RESTRICT num = self->num_;
  uint32_t* BROTLI_RESTRICT buckets = self->buckets_;
  const size_t cur_ix_masked = cur_ix & ring_buffer_mask;
  /* Don't accept a short copy from far away. */
  score_t min_score = out->score;
  score_t best_score = out->score;
  size_t best_len = out->len;
  size_t i;
  out->len = 0;
  out->len_code_delta = 0;
  /* Try last distance first. */
  for (i = 0; i < (size_t)self->num_last_distances_to_check_; ++i) {
    const size_t backward = (size_t)distance_cache[i];
    size_t prev_ix = (size_t)(cur_ix - backward);
    if (prev_ix >= cur_ix) {
      continue;
    }
    if (BROTLI_PREDICT_FALSE(backward > max_backward)) {
      continue;
    }
    prev_ix &= ring_buffer_mask;

    if (cur_ix_masked + best_len > ring_buffer_mask ||
        prev_ix + best_len > ring_buffer_mask ||
        data[cur_ix_masked + best_len] != data[prev_ix + best_len]) {
      continue;
    }
    {
      const size_t len = FindMatchLengthWithLimit(&data[prev_ix],
                                                  &data[cur_ix_masked],
                                                  max_length);
      if (len >= 3 || (len == 2 && i < 2)) {
        /* Comparing for >= 2 does not change the semantics, but just saves for
           a few unnecessary binary logarithms in backward reference score,
           since we are not interested in such short matches. */
        score_t score = BackwardReferenceScoreUsingLastDistance(len);
        if (best_score < score) {
          if (i != 0) score -= BackwardReferencePenaltyUsingLastDistance(i);
          if (best_score < score) {
            best_score = score;
            best_len = len;
            out->len = best_len;
            out->distance = backward;
            out->score = best_score;
          }
        }
      }
    }
  }
  {
    const size_t key = FN(HashBytes)(&data[cur_ix_masked], self->hash_mul_);
    uint32_t* BROTLI_RESTRICT bucket = &buckets[key << self->block_bits_];
    const size_t down =
        (num[key] > self->block_size_) ?
        (num[key] - self->block_size_) : 0u;
    const uint32_t first4 = BrotliUnalignedRead32(data + cur_ix_masked);
    const size_t max_length_m4 = max_length - 4;
    i = num[key];
    for (; i > down;) {
      size_t prev_ix = bucket[--i & self->block_mask_];
      uint32_t current4;
      const size_t backward = cur_ix - prev_ix;
      if (BROTLI_PREDICT_FALSE(backward > max_backward)) {
        break;
      }
      prev_ix &= ring_buffer_mask;
      if (cur_ix_masked + best_len > ring_buffer_mask ||
          prev_ix + best_len > ring_buffer_mask ||
          data[cur_ix_masked + best_len] != data[prev_ix + best_len]) {
        continue;
      }
      current4 = BrotliUnalignedRead32(data + prev_ix);
      if (first4 != current4) continue;
      {
        const size_t len = FindMatchLengthWithLimit(&data[prev_ix + 4],
                                                    &data[cur_ix_masked + 4],
                                                    max_length_m4) + 4;
        const score_t score = BackwardReferenceScore(len, backward);
        if (best_score < score) {
          best_score = score;
          best_len = len;
          out->len = best_len;
          out->distance = backward;
          out->score = best_score;
        }
      }
    }
    bucket[num[key] & self->block_mask_] = (uint32_t)cur_ix;
    ++num[key];
  }
  if (min_score == out->score) {
    SearchInStaticDictionary(dictionary,
        self->common_, &data[cur_ix_masked], max_length, dictionary_distance,
        max_distance, out, BROTLI_FALSE);
  }
}

#undef HashLongestMatch
#undef HASHER

#define BUCKET_BITS 15

#define NUM_LAST_DISTANCES_TO_CHECK 4
#define NUM_BANKS 1
#define BANK_BITS 16
#define HASHER() H40
/* NOLINT(build/header_guard) */
/* Copyright 2016 Google Inc. All Rights Reserved.

   Distributed under MIT license.
   See file LICENSE for detail or copy at https://opensource.org/licenses/MIT
*/

/* template parameters: FN, BUCKET_BITS, NUM_BANKS, BANK_BITS,
                        NUM_LAST_DISTANCES_TO_CHECK */

/* A (forgetful) hash table to the data seen by the compressor, to
   help create backward references to previous data.

   Hashes are stored in chains which are bucketed to groups. Group of chains
   share a storage "bank". When more than "bank size" chain nodes are added,
   oldest nodes are replaced; this way several chains may share a tail. */

#define HashForgetfulChain HASHER()

#define BANK_SIZE (1 << BANK_BITS)

/* Number of hash buckets. */
#define BUCKET_SIZE (1 << BUCKET_BITS)

#define CAPPED_CHAINS 0

static BROTLI_INLINE size_t FN(HashTypeLength)(void) { return 4; }
static BROTLI_INLINE size_t FN(StoreLookahead)(void) { return 4; }

/* HashBytes is the function that chooses the bucket to place the address in.*/
static BROTLI_INLINE size_t FN(HashBytes)(const uint8_t* BROTLI_RESTRICT data) {
  const uint32_t h = BROTLI_UNALIGNED_LOAD32LE(data) * kHashMul32;
  /* The higher bits contain more mixture from the multiplication,
     so we take our results from there. */
  return h >> (32 - BUCKET_BITS);
}

typedef struct FN(Slot) {
  uint16_t delta;
  uint16_t next;
} FN(Slot);

typedef struct FN(Bank) {
  FN(Slot) slots[BANK_SIZE];
} FN(Bank);

typedef struct HashForgetfulChain {
  uint16_t free_slot_idx[NUM_BANKS];  /* Up to 1KiB. Move to dynamic? */
  size_t max_hops;

  /* Shortcuts. */
  void* extra[2];
  HasherCommon* common;

  /* --- Dynamic size members --- */

  /* uint32_t addr[BUCKET_SIZE]; */

  /* uint16_t head[BUCKET_SIZE]; */

  /* Truncated hash used for quick rejection of "distance cache" candidates. */
  /* uint8_t tiny_hash[65536];*/

  /* FN(Bank) banks[NUM_BANKS]; */
} HashForgetfulChain;

static uint32_t* FN(Addr)(void* extra) {
  return (uint32_t*)extra;
}

static uint16_t* FN(Head)(void* extra) {
  return (uint16_t*)(&FN(Addr)(extra)[BUCKET_SIZE]);
}

static uint8_t* FN(TinyHash)(void* extra) {
  return (uint8_t*)(&FN(Head)(extra)[BUCKET_SIZE]);
}

static FN(Bank)* FN(Banks)(void* extra) {
  return (FN(Bank)*)(extra);
}

static void FN(Initialize)(
    HasherCommon* common, HashForgetfulChain* BROTLI_RESTRICT self,
    const BrotliEncoderParams* params) {
  self->common = common;
  self->extra[0] = common->extra[0];
  self->extra[1] = common->extra[1];

  self->max_hops = (params->quality > 6 ? 7u : 8u) << (params->quality - 4);
}

static void FN(Prepare)(
    HashForgetfulChain* BROTLI_RESTRICT self, BROTLI_BOOL one_shot,
    size_t input_size, const uint8_t* BROTLI_RESTRICT data) {
  uint32_t* BROTLI_RESTRICT addr = FN(Addr)(self->extra[0]);
  uint16_t* BROTLI_RESTRICT head = FN(Head)(self->extra[0]);
  uint8_t* BROTLI_RESTRICT tiny_hash = FN(TinyHash)(self->extra[0]);
  /* Partial preparation is 100 times slower (per socket). */
  size_t partial_prepare_threshold = BUCKET_SIZE >> 6;
  if (one_shot && input_size <= partial_prepare_threshold) {
    size_t i;
    for (i = 0; i < input_size; ++i) {
      size_t bucket = FN(HashBytes)(&data[i]);
      /* See InitEmpty comment. */
      addr[bucket] = 0xCCCCCCCC;
      head[bucket] = 0xCCCC;
    }
  } else {
    /* Fill |addr| array with 0xCCCCCCCC value. Because of wrapping, position
       processed by hasher never reaches 3GB + 64M; this makes all new chains
       to be terminated after the first node. */
    memset(addr, 0xCC, sizeof(uint32_t) * BUCKET_SIZE);
    memset(head, 0, sizeof(uint16_t) * BUCKET_SIZE);
  }
  memset(tiny_hash, 0, sizeof(uint8_t) * 65536);
  memset(self->free_slot_idx, 0, sizeof(self->free_slot_idx));
}

static BROTLI_INLINE void FN(HashMemAllocInBytes)(
    const BrotliEncoderParams* params, BROTLI_BOOL one_shot,
    size_t input_size, size_t* alloc_size) {
  BROTLI_UNUSED(params);
  BROTLI_UNUSED(one_shot);
  BROTLI_UNUSED(input_size);
  alloc_size[0] = sizeof(uint32_t) * BUCKET_SIZE +
                  sizeof(uint16_t) * BUCKET_SIZE + sizeof(uint8_t) * 65536;
  alloc_size[1] = sizeof(FN(Bank)) * NUM_BANKS;
}

/* Look at 4 bytes at &data[ix & mask]. Compute a hash from these, and prepend
   node to corresponding chain; also update tiny_hash for current position. */
static BROTLI_INLINE void FN(Store)(HashForgetfulChain* BROTLI_RESTRICT self,
    const uint8_t* BROTLI_RESTRICT data, const size_t mask, const size_t ix) {
  uint32_t* BROTLI_RESTRICT addr = FN(Addr)(self->extra[0]);
  uint16_t* BROTLI_RESTRICT head = FN(Head)(self->extra[0]);
  uint8_t* BROTLI_RESTRICT tiny_hash = FN(TinyHash)(self->extra[0]);
  FN(Bank)* BROTLI_RESTRICT banks = FN(Banks)(self->extra[1]);
  const size_t key = FN(HashBytes)(&data[ix & mask]);
  const size_t bank = key & (NUM_BANKS - 1);
  const size_t idx = self->free_slot_idx[bank]++ & (BANK_SIZE - 1);
  size_t delta = ix - addr[key];
  tiny_hash[(uint16_t)ix] = (uint8_t)key;
  if (delta > 0xFFFF) delta = CAPPED_CHAINS ? 0 : 0xFFFF;
  banks[bank].slots[idx].delta = (uint16_t)delta;
  banks[bank].slots[idx].next = head[key];
  addr[key] = (uint32_t)ix;
  head[key] = (uint16_t)idx;
}

static BROTLI_INLINE void FN(StoreRange)(
    HashForgetfulChain* BROTLI_RESTRICT self,
    const uint8_t* BROTLI_RESTRICT data, const size_t mask,
    const size_t ix_start, const size_t ix_end) {
  size_t i;
  for (i = ix_start; i < ix_end; ++i) {
    FN(Store)(self, data, mask, i);
  }
}

static BROTLI_INLINE void FN(StitchToPreviousBlock)(
    HashForgetfulChain* BROTLI_RESTRICT self,
    size_t num_bytes, size_t position, const uint8_t* ringbuffer,
    size_t ring_buffer_mask) {
  if (num_bytes >= FN(HashTypeLength)() - 1 && position >= 3) {
    /* Prepare the hashes for three last bytes of the last write.
       These could not be calculated before, since they require knowledge
       of both the previous and the current block. */
    FN(Store)(self, ringbuffer, ring_buffer_mask, position - 3);
    FN(Store)(self, ringbuffer, ring_buffer_mask, position - 2);
    FN(Store)(self, ringbuffer, ring_buffer_mask, position - 1);
  }
}

static BROTLI_INLINE void FN(PrepareDistanceCache)(
    HashForgetfulChain* BROTLI_RESTRICT self,
    int* BROTLI_RESTRICT distance_cache) {
  BROTLI_UNUSED(self);
  PrepareDistanceCache(distance_cache, NUM_LAST_DISTANCES_TO_CHECK);
}

/* Find a longest backward match of &data[cur_ix] up to the length of
   max_length and stores the position cur_ix in the hash table.

   REQUIRES: FN(PrepareDistanceCache) must be invoked for current distance cache
             values; if this method is invoked repeatedly with the same distance
             cache values, it is enough to invoke FN(PrepareDistanceCache) once.

   Does not look for matches longer than max_length.
   Does not look for matches further away than max_backward.
   Writes the best match into |out|.
   |out|->score is updated only if a better match is found. */
static BROTLI_INLINE void FN(FindLongestMatch)(
    HashForgetfulChain* BROTLI_RESTRICT self,
    const BrotliEncoderDictionary* dictionary,
    const uint8_t* BROTLI_RESTRICT data, const size_t ring_buffer_mask,
    const int* BROTLI_RESTRICT distance_cache,
    const size_t cur_ix, const size_t max_length, const size_t max_backward,
    const size_t dictionary_distance, const size_t max_distance,
    HasherSearchResult* BROTLI_RESTRICT out) {
  uint32_t* BROTLI_RESTRICT addr = FN(Addr)(self->extra[0]);
  uint16_t* BROTLI_RESTRICT head = FN(Head)(self->extra[0]);
  uint8_t* BROTLI_RESTRICT tiny_hashes = FN(TinyHash)(self->extra[0]);
  FN(Bank)* BROTLI_RESTRICT banks = FN(Banks)(self->extra[1]);
  const size_t cur_ix_masked = cur_ix & ring_buffer_mask;
  /* Don't accept a short copy from far away. */
  score_t min_score = out->score;
  score_t best_score = out->score;
  size_t best_len = out->len;
  size_t i;
  const size_t key = FN(HashBytes)(&data[cur_ix_masked]);
  const uint8_t tiny_hash = (uint8_t)(key);
  out->len = 0;
  out->len_code_delta = 0;
  /* Try last distance first. */
  for (i = 0; i < NUM_LAST_DISTANCES_TO_CHECK; ++i) {
    const size_t backward = (size_t)distance_cache[i];
    size_t prev_ix = (cur_ix - backward);
    /* For distance code 0 we want to consider 2-byte matches. */
    if (i > 0 && tiny_hashes[(uint16_t)prev_ix] != tiny_hash) continue;
    if (prev_ix >= cur_ix || backward > max_backward) {
      continue;
    }
    prev_ix &= ring_buffer_mask;
    {
      const size_t len = FindMatchLengthWithLimit(&data[prev_ix],
                                                  &data[cur_ix_masked],
                                                  max_length);
      if (len >= 2) {
        score_t score = BackwardReferenceScoreUsingLastDistance(len);
        if (best_score < score) {
          if (i != 0) score -= BackwardReferencePenaltyUsingLastDistance(i);
          if (best_score < score) {
            best_score = score;
            best_len = len;
            out->len = best_len;
            out->distance = backward;
            out->score = best_score;
          }
        }
      }
    }
  }
  {
    const size_t bank = key & (NUM_BANKS - 1);
    size_t backward = 0;
    size_t hops = self->max_hops;
    size_t delta = cur_ix - addr[key];
    size_t slot = head[key];
    while (hops--) {
      size_t prev_ix;
      size_t last = slot;
      backward += delta;
      if (backward > max_backward || (CAPPED_CHAINS && !delta)) break;
      prev_ix = (cur_ix - backward) & ring_buffer_mask;
      slot = banks[bank].slots[last].next;
      delta = banks[bank].slots[last].delta;
      if (cur_ix_masked + best_len > ring_buffer_mask ||
          prev_ix + best_len > ring_buffer_mask ||
          data[cur_ix_masked + best_len] != data[prev_ix + best_len]) {
        continue;
      }
      {
        const size_t len = FindMatchLengthWithLimit(&data[prev_ix],
                                                    &data[cur_ix_masked],
                                                    max_length);
        if (len >= 4) {
          /* Comparing for >= 3 does not change the semantics, but just saves
             for a few unnecessary binary logarithms in backward reference
             score, since we are not interested in such short matches. */
          score_t score = BackwardReferenceScore(len, backward);
          if (best_score < score) {
            best_score = score;
            best_len = len;
            out->len = best_len;
            out->distance = backward;
            out->score = best_score;
          }
        }
      }
    }
    FN(Store)(self, data, ring_buffer_mask, cur_ix);
  }
  if (out->score == min_score) {
    SearchInStaticDictionary(dictionary,
        self->common, &data[cur_ix_masked], max_length, dictionary_distance,
        max_distance, out, BROTLI_FALSE);
  }
}

#undef BANK_SIZE
#undef BUCKET_SIZE
#undef CAPPED_CHAINS

#undef HashForgetfulChain
#undef HASHER
#undef NUM_LAST_DISTANCES_TO_CHECK

#define NUM_LAST_DISTANCES_TO_CHECK 10
#define HASHER() H41
/* NOLINT(build/header_guard) */
/* Copyright 2016 Google Inc. All Rights Reserved.

   Distributed under MIT license.
   See file LICENSE for detail or copy at https://opensource.org/licenses/MIT
*/

/* template parameters: FN, BUCKET_BITS, NUM_BANKS, BANK_BITS,
                        NUM_LAST_DISTANCES_TO_CHECK */

/* A (forgetful) hash table to the data seen by the compressor, to
   help create backward references to previous data.

   Hashes are stored in chains which are bucketed to groups. Group of chains
   share a storage "bank". When more than "bank size" chain nodes are added,
   oldest nodes are replaced; this way several chains may share a tail. */

#define HashForgetfulChain HASHER()

#define BANK_SIZE (1 << BANK_BITS)

/* Number of hash buckets. */
#define BUCKET_SIZE (1 << BUCKET_BITS)

#define CAPPED_CHAINS 0

static BROTLI_INLINE size_t FN(HashTypeLength)(void) { return 4; }
static BROTLI_INLINE size_t FN(StoreLookahead)(void) { return 4; }

/* HashBytes is the function that chooses the bucket to place the address in.*/
static BROTLI_INLINE size_t FN(HashBytes)(const uint8_t* BROTLI_RESTRICT data) {
  const uint32_t h = BROTLI_UNALIGNED_LOAD32LE(data) * kHashMul32;
  /* The higher bits contain more mixture from the multiplication,
     so we take our results from there. */
  return h >> (32 - BUCKET_BITS);
}

typedef struct FN(Slot) {
  uint16_t delta;
  uint16_t next;
} FN(Slot);

typedef struct FN(Bank) {
  FN(Slot) slots[BANK_SIZE];
} FN(Bank);

typedef struct HashForgetfulChain {
  uint16_t free_slot_idx[NUM_BANKS];  /* Up to 1KiB. Move to dynamic? */
  size_t max_hops;

  /* Shortcuts. */
  void* extra[2];
  HasherCommon* common;

  /* --- Dynamic size members --- */

  /* uint32_t addr[BUCKET_SIZE]; */

  /* uint16_t head[BUCKET_SIZE]; */

  /* Truncated hash used for quick rejection of "distance cache" candidates. */
  /* uint8_t tiny_hash[65536];*/

  /* FN(Bank) banks[NUM_BANKS]; */
} HashForgetfulChain;

static uint32_t* FN(Addr)(void* extra) {
  return (uint32_t*)extra;
}

static uint16_t* FN(Head)(void* extra) {
  return (uint16_t*)(&FN(Addr)(extra)[BUCKET_SIZE]);
}

static uint8_t* FN(TinyHash)(void* extra) {
  return (uint8_t*)(&FN(Head)(extra)[BUCKET_SIZE]);
}

static FN(Bank)* FN(Banks)(void* extra) {
  return (FN(Bank)*)(extra);
}

static void FN(Initialize)(
    HasherCommon* common, HashForgetfulChain* BROTLI_RESTRICT self,
    const BrotliEncoderParams* params) {
  self->common = common;
  self->extra[0] = common->extra[0];
  self->extra[1] = common->extra[1];

  self->max_hops = (params->quality > 6 ? 7u : 8u) << (params->quality - 4);
}

static void FN(Prepare)(
    HashForgetfulChain* BROTLI_RESTRICT self, BROTLI_BOOL one_shot,
    size_t input_size, const uint8_t* BROTLI_RESTRICT data) {
  uint32_t* BROTLI_RESTRICT addr = FN(Addr)(self->extra[0]);
  uint16_t* BROTLI_RESTRICT head = FN(Head)(self->extra[0]);
  uint8_t* BROTLI_RESTRICT tiny_hash = FN(TinyHash)(self->extra[0]);
  /* Partial preparation is 100 times slower (per socket). */
  size_t partial_prepare_threshold = BUCKET_SIZE >> 6;
  if (one_shot && input_size <= partial_prepare_threshold) {
    size_t i;
    for (i = 0; i < input_size; ++i) {
      size_t bucket = FN(HashBytes)(&data[i]);
      /* See InitEmpty comment. */
      addr[bucket] = 0xCCCCCCCC;
      head[bucket] = 0xCCCC;
    }
  } else {
    /* Fill |addr| array with 0xCCCCCCCC value. Because of wrapping, position
       processed by hasher never reaches 3GB + 64M; this makes all new chains
       to be terminated after the first node. */
    memset(addr, 0xCC, sizeof(uint32_t) * BUCKET_SIZE);
    memset(head, 0, sizeof(uint16_t) * BUCKET_SIZE);
  }
  memset(tiny_hash, 0, sizeof(uint8_t) * 65536);
  memset(self->free_slot_idx, 0, sizeof(self->free_slot_idx));
}

static BROTLI_INLINE void FN(HashMemAllocInBytes)(
    const BrotliEncoderParams* params, BROTLI_BOOL one_shot,
    size_t input_size, size_t* alloc_size) {
  BROTLI_UNUSED(params);
  BROTLI_UNUSED(one_shot);
  BROTLI_UNUSED(input_size);
  alloc_size[0] = sizeof(uint32_t) * BUCKET_SIZE +
                  sizeof(uint16_t) * BUCKET_SIZE + sizeof(uint8_t) * 65536;
  alloc_size[1] = sizeof(FN(Bank)) * NUM_BANKS;
}

/* Look at 4 bytes at &data[ix & mask]. Compute a hash from these, and prepend
   node to corresponding chain; also update tiny_hash for current position. */
static BROTLI_INLINE void FN(Store)(HashForgetfulChain* BROTLI_RESTRICT self,
    const uint8_t* BROTLI_RESTRICT data, const size_t mask, const size_t ix) {
  uint32_t* BROTLI_RESTRICT addr = FN(Addr)(self->extra[0]);
  uint16_t* BROTLI_RESTRICT head = FN(Head)(self->extra[0]);
  uint8_t* BROTLI_RESTRICT tiny_hash = FN(TinyHash)(self->extra[0]);
  FN(Bank)* BROTLI_RESTRICT banks = FN(Banks)(self->extra[1]);
  const size_t key = FN(HashBytes)(&data[ix & mask]);
  const size_t bank = key & (NUM_BANKS - 1);
  const size_t idx = self->free_slot_idx[bank]++ & (BANK_SIZE - 1);
  size_t delta = ix - addr[key];
  tiny_hash[(uint16_t)ix] = (uint8_t)key;
  if (delta > 0xFFFF) delta = CAPPED_CHAINS ? 0 : 0xFFFF;
  banks[bank].slots[idx].delta = (uint16_t)delta;
  banks[bank].slots[idx].next = head[key];
  addr[key] = (uint32_t)ix;
  head[key] = (uint16_t)idx;
}

static BROTLI_INLINE void FN(StoreRange)(
    HashForgetfulChain* BROTLI_RESTRICT self,
    const uint8_t* BROTLI_RESTRICT data, const size_t mask,
    const size_t ix_start, const size_t ix_end) {
  size_t i;
  for (i = ix_start; i < ix_end; ++i) {
    FN(Store)(self, data, mask, i);
  }
}

static BROTLI_INLINE void FN(StitchToPreviousBlock)(
    HashForgetfulChain* BROTLI_RESTRICT self,
    size_t num_bytes, size_t position, const uint8_t* ringbuffer,
    size_t ring_buffer_mask) {
  if (num_bytes >= FN(HashTypeLength)() - 1 && position >= 3) {
    /* Prepare the hashes for three last bytes of the last write.
       These could not be calculated before, since they require knowledge
       of both the previous and the current block. */
    FN(Store)(self, ringbuffer, ring_buffer_mask, position - 3);
    FN(Store)(self, ringbuffer, ring_buffer_mask, position - 2);
    FN(Store)(self, ringbuffer, ring_buffer_mask, position - 1);
  }
}

static BROTLI_INLINE void FN(PrepareDistanceCache)(
    HashForgetfulChain* BROTLI_RESTRICT self,
    int* BROTLI_RESTRICT distance_cache) {
  BROTLI_UNUSED(self);
  PrepareDistanceCache(distance_cache, NUM_LAST_DISTANCES_TO_CHECK);
}

/* Find a longest backward match of &data[cur_ix] up to the length of
   max_length and stores the position cur_ix in the hash table.

   REQUIRES: FN(PrepareDistanceCache) must be invoked for current distance cache
             values; if this method is invoked repeatedly with the same distance
             cache values, it is enough to invoke FN(PrepareDistanceCache) once.

   Does not look for matches longer than max_length.
   Does not look for matches further away than max_backward.
   Writes the best match into |out|.
   |out|->score is updated only if a better match is found. */
static BROTLI_INLINE void FN(FindLongestMatch)(
    HashForgetfulChain* BROTLI_RESTRICT self,
    const BrotliEncoderDictionary* dictionary,
    const uint8_t* BROTLI_RESTRICT data, const size_t ring_buffer_mask,
    const int* BROTLI_RESTRICT distance_cache,
    const size_t cur_ix, const size_t max_length, const size_t max_backward,
    const size_t dictionary_distance, const size_t max_distance,
    HasherSearchResult* BROTLI_RESTRICT out) {
  uint32_t* BROTLI_RESTRICT addr = FN(Addr)(self->extra[0]);
  uint16_t* BROTLI_RESTRICT head = FN(Head)(self->extra[0]);
  uint8_t* BROTLI_RESTRICT tiny_hashes = FN(TinyHash)(self->extra[0]);
  FN(Bank)* BROTLI_RESTRICT banks = FN(Banks)(self->extra[1]);
  const size_t cur_ix_masked = cur_ix & ring_buffer_mask;
  /* Don't accept a short copy from far away. */
  score_t min_score = out->score;
  score_t best_score = out->score;
  size_t best_len = out->len;
  size_t i;
  const size_t key = FN(HashBytes)(&data[cur_ix_masked]);
  const uint8_t tiny_hash = (uint8_t)(key);
  out->len = 0;
  out->len_code_delta = 0;
  /* Try last distance first. */
  for (i = 0; i < NUM_LAST_DISTANCES_TO_CHECK; ++i) {
    const size_t backward = (size_t)distance_cache[i];
    size_t prev_ix = (cur_ix - backward);
    /* For distance code 0 we want to consider 2-byte matches. */
    if (i > 0 && tiny_hashes[(uint16_t)prev_ix] != tiny_hash) continue;
    if (prev_ix >= cur_ix || backward > max_backward) {
      continue;
    }
    prev_ix &= ring_buffer_mask;
    {
      const size_t len = FindMatchLengthWithLimit(&data[prev_ix],
                                                  &data[cur_ix_masked],
                                                  max_length);
      if (len >= 2) {
        score_t score = BackwardReferenceScoreUsingLastDistance(len);
        if (best_score < score) {
          if (i != 0) score -= BackwardReferencePenaltyUsingLastDistance(i);
          if (best_score < score) {
            best_score = score;
            best_len = len;
            out->len = best_len;
            out->distance = backward;
            out->score = best_score;
          }
        }
      }
    }
  }
  {
    const size_t bank = key & (NUM_BANKS - 1);
    size_t backward = 0;
    size_t hops = self->max_hops;
    size_t delta = cur_ix - addr[key];
    size_t slot = head[key];
    while (hops--) {
      size_t prev_ix;
      size_t last = slot;
      backward += delta;
      if (backward > max_backward || (CAPPED_CHAINS && !delta)) break;
      prev_ix = (cur_ix - backward) & ring_buffer_mask;
      slot = banks[bank].slots[last].next;
      delta = banks[bank].slots[last].delta;
      if (cur_ix_masked + best_len > ring_buffer_mask ||
          prev_ix + best_len > ring_buffer_mask ||
          data[cur_ix_masked + best_len] != data[prev_ix + best_len]) {
        continue;
      }
      {
        const size_t len = FindMatchLengthWithLimit(&data[prev_ix],
                                                    &data[cur_ix_masked],
                                                    max_length);
        if (len >= 4) {
          /* Comparing for >= 3 does not change the semantics, but just saves
             for a few unnecessary binary logarithms in backward reference
             score, since we are not interested in such short matches. */
          score_t score = BackwardReferenceScore(len, backward);
          if (best_score < score) {
            best_score = score;
            best_len = len;
            out->len = best_len;
            out->distance = backward;
            out->score = best_score;
          }
        }
      }
    }
    FN(Store)(self, data, ring_buffer_mask, cur_ix);
  }
  if (out->score == min_score) {
    SearchInStaticDictionary(dictionary,
        self->common, &data[cur_ix_masked], max_length, dictionary_distance,
        max_distance, out, BROTLI_FALSE);
  }
}

#undef BANK_SIZE
#undef BUCKET_SIZE
#undef CAPPED_CHAINS

#undef HashForgetfulChain
#undef HASHER
#undef NUM_LAST_DISTANCES_TO_CHECK
#undef NUM_BANKS
#undef BANK_BITS

#define NUM_LAST_DISTANCES_TO_CHECK 16
#define NUM_BANKS 512
#define BANK_BITS 9
#define HASHER() H42
/* NOLINT(build/header_guard) */
/* Copyright 2016 Google Inc. All Rights Reserved.

   Distributed under MIT license.
   See file LICENSE for detail or copy at https://opensource.org/licenses/MIT
*/

/* template parameters: FN, BUCKET_BITS, NUM_BANKS, BANK_BITS,
                        NUM_LAST_DISTANCES_TO_CHECK */

/* A (forgetful) hash table to the data seen by the compressor, to
   help create backward references to previous data.

   Hashes are stored in chains which are bucketed to groups. Group of chains
   share a storage "bank". When more than "bank size" chain nodes are added,
   oldest nodes are replaced; this way several chains may share a tail. */

#define HashForgetfulChain HASHER()

#define BANK_SIZE (1 << BANK_BITS)

/* Number of hash buckets. */
#define BUCKET_SIZE (1 << BUCKET_BITS)

#define CAPPED_CHAINS 0

static BROTLI_INLINE size_t FN(HashTypeLength)(void) { return 4; }
static BROTLI_INLINE size_t FN(StoreLookahead)(void) { return 4; }

/* HashBytes is the function that chooses the bucket to place the address in.*/
static BROTLI_INLINE size_t FN(HashBytes)(const uint8_t* BROTLI_RESTRICT data) {
  const uint32_t h = BROTLI_UNALIGNED_LOAD32LE(data) * kHashMul32;
  /* The higher bits contain more mixture from the multiplication,
     so we take our results from there. */
  return h >> (32 - BUCKET_BITS);
}

typedef struct FN(Slot) {
  uint16_t delta;
  uint16_t next;
} FN(Slot);

typedef struct FN(Bank) {
  FN(Slot) slots[BANK_SIZE];
} FN(Bank);

typedef struct HashForgetfulChain {
  uint16_t free_slot_idx[NUM_BANKS];  /* Up to 1KiB. Move to dynamic? */
  size_t max_hops;

  /* Shortcuts. */
  void* extra[2];
  HasherCommon* common;

  /* --- Dynamic size members --- */

  /* uint32_t addr[BUCKET_SIZE]; */

  /* uint16_t head[BUCKET_SIZE]; */

  /* Truncated hash used for quick rejection of "distance cache" candidates. */
  /* uint8_t tiny_hash[65536];*/

  /* FN(Bank) banks[NUM_BANKS]; */
} HashForgetfulChain;

static uint32_t* FN(Addr)(void* extra) {
  return (uint32_t*)extra;
}

static uint16_t* FN(Head)(void* extra) {
  return (uint16_t*)(&FN(Addr)(extra)[BUCKET_SIZE]);
}

static uint8_t* FN(TinyHash)(void* extra) {
  return (uint8_t*)(&FN(Head)(extra)[BUCKET_SIZE]);
}

static FN(Bank)* FN(Banks)(void* extra) {
  return (FN(Bank)*)(extra);
}

static void FN(Initialize)(
    HasherCommon* common, HashForgetfulChain* BROTLI_RESTRICT self,
    const BrotliEncoderParams* params) {
  self->common = common;
  self->extra[0] = common->extra[0];
  self->extra[1] = common->extra[1];

  self->max_hops = (params->quality > 6 ? 7u : 8u) << (params->quality - 4);
}

static void FN(Prepare)(
    HashForgetfulChain* BROTLI_RESTRICT self, BROTLI_BOOL one_shot,
    size_t input_size, const uint8_t* BROTLI_RESTRICT data) {
  uint32_t* BROTLI_RESTRICT addr = FN(Addr)(self->extra[0]);
  uint16_t* BROTLI_RESTRICT head = FN(Head)(self->extra[0]);
  uint8_t* BROTLI_RESTRICT tiny_hash = FN(TinyHash)(self->extra[0]);
  /* Partial preparation is 100 times slower (per socket). */
  size_t partial_prepare_threshold = BUCKET_SIZE >> 6;
  if (one_shot && input_size <= partial_prepare_threshold) {
    size_t i;
    for (i = 0; i < input_size; ++i) {
      size_t bucket = FN(HashBytes)(&data[i]);
      /* See InitEmpty comment. */
      addr[bucket] = 0xCCCCCCCC;
      head[bucket] = 0xCCCC;
    }
  } else {
    /* Fill |addr| array with 0xCCCCCCCC value. Because of wrapping, position
       processed by hasher never reaches 3GB + 64M; this makes all new chains
       to be terminated after the first node. */
    memset(addr, 0xCC, sizeof(uint32_t) * BUCKET_SIZE);
    memset(head, 0, sizeof(uint16_t) * BUCKET_SIZE);
  }
  memset(tiny_hash, 0, sizeof(uint8_t) * 65536);
  memset(self->free_slot_idx, 0, sizeof(self->free_slot_idx));
}

static BROTLI_INLINE void FN(HashMemAllocInBytes)(
    const BrotliEncoderParams* params, BROTLI_BOOL one_shot,
    size_t input_size, size_t* alloc_size) {
  BROTLI_UNUSED(params);
  BROTLI_UNUSED(one_shot);
  BROTLI_UNUSED(input_size);
  alloc_size[0] = sizeof(uint32_t) * BUCKET_SIZE +
                  sizeof(uint16_t) * BUCKET_SIZE + sizeof(uint8_t) * 65536;
  alloc_size[1] = sizeof(FN(Bank)) * NUM_BANKS;
}

/* Look at 4 bytes at &data[ix & mask]. Compute a hash from these, and prepend
   node to corresponding chain; also update tiny_hash for current position. */
static BROTLI_INLINE void FN(Store)(HashForgetfulChain* BROTLI_RESTRICT self,
    const uint8_t* BROTLI_RESTRICT data, const size_t mask, const size_t ix) {
  uint32_t* BROTLI_RESTRICT addr = FN(Addr)(self->extra[0]);
  uint16_t* BROTLI_RESTRICT head = FN(Head)(self->extra[0]);
  uint8_t* BROTLI_RESTRICT tiny_hash = FN(TinyHash)(self->extra[0]);
  FN(Bank)* BROTLI_RESTRICT banks = FN(Banks)(self->extra[1]);
  const size_t key = FN(HashBytes)(&data[ix & mask]);
  const size_t bank = key & (NUM_BANKS - 1);
  const size_t idx = self->free_slot_idx[bank]++ & (BANK_SIZE - 1);
  size_t delta = ix - addr[key];
  tiny_hash[(uint16_t)ix] = (uint8_t)key;
  if (delta > 0xFFFF) delta = CAPPED_CHAINS ? 0 : 0xFFFF;
  banks[bank].slots[idx].delta = (uint16_t)delta;
  banks[bank].slots[idx].next = head[key];
  addr[key] = (uint32_t)ix;
  head[key] = (uint16_t)idx;
}

static BROTLI_INLINE void FN(StoreRange)(
    HashForgetfulChain* BROTLI_RESTRICT self,
    const uint8_t* BROTLI_RESTRICT data, const size_t mask,
    const size_t ix_start, const size_t ix_end) {
  size_t i;
  for (i = ix_start; i < ix_end; ++i) {
    FN(Store)(self, data, mask, i);
  }
}

static BROTLI_INLINE void FN(StitchToPreviousBlock)(
    HashForgetfulChain* BROTLI_RESTRICT self,
    size_t num_bytes, size_t position, const uint8_t* ringbuffer,
    size_t ring_buffer_mask) {
  if (num_bytes >= FN(HashTypeLength)() - 1 && position >= 3) {
    /* Prepare the hashes for three last bytes of the last write.
       These could not be calculated before, since they require knowledge
       of both the previous and the current block. */
    FN(Store)(self, ringbuffer, ring_buffer_mask, position - 3);
    FN(Store)(self, ringbuffer, ring_buffer_mask, position - 2);
    FN(Store)(self, ringbuffer, ring_buffer_mask, position - 1);
  }
}

static BROTLI_INLINE void FN(PrepareDistanceCache)(
    HashForgetfulChain* BROTLI_RESTRICT self,
    int* BROTLI_RESTRICT distance_cache) {
  BROTLI_UNUSED(self);
  PrepareDistanceCache(distance_cache, NUM_LAST_DISTANCES_TO_CHECK);
}

/* Find a longest backward match of &data[cur_ix] up to the length of
   max_length and stores the position cur_ix in the hash table.

   REQUIRES: FN(PrepareDistanceCache) must be invoked for current distance cache
             values; if this method is invoked repeatedly with the same distance
             cache values, it is enough to invoke FN(PrepareDistanceCache) once.

   Does not look for matches longer than max_length.
   Does not look for matches further away than max_backward.
   Writes the best match into |out|.
   |out|->score is updated only if a better match is found. */
static BROTLI_INLINE void FN(FindLongestMatch)(
    HashForgetfulChain* BROTLI_RESTRICT self,
    const BrotliEncoderDictionary* dictionary,
    const uint8_t* BROTLI_RESTRICT data, const size_t ring_buffer_mask,
    const int* BROTLI_RESTRICT distance_cache,
    const size_t cur_ix, const size_t max_length, const size_t max_backward,
    const size_t dictionary_distance, const size_t max_distance,
    HasherSearchResult* BROTLI_RESTRICT out) {
  uint32_t* BROTLI_RESTRICT addr = FN(Addr)(self->extra[0]);
  uint16_t* BROTLI_RESTRICT head = FN(Head)(self->extra[0]);
  uint8_t* BROTLI_RESTRICT tiny_hashes = FN(TinyHash)(self->extra[0]);
  FN(Bank)* BROTLI_RESTRICT banks = FN(Banks)(self->extra[1]);
  const size_t cur_ix_masked = cur_ix & ring_buffer_mask;
  /* Don't accept a short copy from far away. */
  score_t min_score = out->score;
  score_t best_score = out->score;
  size_t best_len = out->len;
  size_t i;
  const size_t key = FN(HashBytes)(&data[cur_ix_masked]);
  const uint8_t tiny_hash = (uint8_t)(key);
  out->len = 0;
  out->len_code_delta = 0;
  /* Try last distance first. */
  for (i = 0; i < NUM_LAST_DISTANCES_TO_CHECK; ++i) {
    const size_t backward = (size_t)distance_cache[i];
    size_t prev_ix = (cur_ix - backward);
    /* For distance code 0 we want to consider 2-byte matches. */
    if (i > 0 && tiny_hashes[(uint16_t)prev_ix] != tiny_hash) continue;
    if (prev_ix >= cur_ix || backward > max_backward) {
      continue;
    }
    prev_ix &= ring_buffer_mask;
    {
      const size_t len = FindMatchLengthWithLimit(&data[prev_ix],
                                                  &data[cur_ix_masked],
                                                  max_length);
      if (len >= 2) {
        score_t score = BackwardReferenceScoreUsingLastDistance(len);
        if (best_score < score) {
          if (i != 0) score -= BackwardReferencePenaltyUsingLastDistance(i);
          if (best_score < score) {
            best_score = score;
            best_len = len;
            out->len = best_len;
            out->distance = backward;
            out->score = best_score;
          }
        }
      }
    }
  }
  {
    const size_t bank = key & (NUM_BANKS - 1);
    size_t backward = 0;
    size_t hops = self->max_hops;
    size_t delta = cur_ix - addr[key];
    size_t slot = head[key];
    while (hops--) {
      size_t prev_ix;
      size_t last = slot;
      backward += delta;
      if (backward > max_backward || (CAPPED_CHAINS && !delta)) break;
      prev_ix = (cur_ix - backward) & ring_buffer_mask;
      slot = banks[bank].slots[last].next;
      delta = banks[bank].slots[last].delta;
      if (cur_ix_masked + best_len > ring_buffer_mask ||
          prev_ix + best_len > ring_buffer_mask ||
          data[cur_ix_masked + best_len] != data[prev_ix + best_len]) {
        continue;
      }
      {
        const size_t len = FindMatchLengthWithLimit(&data[prev_ix],
                                                    &data[cur_ix_masked],
                                                    max_length);
        if (len >= 4) {
          /* Comparing for >= 3 does not change the semantics, but just saves
             for a few unnecessary binary logarithms in backward reference
             score, since we are not interested in such short matches. */
          score_t score = BackwardReferenceScore(len, backward);
          if (best_score < score) {
            best_score = score;
            best_len = len;
            out->len = best_len;
            out->distance = backward;
            out->score = best_score;
          }
        }
      }
    }
    FN(Store)(self, data, ring_buffer_mask, cur_ix);
  }
  if (out->score == min_score) {
    SearchInStaticDictionary(dictionary,
        self->common, &data[cur_ix_masked], max_length, dictionary_distance,
        max_distance, out, BROTLI_FALSE);
  }
}

#undef BANK_SIZE
#undef BUCKET_SIZE
#undef CAPPED_CHAINS

#undef HashForgetfulChain
#undef HASHER
#undef NUM_LAST_DISTANCES_TO_CHECK
#undef NUM_BANKS
#undef BANK_BITS

#undef BUCKET_BITS

#define HASHER() H54
#define BUCKET_BITS 20
#define BUCKET_SWEEP_BITS 2
#define HASH_LEN 7
#define USE_DICTIONARY 0
/* NOLINT(build/header_guard) */
/* Copyright 2010 Google Inc. All Rights Reserved.

   Distributed under MIT license.
   See file LICENSE for detail or copy at https://opensource.org/licenses/MIT
*/

/* template parameters: FN, BUCKET_BITS, BUCKET_SWEEP_BITS, HASH_LEN,
                        USE_DICTIONARY
 */

#define HashLongestMatchQuickly HASHER()

#define BUCKET_SIZE (1 << BUCKET_BITS)
#define BUCKET_MASK (BUCKET_SIZE - 1)
#define BUCKET_SWEEP (1 << BUCKET_SWEEP_BITS)
#define BUCKET_SWEEP_MASK ((BUCKET_SWEEP - 1) << 3)

static BROTLI_INLINE size_t FN(HashTypeLength)(void) { return 8; }
static BROTLI_INLINE size_t FN(StoreLookahead)(void) { return 8; }

/* HashBytes is the function that chooses the bucket to place
   the address in. The HashLongestMatch and HashLongestMatchQuickly
   classes have separate, different implementations of hashing. */
static uint32_t FN(HashBytes)(const uint8_t* data) {
  const uint64_t h = ((BROTLI_UNALIGNED_LOAD64LE(data) << (64 - 8 * HASH_LEN)) *
                      kHashMul64);
  /* The higher bits contain more mixture from the multiplication,
     so we take our results from there. */
  return (uint32_t)(h >> (64 - BUCKET_BITS));
}

/* A (forgetful) hash table to the data seen by the compressor, to
   help create backward references to previous data.

   This is a hash map of fixed size (BUCKET_SIZE). */
typedef struct HashLongestMatchQuickly {
  /* Shortcuts. */
  HasherCommon* common;

  /* --- Dynamic size members --- */

  uint32_t* buckets_;  /* uint32_t[BUCKET_SIZE]; */
} HashLongestMatchQuickly;

static void FN(Initialize)(
    HasherCommon* common, HashLongestMatchQuickly* BROTLI_RESTRICT self,
    const BrotliEncoderParams* params) {
  self->common = common;

  BROTLI_UNUSED(params);
  self->buckets_ = (uint32_t*)common->extra[0];
}

static void FN(Prepare)(
    HashLongestMatchQuickly* BROTLI_RESTRICT self, BROTLI_BOOL one_shot,
    size_t input_size, const uint8_t* BROTLI_RESTRICT data) {
  uint32_t* BROTLI_RESTRICT buckets = self->buckets_;
  /* Partial preparation is 100 times slower (per socket). */
  size_t partial_prepare_threshold = BUCKET_SIZE >> 5;
  if (one_shot && input_size <= partial_prepare_threshold) {
    size_t i;
    for (i = 0; i < input_size; ++i) {
      const uint32_t key = FN(HashBytes)(&data[i]);
      if (BUCKET_SWEEP == 1) {
        buckets[key] = 0;
      } else {
        uint32_t j;
        for (j = 0; j < BUCKET_SWEEP; ++j) {
          buckets[(key + (j << 3)) & BUCKET_MASK] = 0;
        }
      }
    }
  } else {
    /* It is not strictly necessary to fill this buffer here, but
       not filling will make the results of the compression stochastic
       (but correct). This is because random data would cause the
       system to find accidentally good backward references here and there. */
    memset(buckets, 0, sizeof(uint32_t) * BUCKET_SIZE);
  }
}

static BROTLI_INLINE void FN(HashMemAllocInBytes)(
    const BrotliEncoderParams* params, BROTLI_BOOL one_shot,
    size_t input_size, size_t* alloc_size) {
  BROTLI_UNUSED(params);
  BROTLI_UNUSED(one_shot);
  BROTLI_UNUSED(input_size);
  alloc_size[0] = sizeof(uint32_t) * BUCKET_SIZE;
}

/* Look at 5 bytes at &data[ix & mask].
   Compute a hash from these, and store the value somewhere within
   [ix .. ix+3]. */
static BROTLI_INLINE void FN(Store)(
    HashLongestMatchQuickly* BROTLI_RESTRICT self,
    const uint8_t* BROTLI_RESTRICT data, const size_t mask, const size_t ix) {
  const uint32_t key = FN(HashBytes)(&data[ix & mask]);
  if (BUCKET_SWEEP == 1) {
    self->buckets_[key] = (uint32_t)ix;
  } else {
    /* Wiggle the value with the bucket sweep range. */
    const uint32_t off = ix & BUCKET_SWEEP_MASK;
    self->buckets_[(key + off) & BUCKET_MASK] = (uint32_t)ix;
  }
}

static BROTLI_INLINE void FN(StoreRange)(
    HashLongestMatchQuickly* BROTLI_RESTRICT self,
    const uint8_t* BROTLI_RESTRICT data, const size_t mask,
    const size_t ix_start, const size_t ix_end) {
  size_t i;
  for (i = ix_start; i < ix_end; ++i) {
    FN(Store)(self, data, mask, i);
  }
}

static BROTLI_INLINE void FN(StitchToPreviousBlock)(
    HashLongestMatchQuickly* BROTLI_RESTRICT self,
    size_t num_bytes, size_t position,
    const uint8_t* ringbuffer, size_t ringbuffer_mask) {
  if (num_bytes >= FN(HashTypeLength)() - 1 && position >= 3) {
    /* Prepare the hashes for three last bytes of the last write.
       These could not be calculated before, since they require knowledge
       of both the previous and the current block. */
    FN(Store)(self, ringbuffer, ringbuffer_mask, position - 3);
    FN(Store)(self, ringbuffer, ringbuffer_mask, position - 2);
    FN(Store)(self, ringbuffer, ringbuffer_mask, position - 1);
  }
}

static BROTLI_INLINE void FN(PrepareDistanceCache)(
    HashLongestMatchQuickly* BROTLI_RESTRICT self,
    int* BROTLI_RESTRICT distance_cache) {
  BROTLI_UNUSED(self);
  BROTLI_UNUSED(distance_cache);
}

/* Find a longest backward match of &data[cur_ix & ring_buffer_mask]
   up to the length of max_length and stores the position cur_ix in the
   hash table.

   Does not look for matches longer than max_length.
   Does not look for matches further away than max_backward.
   Writes the best match into |out|.
   |out|->score is updated only if a better match is found. */
static BROTLI_INLINE void FN(FindLongestMatch)(
    HashLongestMatchQuickly* BROTLI_RESTRICT self,
    const BrotliEncoderDictionary* dictionary,
    const uint8_t* BROTLI_RESTRICT data,
    const size_t ring_buffer_mask, const int* BROTLI_RESTRICT distance_cache,
    const size_t cur_ix, const size_t max_length, const size_t max_backward,
    const size_t dictionary_distance, const size_t max_distance,
    HasherSearchResult* BROTLI_RESTRICT out) {
  uint32_t* BROTLI_RESTRICT buckets = self->buckets_;
  const size_t best_len_in = out->len;
  const size_t cur_ix_masked = cur_ix & ring_buffer_mask;
  int compare_char = data[cur_ix_masked + best_len_in];
  size_t key = FN(HashBytes)(&data[cur_ix_masked]);
  size_t key_out;
  score_t min_score = out->score;
  score_t best_score = out->score;
  size_t best_len = best_len_in;
  size_t cached_backward = (size_t)distance_cache[0];
  size_t prev_ix = cur_ix - cached_backward;
  out->len_code_delta = 0;
  if (prev_ix < cur_ix) {
    prev_ix &= (uint32_t)ring_buffer_mask;
    if (compare_char == data[prev_ix + best_len]) {
      const size_t len = FindMatchLengthWithLimit(
          &data[prev_ix], &data[cur_ix_masked], max_length);
      if (len >= 4) {
        const score_t score = BackwardReferenceScoreUsingLastDistance(len);
        if (best_score < score) {
          out->len = len;
          out->distance = cached_backward;
          out->score = score;
          if (BUCKET_SWEEP == 1) {
            buckets[key] = (uint32_t)cur_ix;
            return;
          } else {
            best_len = len;
            best_score = score;
            compare_char = data[cur_ix_masked + len];
          }
        }
      }
    }
  }
  if (BUCKET_SWEEP == 1) {
    size_t backward;
    size_t len;
    /* Only one to look for, don't bother to prepare for a loop. */
    prev_ix = buckets[key];
    buckets[key] = (uint32_t)cur_ix;
    backward = cur_ix - prev_ix;
    prev_ix &= (uint32_t)ring_buffer_mask;
    if (compare_char != data[prev_ix + best_len_in]) {
      return;
    }
    if (BROTLI_PREDICT_FALSE(backward == 0 || backward > max_backward)) {
      return;
    }
    len = FindMatchLengthWithLimit(&data[prev_ix],
                                   &data[cur_ix_masked],
                                   max_length);
    if (len >= 4) {
      const score_t score = BackwardReferenceScore(len, backward);
      if (best_score < score) {
        out->len = len;
        out->distance = backward;
        out->score = score;
        return;
      }
    }
  } else {
    size_t keys[BUCKET_SWEEP];
    size_t i;
    for (i = 0; i < BUCKET_SWEEP; ++i) {
      keys[i] = (key + (i << 3)) & BUCKET_MASK;
    }
    key_out = keys[(cur_ix & BUCKET_SWEEP_MASK) >> 3];
    for (i = 0; i < BUCKET_SWEEP; ++i) {
      size_t len;
      size_t backward;
      prev_ix = buckets[keys[i]];
      backward = cur_ix - prev_ix;
      prev_ix &= (uint32_t)ring_buffer_mask;
      if (compare_char != data[prev_ix + best_len]) {
        continue;
      }
      if (BROTLI_PREDICT_FALSE(backward == 0 || backward > max_backward)) {
        continue;
      }
      len = FindMatchLengthWithLimit(&data[prev_ix],
                                     &data[cur_ix_masked],
                                     max_length);
      if (len >= 4) {
        const score_t score = BackwardReferenceScore(len, backward);
        if (best_score < score) {
          best_len = len;
          out->len = len;
          compare_char = data[cur_ix_masked + len];
          best_score = score;
          out->score = score;
          out->distance = backward;
        }
      }
    }
  }
  if (USE_DICTIONARY && min_score == out->score) {
    SearchInStaticDictionary(dictionary,
        self->common, &data[cur_ix_masked], max_length, dictionary_distance,
        max_distance, out, BROTLI_TRUE);
  }
  if (BUCKET_SWEEP != 1) {
    buckets[key_out] = (uint32_t)cur_ix;
  }
}

#undef BUCKET_SWEEP_MASK
#undef BUCKET_SWEEP
#undef BUCKET_MASK
#undef BUCKET_SIZE

#undef HashLongestMatchQuickly
#undef USE_DICTIONARY
#undef HASH_LEN
#undef BUCKET_SWEEP_BITS
#undef BUCKET_BITS
#undef HASHER

/* fast large window hashers */

#define HASHER() HROLLING_FAST
#define CHUNKLEN 32
#define JUMP 4
#define NUMBUCKETS 16777216
#define MASK ((NUMBUCKETS * 64) - 1)
/* NOLINT(build/header_guard) */
/* Copyright 2018 Google Inc. All Rights Reserved.

   Distributed under MIT license.
   See file LICENSE for detail or copy at https://opensource.org/licenses/MIT
*/

/* template parameters: FN, JUMP, NUMBUCKETS, MASK, CHUNKLEN */
/* NUMBUCKETS / (MASK + 1) = probability of storing and using hash code. */
/* JUMP = skip bytes for speedup */

/* Rolling hash for long distance long string matches. Stores one position
   per bucket, bucket key is computed over a long region. */

#define HashRolling HASHER()

static const uint32_t FN(kRollingHashMul32) = 69069;
static const uint32_t FN(kInvalidPos) = 0xffffffff;

/* This hasher uses a longer forward length, but returning a higher value here
   will hurt compression by the main hasher when combined with a composite
   hasher. The hasher tests for forward itself instead. */
static BROTLI_INLINE size_t FN(HashTypeLength)(void) { return 4; }
static BROTLI_INLINE size_t FN(StoreLookahead)(void) { return 4; }

/* Computes a code from a single byte. A lookup table of 256 values could be
   used, but simply adding 1 works about as good. */
static uint32_t FN(HashByte)(uint8_t byte) {
  return (uint32_t)byte + 1u;
}

static uint32_t FN(HashRollingFunctionInitial)(uint32_t state, uint8_t add,
                                               uint32_t factor) {
  return (uint32_t)(factor * state + FN(HashByte)(add));
}

static uint32_t FN(HashRollingFunction)(uint32_t state, uint8_t add,
                                        uint8_t rem, uint32_t factor,
                                        uint32_t factor_remove) {
  return (uint32_t)(factor * state +
      FN(HashByte)(add) - factor_remove * FN(HashByte)(rem));
}

typedef struct HashRolling {
  uint32_t state;
  uint32_t* table;
  size_t next_ix;

  uint32_t chunk_len;
  uint32_t factor;
  uint32_t factor_remove;
} HashRolling;

static void FN(Initialize)(
    HasherCommon* common, HashRolling* BROTLI_RESTRICT self,
    const BrotliEncoderParams* params) {
  size_t i;
  self->state = 0;
  self->next_ix = 0;

  self->factor = FN(kRollingHashMul32);

  /* Compute the factor of the oldest byte to remove: factor**steps modulo
     0xffffffff (the multiplications rely on 32-bit overflow) */
  self->factor_remove = 1;
  for (i = 0; i < CHUNKLEN; i += JUMP) {
    self->factor_remove *= self->factor;
  }

  self->table = (uint32_t*)common->extra[0];
  for (i = 0; i < NUMBUCKETS; i++) {
    self->table[i] = FN(kInvalidPos);
  }

  BROTLI_UNUSED(params);
}

static void FN(Prepare)(HashRolling* BROTLI_RESTRICT self, BROTLI_BOOL one_shot,
    size_t input_size, const uint8_t* BROTLI_RESTRICT data) {
  size_t i;
  /* Too small size, cannot use this hasher. */
  if (input_size < CHUNKLEN) return;
  self->state = 0;
  for (i = 0; i < CHUNKLEN; i += JUMP) {
    self->state = FN(HashRollingFunctionInitial)(
        self->state, data[i], self->factor);
  }
  BROTLI_UNUSED(one_shot);
}

static BROTLI_INLINE void FN(HashMemAllocInBytes)(
    const BrotliEncoderParams* params, BROTLI_BOOL one_shot,
    size_t input_size, size_t* alloc_size) {
  BROTLI_UNUSED(params);
  BROTLI_UNUSED(one_shot);
  BROTLI_UNUSED(input_size);
  alloc_size[0] = NUMBUCKETS * sizeof(uint32_t);
}

static BROTLI_INLINE void FN(Store)(HashRolling* BROTLI_RESTRICT self,
    const uint8_t* BROTLI_RESTRICT data, const size_t mask, const size_t ix) {
  BROTLI_UNUSED(self);
  BROTLI_UNUSED(data);
  BROTLI_UNUSED(mask);
  BROTLI_UNUSED(ix);
}

static BROTLI_INLINE void FN(StoreRange)(HashRolling* BROTLI_RESTRICT self,
    const uint8_t* BROTLI_RESTRICT data, const size_t mask,
    const size_t ix_start, const size_t ix_end) {
  BROTLI_UNUSED(self);
  BROTLI_UNUSED(data);
  BROTLI_UNUSED(mask);
  BROTLI_UNUSED(ix_start);
  BROTLI_UNUSED(ix_end);
}

static BROTLI_INLINE void FN(StitchToPreviousBlock)(
    HashRolling* BROTLI_RESTRICT self,
    size_t num_bytes, size_t position, const uint8_t* ringbuffer,
    size_t ring_buffer_mask) {
  /* In this case we must re-initialize the hasher from scratch from the
     current position. */
  size_t position_masked;
  size_t available = num_bytes;
  if ((position & (JUMP - 1)) != 0) {
    size_t diff = JUMP - (position & (JUMP - 1));
    available = (diff > available) ? 0 : (available - diff);
    position += diff;
  }
  position_masked = position & ring_buffer_mask;
  /* wrapping around ringbuffer not handled. */
  if (available > ring_buffer_mask - position_masked) {
    available = ring_buffer_mask - position_masked;
  }

  FN(Prepare)(self, BROTLI_FALSE, available,
      ringbuffer + (position & ring_buffer_mask));
  self->next_ix = position;
  BROTLI_UNUSED(num_bytes);
}

static BROTLI_INLINE void FN(PrepareDistanceCache)(
    HashRolling* BROTLI_RESTRICT self,
    int* BROTLI_RESTRICT distance_cache) {
  BROTLI_UNUSED(self);
  BROTLI_UNUSED(distance_cache);
}

static BROTLI_INLINE void FN(FindLongestMatch)(
    HashRolling* BROTLI_RESTRICT self,
    const BrotliEncoderDictionary* dictionary,
    const uint8_t* BROTLI_RESTRICT data, const size_t ring_buffer_mask,
    const int* BROTLI_RESTRICT distance_cache, const size_t cur_ix,
    const size_t max_length, const size_t max_backward,
    const size_t dictionary_distance, const size_t max_distance,
    HasherSearchResult* BROTLI_RESTRICT out) {
  const size_t cur_ix_masked = cur_ix & ring_buffer_mask;
  size_t pos;

  if ((cur_ix & (JUMP - 1)) != 0) return;

  /* Not enough lookahead */
  if (max_length < CHUNKLEN) return;

  for (pos = self->next_ix; pos <= cur_ix; pos += JUMP) {
    uint32_t code = self->state & MASK;

    uint8_t rem = data[pos & ring_buffer_mask];
    uint8_t add = data[(pos + CHUNKLEN) & ring_buffer_mask];
    size_t found_ix = FN(kInvalidPos);

    self->state = FN(HashRollingFunction)(
        self->state, add, rem, self->factor, self->factor_remove);

    if (code < NUMBUCKETS) {
      found_ix = self->table[code];
      self->table[code] = (uint32_t)pos;
      if (pos == cur_ix && found_ix != FN(kInvalidPos)) {
        /* The cast to 32-bit makes backward distances up to 4GB work even
           if cur_ix is above 4GB, despite using 32-bit values in the table. */
        size_t backward = (uint32_t)(cur_ix - found_ix);
        if (backward <= max_backward) {
          const size_t found_ix_masked = found_ix & ring_buffer_mask;
          const size_t len = FindMatchLengthWithLimit(&data[found_ix_masked],
                                                      &data[cur_ix_masked],
                                                      max_length);
          if (len >= 4 && len > out->len) {
            score_t score = BackwardReferenceScore(len, backward);
            if (score > out->score) {
              out->len = len;
              out->distance = backward;
              out->score = score;
              out->len_code_delta = 0;
            }
          }
        }
      }
    }
  }

  self->next_ix = cur_ix + JUMP;

  /* NOTE: this hasher does not search in the dictionary. It is used as
     backup-hasher, the main hasher already searches in it. */
  BROTLI_UNUSED(dictionary);
  BROTLI_UNUSED(distance_cache);
  BROTLI_UNUSED(dictionary_distance);
  BROTLI_UNUSED(max_distance);
}

#undef HashRolling
#undef JUMP
#undef HASHER


#define HASHER() HROLLING
#define JUMP 1
/* NOLINT(build/header_guard) */
/* Copyright 2018 Google Inc. All Rights Reserved.

   Distributed under MIT license.
   See file LICENSE for detail or copy at https://opensource.org/licenses/MIT
*/

/* template parameters: FN, JUMP, NUMBUCKETS, MASK, CHUNKLEN */
/* NUMBUCKETS / (MASK + 1) = probability of storing and using hash code. */
/* JUMP = skip bytes for speedup */

/* Rolling hash for long distance long string matches. Stores one position
   per bucket, bucket key is computed over a long region. */

#define HashRolling HASHER()

static const uint32_t FN(kRollingHashMul32) = 69069;
static const uint32_t FN(kInvalidPos) = 0xffffffff;

/* This hasher uses a longer forward length, but returning a higher value here
   will hurt compression by the main hasher when combined with a composite
   hasher. The hasher tests for forward itself instead. */
static BROTLI_INLINE size_t FN(HashTypeLength)(void) { return 4; }
static BROTLI_INLINE size_t FN(StoreLookahead)(void) { return 4; }

/* Computes a code from a single byte. A lookup table of 256 values could be
   used, but simply adding 1 works about as good. */
static uint32_t FN(HashByte)(uint8_t byte) {
  return (uint32_t)byte + 1u;
}

static uint32_t FN(HashRollingFunctionInitial)(uint32_t state, uint8_t add,
                                               uint32_t factor) {
  return (uint32_t)(factor * state + FN(HashByte)(add));
}

static uint32_t FN(HashRollingFunction)(uint32_t state, uint8_t add,
                                        uint8_t rem, uint32_t factor,
                                        uint32_t factor_remove) {
  return (uint32_t)(factor * state +
      FN(HashByte)(add) - factor_remove * FN(HashByte)(rem));
}

typedef struct HashRolling {
  uint32_t state;
  uint32_t* table;
  size_t next_ix;

  uint32_t chunk_len;
  uint32_t factor;
  uint32_t factor_remove;
} HashRolling;

static void FN(Initialize)(
    HasherCommon* common, HashRolling* BROTLI_RESTRICT self,
    const BrotliEncoderParams* params) {
  size_t i;
  self->state = 0;
  self->next_ix = 0;

  self->factor = FN(kRollingHashMul32);

  /* Compute the factor of the oldest byte to remove: factor**steps modulo
     0xffffffff (the multiplications rely on 32-bit overflow) */
  self->factor_remove = 1;
  for (i = 0; i < CHUNKLEN; i += JUMP) {
    self->factor_remove *= self->factor;
  }

  self->table = (uint32_t*)common->extra[0];
  for (i = 0; i < NUMBUCKETS; i++) {
    self->table[i] = FN(kInvalidPos);
  }

  BROTLI_UNUSED(params);
}

static void FN(Prepare)(HashRolling* BROTLI_RESTRICT self, BROTLI_BOOL one_shot,
    size_t input_size, const uint8_t* BROTLI_RESTRICT data) {
  size_t i;
  /* Too small size, cannot use this hasher. */
  if (input_size < CHUNKLEN) return;
  self->state = 0;
  for (i = 0; i < CHUNKLEN; i += JUMP) {
    self->state = FN(HashRollingFunctionInitial)(
        self->state, data[i], self->factor);
  }
  BROTLI_UNUSED(one_shot);
}

static BROTLI_INLINE void FN(HashMemAllocInBytes)(
    const BrotliEncoderParams* params, BROTLI_BOOL one_shot,
    size_t input_size, size_t* alloc_size) {
  BROTLI_UNUSED(params);
  BROTLI_UNUSED(one_shot);
  BROTLI_UNUSED(input_size);
  alloc_size[0] = NUMBUCKETS * sizeof(uint32_t);
}

static BROTLI_INLINE void FN(Store)(HashRolling* BROTLI_RESTRICT self,
    const uint8_t* BROTLI_RESTRICT data, const size_t mask, const size_t ix) {
  BROTLI_UNUSED(self);
  BROTLI_UNUSED(data);
  BROTLI_UNUSED(mask);
  BROTLI_UNUSED(ix);
}

static BROTLI_INLINE void FN(StoreRange)(HashRolling* BROTLI_RESTRICT self,
    const uint8_t* BROTLI_RESTRICT data, const size_t mask,
    const size_t ix_start, const size_t ix_end) {
  BROTLI_UNUSED(self);
  BROTLI_UNUSED(data);
  BROTLI_UNUSED(mask);
  BROTLI_UNUSED(ix_start);
  BROTLI_UNUSED(ix_end);
}

static BROTLI_INLINE void FN(StitchToPreviousBlock)(
    HashRolling* BROTLI_RESTRICT self,
    size_t num_bytes, size_t position, const uint8_t* ringbuffer,
    size_t ring_buffer_mask) {
  /* In this case we must re-initialize the hasher from scratch from the
     current position. */
  size_t position_masked;
  size_t available = num_bytes;
  if ((position & (JUMP - 1)) != 0) {
    size_t diff = JUMP - (position & (JUMP - 1));
    available = (diff > available) ? 0 : (available - diff);
    position += diff;
  }
  position_masked = position & ring_buffer_mask;
  /* wrapping around ringbuffer not handled. */
  if (available > ring_buffer_mask - position_masked) {
    available = ring_buffer_mask - position_masked;
  }

  FN(Prepare)(self, BROTLI_FALSE, available,
      ringbuffer + (position & ring_buffer_mask));
  self->next_ix = position;
  BROTLI_UNUSED(num_bytes);
}

static BROTLI_INLINE void FN(PrepareDistanceCache)(
    HashRolling* BROTLI_RESTRICT self,
    int* BROTLI_RESTRICT distance_cache) {
  BROTLI_UNUSED(self);
  BROTLI_UNUSED(distance_cache);
}

static BROTLI_INLINE void FN(FindLongestMatch)(
    HashRolling* BROTLI_RESTRICT self,
    const BrotliEncoderDictionary* dictionary,
    const uint8_t* BROTLI_RESTRICT data, const size_t ring_buffer_mask,
    const int* BROTLI_RESTRICT distance_cache, const size_t cur_ix,
    const size_t max_length, const size_t max_backward,
    const size_t dictionary_distance, const size_t max_distance,
    HasherSearchResult* BROTLI_RESTRICT out) {
  const size_t cur_ix_masked = cur_ix & ring_buffer_mask;
  size_t pos;

  if ((cur_ix & (JUMP - 1)) != 0) return;

  /* Not enough lookahead */
  if (max_length < CHUNKLEN) return;

  for (pos = self->next_ix; pos <= cur_ix; pos += JUMP) {
    uint32_t code = self->state & MASK;

    uint8_t rem = data[pos & ring_buffer_mask];
    uint8_t add = data[(pos + CHUNKLEN) & ring_buffer_mask];
    size_t found_ix = FN(kInvalidPos);

    self->state = FN(HashRollingFunction)(
        self->state, add, rem, self->factor, self->factor_remove);

    if (code < NUMBUCKETS) {
      found_ix = self->table[code];
      self->table[code] = (uint32_t)pos;
      if (pos == cur_ix && found_ix != FN(kInvalidPos)) {
        /* The cast to 32-bit makes backward distances up to 4GB work even
           if cur_ix is above 4GB, despite using 32-bit values in the table. */
        size_t backward = (uint32_t)(cur_ix - found_ix);
        if (backward <= max_backward) {
          const size_t found_ix_masked = found_ix & ring_buffer_mask;
          const size_t len = FindMatchLengthWithLimit(&data[found_ix_masked],
                                                      &data[cur_ix_masked],
                                                      max_length);
          if (len >= 4 && len > out->len) {
            score_t score = BackwardReferenceScore(len, backward);
            if (score > out->score) {
              out->len = len;
              out->distance = backward;
              out->score = score;
              out->len_code_delta = 0;
            }
          }
        }
      }
    }
  }

  self->next_ix = cur_ix + JUMP;

  /* NOTE: this hasher does not search in the dictionary. It is used as
     backup-hasher, the main hasher already searches in it. */
  BROTLI_UNUSED(dictionary);
  BROTLI_UNUSED(distance_cache);
  BROTLI_UNUSED(dictionary_distance);
  BROTLI_UNUSED(max_distance);
}

#undef HashRolling
#undef MASK
#undef NUMBUCKETS
#undef JUMP
#undef CHUNKLEN
#undef HASHER

#define HASHER() H35
#define HASHER_A H3
#define HASHER_B HROLLING_FAST
/* NOLINT(build/header_guard) */
/* Copyright 2018 Google Inc. All Rights Reserved.

   Distributed under MIT license.
   See file LICENSE for detail or copy at https://opensource.org/licenses/MIT
*/

/* template parameters: FN, HASHER_A, HASHER_B */

/* Composite hasher: This hasher allows to combine two other hashers, HASHER_A
   and HASHER_B. */

#define HashComposite HASHER()

#define FN_A(X) EXPAND_CAT(X, HASHER_A)
#define FN_B(X) EXPAND_CAT(X, HASHER_B)

static BROTLI_INLINE size_t FN(HashTypeLength)(void) {
  size_t a =  FN_A(HashTypeLength)();
  size_t b =  FN_B(HashTypeLength)();
  return a > b ? a : b;
}

static BROTLI_INLINE size_t FN(StoreLookahead)(void) {
  size_t a =  FN_A(StoreLookahead)();
  size_t b =  FN_B(StoreLookahead)();
  return a > b ? a : b;
}

typedef struct HashComposite {
  HASHER_A ha;
  HASHER_B hb;
  HasherCommon ha_common;
  HasherCommon hb_common;

  /* Shortcuts. */
  HasherCommon* common;

  BROTLI_BOOL fresh;
  const BrotliEncoderParams* params;
} HashComposite;

static void FN(Initialize)(HasherCommon* common,
    HashComposite* BROTLI_RESTRICT self, const BrotliEncoderParams* params) {
  self->common = common;

  self->ha_common = *self->common;
  self->hb_common = *self->common;
  self->fresh = BROTLI_TRUE;
  self->params = params;
  /* TODO(lode): Initialize of the hashers is deferred to Prepare (and params
     remembered here) because we don't get the one_shot and input_size params
     here that are needed to know the memory size of them. Instead provide
     those params to all hashers FN(Initialize) */
}

static void FN(Prepare)(
    HashComposite* BROTLI_RESTRICT self, BROTLI_BOOL one_shot,
    size_t input_size, const uint8_t* BROTLI_RESTRICT data) {
  if (self->fresh) {
    self->fresh = BROTLI_FALSE;
    self->ha_common.extra[0] = self->common->extra[0];
    self->ha_common.extra[1] = self->common->extra[1];
    self->ha_common.extra[2] = NULL;
    self->ha_common.extra[3] = NULL;
    self->hb_common.extra[0] = self->common->extra[2];
    self->hb_common.extra[1] = self->common->extra[3];
    self->hb_common.extra[2] = NULL;
    self->hb_common.extra[3] = NULL;

    FN_A(Initialize)(&self->ha_common, &self->ha, self->params);
    FN_B(Initialize)(&self->hb_common, &self->hb, self->params);
  }
  FN_A(Prepare)(&self->ha, one_shot, input_size, data);
  FN_B(Prepare)(&self->hb, one_shot, input_size, data);
}

static BROTLI_INLINE void FN(HashMemAllocInBytes)(
    const BrotliEncoderParams* params, BROTLI_BOOL one_shot,
    size_t input_size, size_t* alloc_size) {
  size_t alloc_size_a[4] = {0};
  size_t alloc_size_b[4] = {0};
  FN_A(HashMemAllocInBytes)(params, one_shot, input_size, alloc_size_a);
  FN_B(HashMemAllocInBytes)(params, one_shot, input_size, alloc_size_b);
  /* Should never happen. */
  if (alloc_size_a[2] != 0 || alloc_size_a[3] != 0) exit(EXIT_FAILURE);
  if (alloc_size_b[2] != 0 || alloc_size_b[3] != 0) exit(EXIT_FAILURE);
  alloc_size[0] = alloc_size_a[0];
  alloc_size[1] = alloc_size_a[1];
  alloc_size[2] = alloc_size_b[0];
  alloc_size[3] = alloc_size_b[1];
}

static BROTLI_INLINE void FN(Store)(HashComposite* BROTLI_RESTRICT self,
    const uint8_t* BROTLI_RESTRICT data, const size_t mask, const size_t ix) {
  FN_A(Store)(&self->ha, data, mask, ix);
  FN_B(Store)(&self->hb, data, mask, ix);
}

static BROTLI_INLINE void FN(StoreRange)(
    HashComposite* BROTLI_RESTRICT self, const uint8_t* BROTLI_RESTRICT data,
    const size_t mask, const size_t ix_start,
    const size_t ix_end) {
  FN_A(StoreRange)(&self->ha, data, mask, ix_start, ix_end);
  FN_B(StoreRange)(&self->hb, data, mask, ix_start, ix_end);
}

static BROTLI_INLINE void FN(StitchToPreviousBlock)(
    HashComposite* BROTLI_RESTRICT self,
    size_t num_bytes, size_t position, const uint8_t* ringbuffer,
    size_t ring_buffer_mask) {
  FN_A(StitchToPreviousBlock)(&self->ha, num_bytes, position,
      ringbuffer, ring_buffer_mask);
  FN_B(StitchToPreviousBlock)(&self->hb, num_bytes, position,
      ringbuffer, ring_buffer_mask);
}

static BROTLI_INLINE void FN(PrepareDistanceCache)(
    HashComposite* BROTLI_RESTRICT self, int* BROTLI_RESTRICT distance_cache) {
  FN_A(PrepareDistanceCache)(&self->ha, distance_cache);
  FN_B(PrepareDistanceCache)(&self->hb, distance_cache);
}

static BROTLI_INLINE void FN(FindLongestMatch)(
    HashComposite* BROTLI_RESTRICT self,
    const BrotliEncoderDictionary* dictionary,
    const uint8_t* BROTLI_RESTRICT data, const size_t ring_buffer_mask,
    const int* BROTLI_RESTRICT distance_cache, const size_t cur_ix,
    const size_t max_length, const size_t max_backward,
    const size_t dictionary_distance, const size_t max_distance,
    HasherSearchResult* BROTLI_RESTRICT out) {
  FN_A(FindLongestMatch)(&self->ha, dictionary, data, ring_buffer_mask,
      distance_cache, cur_ix, max_length, max_backward, dictionary_distance,
      max_distance, out);
  FN_B(FindLongestMatch)(&self->hb, dictionary, data, ring_buffer_mask,
      distance_cache, cur_ix, max_length, max_backward, dictionary_distance,
      max_distance, out);
}

#undef HashComposite
#undef HASHER_A
#undef HASHER_B
#undef HASHER

#define HASHER() H55
#define HASHER_A H54
#define HASHER_B HROLLING_FAST
/* NOLINT(build/header_guard) */
/* Copyright 2018 Google Inc. All Rights Reserved.

   Distributed under MIT license.
   See file LICENSE for detail or copy at https://opensource.org/licenses/MIT
*/

/* template parameters: FN, HASHER_A, HASHER_B */

/* Composite hasher: This hasher allows to combine two other hashers, HASHER_A
   and HASHER_B. */

#define HashComposite HASHER()

#define FN_A(X) EXPAND_CAT(X, HASHER_A)
#define FN_B(X) EXPAND_CAT(X, HASHER_B)

static BROTLI_INLINE size_t FN(HashTypeLength)(void) {
  size_t a =  FN_A(HashTypeLength)();
  size_t b =  FN_B(HashTypeLength)();
  return a > b ? a : b;
}

static BROTLI_INLINE size_t FN(StoreLookahead)(void) {
  size_t a =  FN_A(StoreLookahead)();
  size_t b =  FN_B(StoreLookahead)();
  return a > b ? a : b;
}

typedef struct HashComposite {
  HASHER_A ha;
  HASHER_B hb;
  HasherCommon ha_common;
  HasherCommon hb_common;

  /* Shortcuts. */
  HasherCommon* common;

  BROTLI_BOOL fresh;
  const BrotliEncoderParams* params;
} HashComposite;

static void FN(Initialize)(HasherCommon* common,
    HashComposite* BROTLI_RESTRICT self, const BrotliEncoderParams* params) {
  self->common = common;

  self->ha_common = *self->common;
  self->hb_common = *self->common;
  self->fresh = BROTLI_TRUE;
  self->params = params;
  /* TODO(lode): Initialize of the hashers is deferred to Prepare (and params
     remembered here) because we don't get the one_shot and input_size params
     here that are needed to know the memory size of them. Instead provide
     those params to all hashers FN(Initialize) */
}

static void FN(Prepare)(
    HashComposite* BROTLI_RESTRICT self, BROTLI_BOOL one_shot,
    size_t input_size, const uint8_t* BROTLI_RESTRICT data) {
  if (self->fresh) {
    self->fresh = BROTLI_FALSE;
    self->ha_common.extra[0] = self->common->extra[0];
    self->ha_common.extra[1] = self->common->extra[1];
    self->ha_common.extra[2] = NULL;
    self->ha_common.extra[3] = NULL;
    self->hb_common.extra[0] = self->common->extra[2];
    self->hb_common.extra[1] = self->common->extra[3];
    self->hb_common.extra[2] = NULL;
    self->hb_common.extra[3] = NULL;

    FN_A(Initialize)(&self->ha_common, &self->ha, self->params);
    FN_B(Initialize)(&self->hb_common, &self->hb, self->params);
  }
  FN_A(Prepare)(&self->ha, one_shot, input_size, data);
  FN_B(Prepare)(&self->hb, one_shot, input_size, data);
}

static BROTLI_INLINE void FN(HashMemAllocInBytes)(
    const BrotliEncoderParams* params, BROTLI_BOOL one_shot,
    size_t input_size, size_t* alloc_size) {
  size_t alloc_size_a[4] = {0};
  size_t alloc_size_b[4] = {0};
  FN_A(HashMemAllocInBytes)(params, one_shot, input_size, alloc_size_a);
  FN_B(HashMemAllocInBytes)(params, one_shot, input_size, alloc_size_b);
  /* Should never happen. */
  if (alloc_size_a[2] != 0 || alloc_size_a[3] != 0) exit(EXIT_FAILURE);
  if (alloc_size_b[2] != 0 || alloc_size_b[3] != 0) exit(EXIT_FAILURE);
  alloc_size[0] = alloc_size_a[0];
  alloc_size[1] = alloc_size_a[1];
  alloc_size[2] = alloc_size_b[0];
  alloc_size[3] = alloc_size_b[1];
}

static BROTLI_INLINE void FN(Store)(HashComposite* BROTLI_RESTRICT self,
    const uint8_t* BROTLI_RESTRICT data, const size_t mask, const size_t ix) {
  FN_A(Store)(&self->ha, data, mask, ix);
  FN_B(Store)(&self->hb, data, mask, ix);
}

static BROTLI_INLINE void FN(StoreRange)(
    HashComposite* BROTLI_RESTRICT self, const uint8_t* BROTLI_RESTRICT data,
    const size_t mask, const size_t ix_start,
    const size_t ix_end) {
  FN_A(StoreRange)(&self->ha, data, mask, ix_start, ix_end);
  FN_B(StoreRange)(&self->hb, data, mask, ix_start, ix_end);
}

static BROTLI_INLINE void FN(StitchToPreviousBlock)(
    HashComposite* BROTLI_RESTRICT self,
    size_t num_bytes, size_t position, const uint8_t* ringbuffer,
    size_t ring_buffer_mask) {
  FN_A(StitchToPreviousBlock)(&self->ha, num_bytes, position,
      ringbuffer, ring_buffer_mask);
  FN_B(StitchToPreviousBlock)(&self->hb, num_bytes, position,
      ringbuffer, ring_buffer_mask);
}

static BROTLI_INLINE void FN(PrepareDistanceCache)(
    HashComposite* BROTLI_RESTRICT self, int* BROTLI_RESTRICT distance_cache) {
  FN_A(PrepareDistanceCache)(&self->ha, distance_cache);
  FN_B(PrepareDistanceCache)(&self->hb, distance_cache);
}

static BROTLI_INLINE void FN(FindLongestMatch)(
    HashComposite* BROTLI_RESTRICT self,
    const BrotliEncoderDictionary* dictionary,
    const uint8_t* BROTLI_RESTRICT data, const size_t ring_buffer_mask,
    const int* BROTLI_RESTRICT distance_cache, const size_t cur_ix,
    const size_t max_length, const size_t max_backward,
    const size_t dictionary_distance, const size_t max_distance,
    HasherSearchResult* BROTLI_RESTRICT out) {
  FN_A(FindLongestMatch)(&self->ha, dictionary, data, ring_buffer_mask,
      distance_cache, cur_ix, max_length, max_backward, dictionary_distance,
      max_distance, out);
  FN_B(FindLongestMatch)(&self->hb, dictionary, data, ring_buffer_mask,
      distance_cache, cur_ix, max_length, max_backward, dictionary_distance,
      max_distance, out);
}

#undef HashComposite
#undef HASHER_A
#undef HASHER_B
#undef HASHER

#define HASHER() H65
#define HASHER_A H6
#define HASHER_B HROLLING
/* NOLINT(build/header_guard) */
/* Copyright 2018 Google Inc. All Rights Reserved.

   Distributed under MIT license.
   See file LICENSE for detail or copy at https://opensource.org/licenses/MIT
*/

/* template parameters: FN, HASHER_A, HASHER_B */

/* Composite hasher: This hasher allows to combine two other hashers, HASHER_A
   and HASHER_B. */

#define HashComposite HASHER()

#define FN_A(X) EXPAND_CAT(X, HASHER_A)
#define FN_B(X) EXPAND_CAT(X, HASHER_B)

static BROTLI_INLINE size_t FN(HashTypeLength)(void) {
  size_t a =  FN_A(HashTypeLength)();
  size_t b =  FN_B(HashTypeLength)();
  return a > b ? a : b;
}

static BROTLI_INLINE size_t FN(StoreLookahead)(void) {
  size_t a =  FN_A(StoreLookahead)();
  size_t b =  FN_B(StoreLookahead)();
  return a > b ? a : b;
}

typedef struct HashComposite {
  HASHER_A ha;
  HASHER_B hb;
  HasherCommon ha_common;
  HasherCommon hb_common;

  /* Shortcuts. */
  HasherCommon* common;

  BROTLI_BOOL fresh;
  const BrotliEncoderParams* params;
} HashComposite;

static void FN(Initialize)(HasherCommon* common,
    HashComposite* BROTLI_RESTRICT self, const BrotliEncoderParams* params) {
  self->common = common;

  self->ha_common = *self->common;
  self->hb_common = *self->common;
  self->fresh = BROTLI_TRUE;
  self->params = params;
  /* TODO(lode): Initialize of the hashers is deferred to Prepare (and params
     remembered here) because we don't get the one_shot and input_size params
     here that are needed to know the memory size of them. Instead provide
     those params to all hashers FN(Initialize) */
}

static void FN(Prepare)(
    HashComposite* BROTLI_RESTRICT self, BROTLI_BOOL one_shot,
    size_t input_size, const uint8_t* BROTLI_RESTRICT data) {
  if (self->fresh) {
    self->fresh = BROTLI_FALSE;
    self->ha_common.extra[0] = self->common->extra[0];
    self->ha_common.extra[1] = self->common->extra[1];
    self->ha_common.extra[2] = NULL;
    self->ha_common.extra[3] = NULL;
    self->hb_common.extra[0] = self->common->extra[2];
    self->hb_common.extra[1] = self->common->extra[3];
    self->hb_common.extra[2] = NULL;
    self->hb_common.extra[3] = NULL;

    FN_A(Initialize)(&self->ha_common, &self->ha, self->params);
    FN_B(Initialize)(&self->hb_common, &self->hb, self->params);
  }
  FN_A(Prepare)(&self->ha, one_shot, input_size, data);
  FN_B(Prepare)(&self->hb, one_shot, input_size, data);
}

static BROTLI_INLINE void FN(HashMemAllocInBytes)(
    const BrotliEncoderParams* params, BROTLI_BOOL one_shot,
    size_t input_size, size_t* alloc_size) {
  size_t alloc_size_a[4] = {0};
  size_t alloc_size_b[4] = {0};
  FN_A(HashMemAllocInBytes)(params, one_shot, input_size, alloc_size_a);
  FN_B(HashMemAllocInBytes)(params, one_shot, input_size, alloc_size_b);
  /* Should never happen. */
  if (alloc_size_a[2] != 0 || alloc_size_a[3] != 0) exit(EXIT_FAILURE);
  if (alloc_size_b[2] != 0 || alloc_size_b[3] != 0) exit(EXIT_FAILURE);
  alloc_size[0] = alloc_size_a[0];
  alloc_size[1] = alloc_size_a[1];
  alloc_size[2] = alloc_size_b[0];
  alloc_size[3] = alloc_size_b[1];
}

static BROTLI_INLINE void FN(Store)(HashComposite* BROTLI_RESTRICT self,
    const uint8_t* BROTLI_RESTRICT data, const size_t mask, const size_t ix) {
  FN_A(Store)(&self->ha, data, mask, ix);
  FN_B(Store)(&self->hb, data, mask, ix);
}

static BROTLI_INLINE void FN(StoreRange)(
    HashComposite* BROTLI_RESTRICT self, const uint8_t* BROTLI_RESTRICT data,
    const size_t mask, const size_t ix_start,
    const size_t ix_end) {
  FN_A(StoreRange)(&self->ha, data, mask, ix_start, ix_end);
  FN_B(StoreRange)(&self->hb, data, mask, ix_start, ix_end);
}

static BROTLI_INLINE void FN(StitchToPreviousBlock)(
    HashComposite* BROTLI_RESTRICT self,
    size_t num_bytes, size_t position, const uint8_t* ringbuffer,
    size_t ring_buffer_mask) {
  FN_A(StitchToPreviousBlock)(&self->ha, num_bytes, position,
      ringbuffer, ring_buffer_mask);
  FN_B(StitchToPreviousBlock)(&self->hb, num_bytes, position,
      ringbuffer, ring_buffer_mask);
}

static BROTLI_INLINE void FN(PrepareDistanceCache)(
    HashComposite* BROTLI_RESTRICT self, int* BROTLI_RESTRICT distance_cache) {
  FN_A(PrepareDistanceCache)(&self->ha, distance_cache);
  FN_B(PrepareDistanceCache)(&self->hb, distance_cache);
}

static BROTLI_INLINE void FN(FindLongestMatch)(
    HashComposite* BROTLI_RESTRICT self,
    const BrotliEncoderDictionary* dictionary,
    const uint8_t* BROTLI_RESTRICT data, const size_t ring_buffer_mask,
    const int* BROTLI_RESTRICT distance_cache, const size_t cur_ix,
    const size_t max_length, const size_t max_backward,
    const size_t dictionary_distance, const size_t max_distance,
    HasherSearchResult* BROTLI_RESTRICT out) {
  FN_A(FindLongestMatch)(&self->ha, dictionary, data, ring_buffer_mask,
      distance_cache, cur_ix, max_length, max_backward, dictionary_distance,
      max_distance, out);
  FN_B(FindLongestMatch)(&self->hb, dictionary, data, ring_buffer_mask,
      distance_cache, cur_ix, max_length, max_backward, dictionary_distance,
      max_distance, out);
}

#undef HashComposite
#undef HASHER_A
#undef HASHER_B
#undef HASHER

#undef FN
#undef CAT
#undef EXPAND_CAT

#define FOR_SIMPLE_HASHERS(H) H(2) H(3) H(4) H(5) H(6) H(40) H(41) H(42) H(54)
#define FOR_COMPOSITE_HASHERS(H) H(35) H(55) H(65)
#define FOR_GENERIC_HASHERS(H) FOR_SIMPLE_HASHERS(H) FOR_COMPOSITE_HASHERS(H)
#define FOR_ALL_HASHERS(H) FOR_GENERIC_HASHERS(H) H(10)

typedef struct {
  HasherCommon common;

  union {
#define MEMBER_(N) \
    H ## N _H ## N;
    FOR_ALL_HASHERS(MEMBER_)
#undef MEMBER_
  } privat;
} Hasher;

/* MUST be invoked before any other method. */
static BROTLI_INLINE void HasherInit(Hasher* hasher) {
  hasher->common.is_setup_ = BROTLI_FALSE;
  hasher->common.extra[0] = NULL;
  hasher->common.extra[1] = NULL;
  hasher->common.extra[2] = NULL;
  hasher->common.extra[3] = NULL;
}

static BROTLI_INLINE void DestroyHasher(MemoryManager* m, Hasher* hasher) {
  if (hasher->common.extra[0] != NULL) BROTLI_FREE(m, hasher->common.extra[0]);
  if (hasher->common.extra[1] != NULL) BROTLI_FREE(m, hasher->common.extra[1]);
  if (hasher->common.extra[2] != NULL) BROTLI_FREE(m, hasher->common.extra[2]);
  if (hasher->common.extra[3] != NULL) BROTLI_FREE(m, hasher->common.extra[3]);
}

static BROTLI_INLINE void HasherReset(Hasher* hasher) {
  hasher->common.is_prepared_ = BROTLI_FALSE;
}

static BROTLI_INLINE void HasherSize(const BrotliEncoderParams* params,
    BROTLI_BOOL one_shot, const size_t input_size, size_t* alloc_size) {
  switch (params->hasher.type) {
#define SIZE_(N)                                                           \
    case N:                                                                \
      HashMemAllocInBytesH ## N(params, one_shot, input_size, alloc_size); \
      break;
    FOR_ALL_HASHERS(SIZE_)
#undef SIZE_
    default:
      break;
  }
}

static BROTLI_INLINE void HasherSetup(MemoryManager* m, Hasher* hasher,
    BrotliEncoderParams* params, const uint8_t* data, size_t position,
    size_t input_size, BROTLI_BOOL is_last) {
  BROTLI_BOOL one_shot = (position == 0 && is_last);
  if (!hasher->common.is_setup_) {
    size_t alloc_size[4] = {0};
    size_t i;
    ChooseHasher(params, &params->hasher);
    hasher->common.params = params->hasher;
    hasher->common.dict_num_lookups = 0;
    hasher->common.dict_num_matches = 0;
    HasherSize(params, one_shot, input_size, alloc_size);
    for (i = 0; i < 4; ++i) {
      if (alloc_size[i] == 0) continue;
      hasher->common.extra[i] = BROTLI_ALLOC(m, uint8_t, alloc_size[i]);
      if (BROTLI_IS_OOM(m) || BROTLI_IS_NULL(hasher->common.extra[i])) return;
    }
    switch (hasher->common.params.type) {
#define INITIALIZE_(N)                        \
      case N:                                 \
        InitializeH ## N(&hasher->common,     \
            &hasher->privat._H ## N, params); \
        break;
      FOR_ALL_HASHERS(INITIALIZE_);
#undef INITIALIZE_
      default:
        break;
    }
    HasherReset(hasher);
    hasher->common.is_setup_ = BROTLI_TRUE;
  }

  if (!hasher->common.is_prepared_) {
    switch (hasher->common.params.type) {
#define PREPARE_(N)                      \
      case N:                            \
        PrepareH ## N(                   \
            &hasher->privat._H ## N,     \
            one_shot, input_size, data); \
        break;
      FOR_ALL_HASHERS(PREPARE_)
#undef PREPARE_
      default: break;
    }
    hasher->common.is_prepared_ = BROTLI_TRUE;
  }
}

static BROTLI_INLINE void InitOrStitchToPreviousBlock(
    MemoryManager* m, Hasher* hasher, const uint8_t* data, size_t mask,
    BrotliEncoderParams* params, size_t position, size_t input_size,
    BROTLI_BOOL is_last) {
  HasherSetup(m, hasher, params, data, position, input_size, is_last);
  if (BROTLI_IS_OOM(m)) return;
  switch (hasher->common.params.type) {
#define INIT_(N)                             \
    case N:                                  \
      StitchToPreviousBlockH ## N(           \
          &hasher->privat._H ## N,           \
          input_size, position, data, mask); \
    break;
    FOR_ALL_HASHERS(INIT_)
#undef INIT_
    default: break;
  }
}

/* NB: when seamless dictionary-ring-buffer copies are implemented, don't forget
       to add proper guards for non-zero-BROTLI_PARAM_STREAM_OFFSET. */
static BROTLI_INLINE void FindCompoundDictionaryMatch(
    const PreparedDictionary* self, const uint8_t* BROTLI_RESTRICT data,
    const size_t ring_buffer_mask, const int* BROTLI_RESTRICT distance_cache,
    const size_t cur_ix, const size_t max_length, const size_t distance_offset,
    const size_t max_distance, HasherSearchResult* BROTLI_RESTRICT out) {
  const uint32_t source_size = self->source_size;
  const size_t boundary = distance_offset - source_size;
  const uint32_t hash_bits = self->hash_bits;
  const uint32_t bucket_bits = self->bucket_bits;
  const uint32_t slot_bits = self->slot_bits;

  const uint32_t hash_shift = 64u - bucket_bits;
  const uint32_t slot_mask = (~((uint32_t)0U)) >> (32 - slot_bits);
  const uint64_t hash_mask = (~((uint64_t)0U)) >> (64 - hash_bits);

  const uint32_t* slot_offsets = (uint32_t*)(&self[1]);
  const uint16_t* heads = (uint16_t*)(&slot_offsets[1u << slot_bits]);
  const uint32_t* items = (uint32_t*)(&heads[1u << bucket_bits]);
  const uint8_t* source = NULL;

  const size_t cur_ix_masked = cur_ix & ring_buffer_mask;
  score_t best_score = out->score;
  size_t best_len = out->len;
  size_t i;
  const uint64_t h =
      (BROTLI_UNALIGNED_LOAD64LE(&data[cur_ix_masked]) & hash_mask) *
      kPreparedDictionaryHashMul64Long;
  const uint32_t key = (uint32_t)(h >> hash_shift);
  const uint32_t slot = key & slot_mask;
  const uint32_t head = heads[key];
  const uint32_t* BROTLI_RESTRICT chain = &items[slot_offsets[slot] + head];
  uint32_t item = (head == 0xFFFF) ? 1 : 0;

  const void* tail = (void*)&items[self->num_items];
  if (self->magic == kPreparedDictionaryMagic) {
    source = (const uint8_t*)tail;
  } else {
    /* kLeanPreparedDictionaryMagic */
    source = (const uint8_t*)BROTLI_UNALIGNED_LOAD_PTR((const uint8_t**)tail);
  }

  for (i = 0; i < 4; ++i) {
    const size_t distance = (size_t)distance_cache[i];
    size_t offset;
    size_t limit;
    size_t len;
    if (distance <= boundary || distance > distance_offset) continue;
    offset = distance_offset - distance;
    limit = source_size - offset;
    limit = limit > max_length ? max_length : limit;
    len = FindMatchLengthWithLimit(&source[offset], &data[cur_ix_masked],
                                   limit);
    if (len >= 2) {
      score_t score = BackwardReferenceScoreUsingLastDistance(len);
      if (best_score < score) {
        if (i != 0) score -= BackwardReferencePenaltyUsingLastDistance(i);
        if (best_score < score) {
          best_score = score;
          if (len > best_len) best_len = len;
          out->len = len;
          out->len_code_delta = 0;
          out->distance = distance;
          out->score = best_score;
        }
      }
    }
  }
  while (item == 0) {
    size_t offset;
    size_t distance;
    size_t limit;
    item = *chain;
    chain++;
    offset = item & 0x7FFFFFFF;
    item &= 0x80000000;
    distance = distance_offset - offset;
    limit = source_size - offset;
    limit = (limit > max_length) ? max_length : limit;
    if (distance > max_distance) continue;
    if (cur_ix_masked + best_len > ring_buffer_mask ||
        best_len >= limit ||
        data[cur_ix_masked + best_len] != source[offset + best_len]) {
      continue;
    }
    {
      const size_t len = FindMatchLengthWithLimit(&source[offset],
                                                  &data[cur_ix_masked],
                                                  limit);
      if (len >= 4) {
        score_t score = BackwardReferenceScore(len, distance);
        if (best_score < score) {
          best_score = score;
          best_len = len;
          out->len = best_len;
          out->len_code_delta = 0;
          out->distance = distance;
          out->score = best_score;
        }
      }
    }
  }
}

/* NB: when seamless dictionary-ring-buffer copies are implemented, don't forget
       to add proper guards for non-zero-BROTLI_PARAM_STREAM_OFFSET. */
static BROTLI_INLINE size_t FindAllCompoundDictionaryMatches(
    const PreparedDictionary* self, const uint8_t* BROTLI_RESTRICT data,
    const size_t ring_buffer_mask, const size_t cur_ix, const size_t min_length,
    const size_t max_length, const size_t distance_offset,
    const size_t max_distance, BackwardMatch* matches, size_t match_limit) {
  const uint32_t source_size = self->source_size;
  const uint32_t hash_bits = self->hash_bits;
  const uint32_t bucket_bits = self->bucket_bits;
  const uint32_t slot_bits = self->slot_bits;

  const uint32_t hash_shift = 64u - bucket_bits;
  const uint32_t slot_mask = (~((uint32_t)0U)) >> (32 - slot_bits);
  const uint64_t hash_mask = (~((uint64_t)0U)) >> (64 - hash_bits);

  const uint32_t* slot_offsets = (uint32_t*)(&self[1]);
  const uint16_t* heads = (uint16_t*)(&slot_offsets[1u << slot_bits]);
  const uint32_t* items = (uint32_t*)(&heads[1u << bucket_bits]);
  const uint8_t* source = NULL;

  const size_t cur_ix_masked = cur_ix & ring_buffer_mask;
  size_t best_len = min_length;
  const uint64_t h =
      (BROTLI_UNALIGNED_LOAD64LE(&data[cur_ix_masked]) & hash_mask) *
      kPreparedDictionaryHashMul64Long;
  const uint32_t key = (uint32_t)(h >> hash_shift);
  const uint32_t slot = key & slot_mask;
  const uint32_t head = heads[key];
  const uint32_t* BROTLI_RESTRICT chain = &items[slot_offsets[slot] + head];
  uint32_t item = (head == 0xFFFF) ? 1 : 0;
  size_t found = 0;

  const void* tail = (void*)&items[self->num_items];
  if (self->magic == kPreparedDictionaryMagic) {
    source = (const uint8_t*)tail;
  } else {
    /* kLeanPreparedDictionaryMagic */
    source = (const uint8_t*)BROTLI_UNALIGNED_LOAD_PTR((const uint8_t**)tail);
  }

  while (item == 0) {
    size_t offset;
    size_t distance;
    size_t limit;
    size_t len;
    item = *chain;
    chain++;
    offset = item & 0x7FFFFFFF;
    item &= 0x80000000;
    distance = distance_offset - offset;
    limit = source_size - offset;
    limit = (limit > max_length) ? max_length : limit;
    if (distance > max_distance) continue;
    if (cur_ix_masked + best_len > ring_buffer_mask ||
        best_len >= limit ||
        data[cur_ix_masked + best_len] != source[offset + best_len]) {
      continue;
    }
    len = FindMatchLengthWithLimit(
        &source[offset], &data[cur_ix_masked], limit);
    if (len > best_len) {
      best_len = len;
      InitBackwardMatch(matches++, distance, len);
      found++;
      if (found == match_limit) break;
    }
  }
  return found;
}

static BROTLI_INLINE void LookupCompoundDictionaryMatch(
    const CompoundDictionary* addon, const uint8_t* BROTLI_RESTRICT data,
    const size_t ring_buffer_mask, const int* BROTLI_RESTRICT distance_cache,
    const size_t cur_ix, const size_t max_length,
    const size_t max_ring_buffer_distance, const size_t max_distance,
    HasherSearchResult* sr) {
  size_t base_offset = max_ring_buffer_distance + 1 + addon->total_size - 1;
  size_t d;
  for (d = 0; d < addon->num_chunks; ++d) {
    /* Only one prepared dictionary type is currently supported. */
    FindCompoundDictionaryMatch(
        (const PreparedDictionary*)addon->chunks[d], data, ring_buffer_mask,
        distance_cache, cur_ix, max_length,
        base_offset - addon->chunk_offsets[d], max_distance, sr);
  }
}

static BROTLI_INLINE size_t LookupAllCompoundDictionaryMatches(
    const CompoundDictionary* addon, const uint8_t* BROTLI_RESTRICT data,
    const size_t ring_buffer_mask, const size_t cur_ix, size_t min_length,
    const size_t max_length, const size_t max_ring_buffer_distance,
    const size_t max_distance, BackwardMatch* matches,
    size_t match_limit) {
  size_t base_offset = max_ring_buffer_distance + 1 + addon->total_size - 1;
  size_t d;
  size_t total_found = 0;
  for (d = 0; d < addon->num_chunks; ++d) {
    /* Only one prepared dictionary type is currently supported. */
    total_found += FindAllCompoundDictionaryMatches(
        (const PreparedDictionary*)addon->chunks[d], data, ring_buffer_mask,
        cur_ix, min_length, max_length, base_offset - addon->chunk_offsets[d],
        max_distance, matches + total_found, match_limit - total_found);
    if (total_found == match_limit) break;
    if (total_found > 0) {
      min_length = BackwardMatchLength(&matches[total_found - 1]);
    }
  }
  return total_found;
}

}

#endif  /* BROTLI_ENC_HASH_H_ */
