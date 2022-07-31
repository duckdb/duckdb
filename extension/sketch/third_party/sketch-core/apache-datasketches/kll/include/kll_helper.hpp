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

#ifndef KLL_HELPER_HPP_
#define KLL_HELPER_HPP_

#include <random>
#include <stdexcept>

namespace datasketches {

#ifdef KLL_VALIDATION
extern uint32_t kll_next_offset;
#endif

// 0 <= power <= 30
static const uint64_t powers_of_three[] =  {1, 3, 9, 27, 81, 243, 729, 2187, 6561, 19683, 59049, 177147, 531441,
1594323, 4782969, 14348907, 43046721, 129140163, 387420489, 1162261467,
3486784401, 10460353203, 31381059609, 94143178827, 282429536481,
847288609443, 2541865828329, 7625597484987, 22876792454961, 68630377364883,
205891132094649};

class kll_helper {
  public:
    static inline bool is_even(uint32_t value);
    static inline bool is_odd(uint32_t value);
    static inline uint8_t floor_of_log2_of_fraction(uint64_t numer, uint64_t denom);
    static inline uint8_t ub_on_num_levels(uint64_t n);
    static inline uint32_t compute_total_capacity(uint16_t k, uint8_t m, uint8_t num_levels);
    static inline uint16_t level_capacity(uint16_t k, uint8_t numLevels, uint8_t height, uint8_t min_wid);
    static inline uint16_t int_cap_aux(uint16_t k, uint8_t depth);
    static inline uint16_t int_cap_aux_aux(uint16_t k, uint8_t depth);
    static inline uint64_t sum_the_sample_weights(uint8_t num_levels, const uint32_t* levels);

    /*
     * This version is for floating point types
     * Checks the sequential validity of the given array of values.
     * They must be unique, monotonically increasing and not NaN.
     */
    template <typename T, typename C>
    static typename std::enable_if<std::is_floating_point<T>::value, void>::type
    validate_values(const T* values, uint32_t size) {
      for (uint32_t i = 0; i < size ; i++) {
        if (std::isnan(values[i])) {
          throw std::invalid_argument("Values must not be NaN");
        }
        if ((i < (size - 1)) && !(C()(values[i], values[i + 1]))) {
          throw std::invalid_argument("Values must be unique and monotonically increasing");
        }
      }
    }
    /*
     * This version is for non-floating point types
     * Checks the sequential validity of the given array of values.
     * They must be unique and monotonically increasing.
     */
    template <typename T, typename C>
    static typename std::enable_if<!std::is_floating_point<T>::value, void>::type
    validate_values(const T* values, uint32_t size) {
      for (uint32_t i = 0; i < size ; i++) {
        if ((i < (size - 1)) && !(C()(values[i], values[i + 1]))) {
          throw std::invalid_argument("Values must be unique and monotonically increasing");
        }
      }
    }

    template <typename T>
    static void randomly_halve_down(T* buf, uint32_t start, uint32_t length);

    template <typename T>
    static void randomly_halve_up(T* buf, uint32_t start, uint32_t length);

    // this version moves objects within the same buffer
    // assumes that destination has initialized objects
    // does not destroy the originals after the move
    template <typename T, typename C>
    static void merge_sorted_arrays(T* buf, uint32_t start_a, uint32_t len_a, uint32_t start_b, uint32_t len_b, uint32_t start_c);

    // this version is to merge from two different buffers into a third buffer
    // initializes objects is the destination buffer
    // moves objects from buf_a and destroys the originals
    // copies objects from buf_b
    template <typename T, typename C>
    static void merge_sorted_arrays(const T* buf_a, uint32_t start_a, uint32_t len_a, const T* buf_b, uint32_t start_b, uint32_t len_b, T* buf_c, uint32_t start_c);

    struct compress_result {
      uint8_t final_num_levels;
      uint32_t final_capacity;
      uint32_t final_num_items;
    };

    /*
     * Here is what we do for each level:
     * If it does not need to be compacted, then simply copy it over.
     *
     * Otherwise, it does need to be compacted, so...
     *   Copy zero or one guy over.
     *   If the level above is empty, halve up.
     *   Else the level above is nonempty, so...
     *        halve down, then merge up.
     *   Adjust the boundaries of the level above.
     *
     * It can be proved that general_compress returns a sketch that satisfies the space constraints
     * no matter how much data is passed in.
     * All levels except for level zero must be sorted before calling this, and will still be
     * sorted afterwards.
     * Level zero is not required to be sorted before, and may not be sorted afterwards.
     */
    template <typename T, typename C>
    static compress_result general_compress(uint16_t k, uint8_t m, uint8_t num_levels_in, T* items,
            uint32_t* in_levels, uint32_t* out_levels, bool is_level_zero_sorted);

    template<typename T>
    static void copy_construct(const T* src, size_t src_first, size_t src_last, T* dst, size_t dst_first);

    template<typename T>
    static void move_construct(T* src, size_t src_first, size_t src_last, T* dst, size_t dst_first, bool destroy);

#ifdef KLL_VALIDATION
  private:

    static inline uint32_t deterministic_offset();
#endif

};

} /* namespace datasketches */

#include "kll_helper_impl.hpp"

#endif // KLL_HELPER_HPP_
