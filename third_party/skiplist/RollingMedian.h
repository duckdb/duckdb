#ifndef __SkipList__RollingMedian__
#define __SkipList__RollingMedian__

/**
 * @file
 *
 * Project: skiplist
 *
 * Rolling Median.
 *
 * Created by Paul Ross on 18/12/2015.
 *
 * Copyright (c) 2015-2023 Paul Ross. All rights reserved.
 *
 * @code
 * MIT License
 *
 * Copyright (c) 2015-2023 Paul Ross
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 * @endcode
 */

#include <stdlib.h>

#include "SkipList.h"

namespace OrderedStructs {
    /**
     * @brief Namespace for the C++ Rolling Median.
     */
    namespace RollingMedian {

/**
 * Error codes.
 */
        enum RollingMedianResult {
            ROLLING_MEDIAN_SUCCESS = 0,
            ROLLING_MEDIAN_SOURCE_STRIDE,
            ROLLING_MEDIAN_DESTINATION_STRIDE,
            ROLLING_MEDIAN_WIN_LENGTH,
        };

/**
 * Return an error code.
 */
#define ROLLING_MEDIAN_ERROR_CHECK                      \
    do {                                                \
        if (src_stride == 0) {                          \
            return ROLLING_MEDIAN_SOURCE_STRIDE;        \
        }                                               \
        if (dest_stride == 0) {                         \
            return ROLLING_MEDIAN_DESTINATION_STRIDE;   \
        }                                               \
        if (win_length == 0) {                          \
            return ROLLING_MEDIAN_WIN_LENGTH;           \
        }                                               \
    } while (0)

/* Helpers for the destination memory area.
 * Iterating through the destination to see the replaced values is done thus:
 *
 * for (int i = 0;
 *      i < RollingMedian::dest_size(COUNT, WIN_LENGTH, DEST_STRIDE);
 *      i += DEST_STRIDE) {
 *    ...
 * }
 */

/**
 * Returns the size of the destination array for a rolling median on an array
 * of count values with a window of win_length and a destination stride.
 *
 * @param count Number of input values.
 * @param win_length Window length.
 * @return Number of destination values.
 */
        size_t dest_count(size_t count, size_t win_length) {
            return 1 + count - win_length;
        }

/**
 * Returns the size of the destination array for a rolling median on an array
 * of count values with a window of win_length and a destination stride.
 *
 * @param count Number of input values.
 * @param win_length Window length.
 * @param dest_stride The destination stride given a 2D array.
 * @return Size of destination array.
 */
        size_t dest_size(size_t count,
                         size_t win_length,
                         size_t dest_stride) {
            return dest_count(count, win_length) * dest_stride;
        }

/**
 * Rolling median where only the odd mid-index is considered.
 * If the win_length is even then (win_length - 1) / 2 value is used.
 * See even_odd_index() for a different treatment of even lengths.
 * This is valid for all types T.
 * It is up to the caller to ensure that there is enough space in dest for
 * the results, use dest_size() for this.
 *
 * @tparam T Type of the value(s).
 * @param src Source array of values.
 * @param src_stride Source stride for 2D arrays.
 * @param count Number of input values.
 * @param win_length Window length.
 * @param dest The destination array.
 * @param dest_stride The destination stride given a 2D array.
 * @return The result of the Rolling Median operation as a RollingMedianResult enum.
 */
        template<typename T>
        RollingMedianResult odd_index(const T *src, size_t src_stride,
                                      size_t count, size_t win_length,
                                      T *dest, size_t dest_stride) {
            SkipList::HeadNode<T> sl;
            const T *tail = src;

            ROLLING_MEDIAN_ERROR_CHECK;
            for (size_t i = 0; i < count; ++i) {
                sl.insert(*src);
                if (i + 1 >= win_length) {
                    *dest = sl.at(win_length / 2);
                    dest += dest_stride;
                    sl.remove(*tail);
                    tail += src_stride;
                }
                src += src_stride;
            }
            return ROLLING_MEDIAN_SUCCESS;
        }

/*
 */
/**
 * Rolling median where the mean of adjacent values is used
 * when the window size is even length.
 * This requires T / 2 to be meaningful.
 * It is up to the caller to ensure that there is enough space in dest for
 * the results, use dest_size() for this.
 *
 * @tparam T Type of the value(s).
 * @param src Source array of values.
 * @param src_stride Source stride for 2D arrays.
 * @param count Number of input values.
 * @param win_length Window length.
 * @param dest The destination array.
 * @param dest_stride The destination stride given a 2D array.
 * @return The result of the Rolling Median operation as a RollingMedianResult enum.
 */
        template<typename T>
        RollingMedianResult even_odd_index(const T *src, size_t src_stride,
                                           size_t count, size_t win_length,
                                           T *dest, size_t dest_stride) {
            if (win_length % 2 == 1) {
                return odd_index(src, src_stride,
                                 count, win_length,
                                 dest, dest_stride);
            } else {
                ROLLING_MEDIAN_ERROR_CHECK;
                SkipList::HeadNode<T> sl;
                std::vector<T> buffer;

                const T *tail = src;
                for (size_t i = 0; i < count; ++i) {
                    sl.insert(*src);
                    if (i + 1 >= win_length) {
                        sl.at((win_length - 1) / 2, 2, buffer);
                        assert(buffer.size() == 2);
                        *dest = buffer[0] / 2 + buffer[1] / 2;
                        dest += dest_stride;
                        sl.remove(*tail);
                        tail += src_stride;
                    }
                    src += src_stride;
                }
            }
            return ROLLING_MEDIAN_SUCCESS;
        }

    } // namespace RollingMedian
} // namespace OrderedStructs

#endif /* defined(__SkipList__RollingMedian__) */
