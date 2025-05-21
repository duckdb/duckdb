/*
 * vergesort.h - General-purpose hybrid sort
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2015-2016 Morwenn <morwenn29@hotmail.fr>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
#ifndef VERGESORT_DETAIL_QUICKSORT_H_
#define VERGESORT_DETAIL_QUICKSORT_H_

#include <algorithm>
#include <iterator>
#include "detail/insertion_sort.h"
#include "detail/iter_sort3.h"
#include "detail/prevnext.h"

namespace duckdb_vergesort
{
namespace detail
{
    // partial application structs for partition
    template<typename T, typename Compare>
    struct partition_pivot_left
    {
        const T& pivot;
        Compare compare;

        partition_pivot_left(const T& pivot, const Compare& compare):
            pivot(pivot),
            compare(compare)
        {}

        bool operator()(const T& elem) const
        {
            return compare(elem, pivot);
        }
    };

    template<typename T, typename Compare>
    struct partition_pivot_right
    {
        const T& pivot;
        Compare compare;

        partition_pivot_right(const T& pivot, const Compare& compare):
            pivot(pivot),
            compare(compare)
        {}

        bool operator()(const T& elem) const
        {
            return not compare(pivot, elem);
        }
    };

    template<typename ForwardIterator, typename Compare>
    void quicksort(ForwardIterator first, ForwardIterator last,
                   typename std::iterator_traits<ForwardIterator>::difference_type size,
                   Compare compare)
    {
        typedef typename std::iterator_traits<ForwardIterator>::value_type value_type;
        typedef typename std::iterator_traits<ForwardIterator>::difference_type difference_type;
        using std::swap;

        // If the collection is small, fall back to insertion sort
        if (size < 32) {
            insertion_sort(first, last, compare);
            return;
        }

        // Choose pivot as median of 9
        ForwardIterator it1 = detail::next(first, size / 8);
        ForwardIterator it2 = detail::next(it1, size / 8);
        ForwardIterator it3 = detail::next(it2, size / 8);
        ForwardIterator middle = detail::next(it3, size/2 - 3*(size/8));
        ForwardIterator it4 = detail::next(middle, size / 8);
        ForwardIterator it5 = detail::next(it4, size / 8);
        ForwardIterator it6 = detail::next(it5, size / 8);
        ForwardIterator last_1 = detail::next(it6, size - size/2 - 3*(size/8) - 1);

        iter_sort3(first, it1, it2, compare);
        iter_sort3(it3, middle, it4, compare);
        iter_sort3(it5, it6, last_1, compare);
        iter_sort3(it1, middle, it4, compare);

        // Put the pivot at position prev(last) and partition
        std::iter_swap(middle, last_1);
        ForwardIterator middle1 = std::partition(
            first, last_1,
            partition_pivot_left<value_type, Compare>(*last_1, compare)
        );

        // Put the pivot in its final position and partition
        std::iter_swap(middle1, last_1);
        ForwardIterator middle2 = std::partition(
            detail::next(middle1), last,
            partition_pivot_right<value_type, Compare>(*middle1, compare)
        );

        // Recursive call: heuristic trick here: in real world cases,
        // the middle partition is more likely to be smaller than the
        // right one, so computing its size should generally be cheaper
        difference_type size_left = std::distance(first, middle1);
        difference_type size_middle = std::distance(middle1, middle2);
        difference_type size_right = size - size_left - size_middle;

        // Recurse in the smallest partition first to limit the call
        // stack overhead
        if (size_left > size_right) {
            swap(first, middle2);
            swap(middle1, last);
            swap(size_left, size_right);
        }
        quicksort(first, middle1, size_left, compare);
        quicksort(middle2, last, size_right,
                  VERGESORT_PREFER_MOVE(compare));
    }
}}

#endif // VERGESORT_DETAIL_QUICKSORT_H_
