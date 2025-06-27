/*
    pdqsort.h - Pattern-defeating quicksort.

    Copyright (c) 2015 Orson Peters
    Modified by Morwenn in 2015-2016 to use in vergesort

    This software is provided 'as-is', without any express or implied warranty. In no event will the
    authors be held liable for any damages arising from the use of this software.

    Permission is granted to anyone to use this software for any purpose, including commercial
    applications, and to alter it and redistribute it freely, subject to the following restrictions:

    1. The origin of this software must not be misrepresented; you must not claim that you wrote the
       original software. If you use this software in a product, an acknowledgment in the product
       documentation would be appreciated but is not required.

    2. Altered source versions must be plainly marked as such, and must not be misrepresented as
       being the original software.

    3. This notice may not be removed or altered from any source distribution.
*/
#ifndef VERGESORT_DETAIL_INSERTION_SORT_H_
#define VERGESORT_DETAIL_INSERTION_SORT_H_

#include <iterator>
#include "detail/prevnext.h"

namespace duckdb_vergesort
{
namespace detail
{
    // Sorts [begin, end) using insertion sort with the given comparison function.
    template<typename BidirectionalIterator, typename Compare>
    void insertion_sort(BidirectionalIterator begin, BidirectionalIterator end, Compare comp) {
        typedef typename std::iterator_traits<BidirectionalIterator>::value_type T;
        if (begin == end) return;

        for (BidirectionalIterator cur = detail::next(begin) ; cur != end ; ++cur) {
            BidirectionalIterator sift = cur;
            BidirectionalIterator sift_1 = detail::prev(cur);

            // Compare first so we can avoid 2 moves for an element already positioned correctly.
            if (comp(*sift, *sift_1)) {
                T tmp = VERGESORT_PREFER_MOVE(*sift);

                do {
                    *sift-- = VERGESORT_PREFER_MOVE(*sift_1);
                } while (sift != begin && comp(tmp, *--sift_1));

                *sift = VERGESORT_PREFER_MOVE(tmp);
            }
        }
    }
}}

#endif // VERGESORT_DETAIL_INSERTION_SORT_H_
