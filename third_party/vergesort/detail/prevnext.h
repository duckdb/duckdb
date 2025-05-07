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
#ifndef VERGESORT_DETAIL_PREVNEXT_H_
#define VERGESORT_DETAIL_PREVNEXT_H_

#include <iterator>

namespace duckdb_vergesort
{
namespace detail
{
    template<typename BidirectionalIterator>
    BidirectionalIterator
    prev(BidirectionalIterator it)
    {
        return --it;
    }

    template<typename BidirectionalIterator>
    BidirectionalIterator
    prev(BidirectionalIterator it,
         typename std::iterator_traits<BidirectionalIterator>::difference_type n)
    {
        std::advance(it, -n);
        return it;
    }

    template<typename ForwardIterator>
    ForwardIterator
    next(ForwardIterator it)
    {
        return ++it;
    }

    template<typename ForwardIterator>
    ForwardIterator
    next(ForwardIterator it,
         typename std::iterator_traits<ForwardIterator>::difference_type n)
    {
        std::advance(it, n);
        return it;
    }
}}

#endif // VERGESORT_DETAIL_PREVNEXT_H_
