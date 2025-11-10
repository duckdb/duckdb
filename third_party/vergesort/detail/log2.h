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
#ifndef VERGESORT_DETAIL_LOG2_H_
#define VERGESORT_DETAIL_LOG2_H_

namespace duckdb_vergesort
{
namespace detail
{
    // Returns floor(log2(n)), assumes n > 0
    template<typename Integer>
    Integer log2(Integer n)
    {
        Integer log = 0;
        while (n >>= 1) {
            ++log;
        }
        return log;
    }
}}

#endif // VERGESORT_DETAIL_LOG2_H_
