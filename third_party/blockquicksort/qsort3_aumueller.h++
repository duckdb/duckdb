/******************************************************************************
 * qsort3_aumueller.h 
 *
 * from src/algorithms/qsort3.h (available at http://eiche.theoinf.tu-ilmenau.de/quicksort-experiments/) 
 * modified (added sort()) by Armin Weiß <armin.weiss@fmi.uni-stuttgart.de>
 *
 * Sorting using Quicksort, three pivots.  
 * Pretty much the algorithm of Kushagra et al. (2014) at ALENEX.
 *
 ******************************************************************************
 * Copyright (C) 2014 Martin Aumueller <martin.aumueller@tu-ilmenau.de>
 * Copyright (C) 2014 Timo Bingmann <tb@panthema.net>
 *
 * This program is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program.  If not, see <http://www.gnu.org/licenses/>.
 *****************************************************************************/

#include <algorithm>
#include "rotations.h"
#include "inssort.h"

namespace qsort3_aumueller {

template <typename Iterator>
void three_pivot(Iterator left, Iterator right)
{
    typedef typename std::iterator_traits<Iterator>::value_type value_type;

    using rotations::swap;

//    if (left >= right) return;
    if (left + 23 > right)
    {
		InsertionSort(left, right - left + 1);
		return;
    }
	

    Iterator i = left + 2;
    Iterator j = i;

    Iterator k = right - 1;
    Iterator l = k;

    if (*left > *(left+1))
        swap(*left, *(left+1));

    if (*left > *right)
    {
        swap(*left, *(left+1));
        swap(*left, *right);
    }
    else if (*(left + 1) > *right)
    {
        swap(*(left+1), *right);
    }

    value_type p = *left;
    value_type q = *(left+1);
    value_type r = *right;

    while (j <= k)
    {
        while (*j < q)
        {
            if (*j < p) {
                swap(*i, *j);
                i++;
            }
            j++;
        }

        while (*k > q)
        {
            if (*k > r) {
                swap(*k, *l);
                l--;
            }
            k--;
        }

        if (j <= k)
        {
            if (*j > r)
            {
                if (*k < p)
                {
		    rotations::rotate4(*j, *i, *k, *l);
                    i++;
                }
                else
                {
		    rotations::rotate3(*j, *k, *l);
                }
                l--;
            }
            else
            {
                if (*k < p)
                {
		    rotations::rotate3(*j, *i, *k);
                    i++;
                }
                else
                {
                    swap(*j, *k);
                }
            }
            j++; k--;
        }
    }

    rotations::rotate3(*(left + 1), *(i - 1), *(j - 1));

    swap(*left, *(i - 2));
    swap(*right, *(l + 1));

    three_pivot(left, i - 3);
    three_pivot(i - 1, j - 2);
    three_pivot(j, l);
    three_pivot(l + 2, right);
}

template <typename Iterator, typename Comparator>
void sort(Iterator begin, Iterator end, Comparator less)
{
    three_pivot(begin, end - 1);
}

} // namespace qsort3_aumueller
