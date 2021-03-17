/******************************************************************************
* partition.h
*
* Different partition algorithms with interfaces for different pivot selection strategies: 
* 4x block partition (simple, loops unrolled, loops unrolled + duplicate check, Hoare finish),
* Lomuto partitioner by Elmasry, Katajainen and Stenmark, and Hoare parititioner
*
******************************************************************************
* Copyright (C) 2016 Stefan Edelkamp <edelkamp@tzi.de>
* Copyright (C) 2016 Armin Wei� <armin.weiss@fmi.uni-stuttgart.de>
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


#pragma once
#include <iostream>
#include <fstream>
#include <vector>
#include <queue>
#include <algorithm>
#include <string>
#include <stdlib.h>
#include <ctime>
#include <cmath>
#include <assert.h>
#include <functional>
#include "rotations.h"
#include "median.h"
#ifndef BLOCKSIZE
#define BLOCKSIZE 128
#endif
#ifndef PIVOTSAMPLESIZE
#define PIVOTSAMPLESIZE 23
#endif
#ifndef MO3_THRESH
#define MO3_THRESH (PIVOTSAMPLESIZE*PIVOTSAMPLESIZE)
#endif


namespace partition {

	//agrees with median::sort_pair
	template<typename iter, typename Compare>
	inline void leanswap(iter i1, iter i2, Compare less) {
		using t = typename std::iterator_traits<iter>::value_type;
		bool smaller = less(*i2, *i1);
		t temp = std::move(smaller ? *i1 : temp);
		*i1 = std::move(smaller ? *i2 : *i1);
		*i2 = std::move(smaller ? temp : *i2);
	}

	//pivot choice for Tuned Quicksort by Elmasry, Katajainen, and Stenmark
	template<typename iter, typename Compare>
	inline iter pivot(iter p, iter r, Compare less) {
		iter last = r - 1;
		iter q = p + (r - p) / 2;
		iter v = less(*q, *p) ? p : q;
		v = less(*v, *last) ? last : v;
		iter i = (v == p) ? q : p;
		iter j = (v == last) ? q : last;
		return less(*j, *i) ? i : j;
	}

	//Tuned Quicksort by Elmasry, Katajainen, and Stenmark
	//Code from http://www.diku.dk/~jyrki/Myris/Kat2014S.html
	template<typename iter, typename Compare>
	struct Lomuto_partition {
		static inline iter partition(iter begin, iter end, Compare less) {
			typedef typename std::iterator_traits<iter>::difference_type index;
			typedef typename std::iterator_traits<iter>::value_type t;
			iter q = pivot(begin, end, less);
			t v = std::move(*q);
			iter first = begin;
			*q = std::move(*first);
			q = first + 1;
			while (q < end) {
				t x = std::move(*q);
				bool smaller = less(x, v);
				begin += smaller; // smaller = 1 ? begin++ : begin
				index delta = smaller * (q - begin);
				iter s = begin + delta; // smaller = 1 => s = q : s = begin
				iter y = q - delta; // smaller = 1 => y = begin : y = q
				*s = std::move(*begin);
				*y = std::move(x);
				++q;
			}
			*first = std::move(*begin);
			*begin = std::move(v);
			return begin;
		}
	};


	template<typename iter, typename Compare>
	inline iter hoare_block_partition_simple(iter begin, iter end, iter pivot_position, Compare less) {
		typedef typename std::iterator_traits<iter>::difference_type index;
		index indexL[BLOCKSIZE], indexR[BLOCKSIZE];
		
		iter last = end - 1;
		std::iter_swap(pivot_position, last);
		const typename std::iterator_traits<iter>::value_type & pivot = *last;
		pivot_position = last;
		last--;

		int num_left = 0;
		int num_right = 0;
		int start_left = 0;
		int start_right = 0;
		int num;
		//main loop
		while (last - begin + 1 > 2 * BLOCKSIZE)
		{
			//Compare and store in buffers
			if (num_left == 0) {
				start_left = 0;
				for (index j = 0; j < BLOCKSIZE; j++) {
					indexL[num_left] = j;
					num_left += (!(less(begin[j], pivot)));				
				}
			}
			if (num_right == 0) {
				start_right = 0;
				for (index j = 0; j < BLOCKSIZE; j++) {
					indexR[num_right] = j;
					num_right += !(less(pivot, *(last - j)));				
				}
			}
			//rearrange elements
			num = std::min(num_left, num_right);
			for (int j = 0; j < num; j++)
				std::iter_swap(begin + indexL[start_left + j], last - indexR[start_right + j]);

			num_left -= num;
			num_right -= num;
			start_left += num;
			start_right += num;
			begin += (num_left == 0) ? BLOCKSIZE : 0;
			last -= (num_right == 0) ? BLOCKSIZE : 0;

		}//end main loop

		//Compare and store in buffers final iteration
		index shiftR = 0, shiftL = 0;
		if (num_right == 0 && num_left == 0) {	//for small arrays or in the unlikely case that both buffers are empty
			shiftL = ((last - begin) + 1) / 2;
			shiftR = (last - begin) + 1 - shiftL;
			assert(shiftL >= 0); assert(shiftL <= BLOCKSIZE);
			assert(shiftR >= 0); assert(shiftR <= BLOCKSIZE);
			start_left = 0; start_right = 0;
			for (index j = 0; j < shiftL; j++) {
				indexL[num_left] = j;
				num_left += (!less(begin[j], pivot));
				indexR[num_right] = j;
				num_right += !less(pivot, *(last - j));
			}
			if (shiftL < shiftR)
			{
				assert(shiftL + 1 == shiftR);
				indexR[num_right] = shiftR - 1;
				num_right += !less(pivot, *(last - shiftR + 1));
			}
		}
		else if (num_right != 0) {
			shiftL = (last - begin) - BLOCKSIZE + 1;
			shiftR = BLOCKSIZE;
			assert(shiftL >= 0); assert(shiftL <= BLOCKSIZE); assert(num_left == 0);
			start_left = 0;
			for (index j = 0; j < shiftL; j++) {
				indexL[num_left] = j;
				num_left += (!less(begin[j], pivot));
			}
		}
		else {
			shiftL = BLOCKSIZE;
			shiftR = (last - begin) - BLOCKSIZE + 1;
			assert(shiftR >= 0); assert(shiftR <= BLOCKSIZE); assert(num_right == 0);
			start_right = 0;
			for (index j = 0; j < shiftR; j++) {
				indexR[num_right] = j;
				num_right += !(less(pivot, *(last - j)));
			}
		}

		//rearrange final iteration
		num = std::min(num_left, num_right);
		for (int j = 0; j < num; j++)
			std::iter_swap(begin + indexL[start_left + j], last - indexR[start_right + j]);

		num_left -= num;
		num_right -= num;
		start_left += num;
		start_right += num;
		begin += (num_left == 0) ? shiftL : 0;
		last -= (num_right == 0) ? shiftR : 0;			
		//end final iteration


		//rearrange elements remaining in buffer
		if (num_left != 0)
		{
			
			assert(num_right == 0);
			int lowerI = start_left + num_left - 1;
			index upper = last - begin;
			//search first element to be swapped
			while (lowerI >= start_left && indexL[lowerI] == upper) {
				upper--; lowerI--;
			}
			while (lowerI >= start_left)
				std::iter_swap(begin + upper--, begin + indexL[lowerI--]);

			std::iter_swap(pivot_position, begin + upper + 1); // fetch the pivot 
			return begin + upper + 1;
		}
		else if (num_right != 0) {
			assert(num_left == 0);
			int lowerI = start_right + num_right - 1;
			index upper = last - begin;
			//search first element to be swapped
			while (lowerI >= start_right && indexR[lowerI] == upper) {
				upper--; lowerI--;
			}
			
			while (lowerI >= start_right)
				std::iter_swap(last - upper--, last - indexR[lowerI--]);

			std::iter_swap(pivot_position, last - upper);// fetch the pivot 
			return last - upper;
		}
		else { //no remaining elements
			assert(last + 1 == begin);
			std::iter_swap(pivot_position, begin);// fetch the pivot 
			return begin;
		}
	}



	template< typename iter, typename Compare>
	inline iter hoare_block_partition_unroll_loop(iter begin, iter end, iter pivot_pos, Compare less ) {
		using t = typename std::iterator_traits<iter>::value_type;
		iter last = end - 1;
		int indexL[BLOCKSIZE], indexR[BLOCKSIZE];

		t pivot = std::move(*pivot_pos);
		*pivot_pos = std::move(*last);
		iter hole = last;
		t temp;
		last--;

		int num_left = 0;
		int num_right = 0;
		int start_left = 0;
		int start_right = 0;

		int j;
		int num;
		//main loop
		while (last - begin + 1 > 2 * BLOCKSIZE)
		{
			//Compare and store in buffers
			if (num_left == 0) {
				start_left = 0;
				for (j = 0; j < BLOCKSIZE; ) {
					indexL[num_left] = j;
					num_left += (!(less(begin[j], pivot)));
					j++;
					indexL[num_left] = j;
					num_left += (!(less(begin[j], pivot)));
					j++;
					indexL[num_left] = j;
					num_left += (!(less(begin[j], pivot)));
					j++;
					indexL[num_left] = j;
					num_left += (!(less(begin[j], pivot)));
					j++;
				}
			}
			if (num_right == 0) {
				start_right = 0;
				for (j = 0; j < BLOCKSIZE; ) {
					indexR[num_right] = j;
					num_right += !(less(pivot, *(last - j)));
					j++;
					indexR[num_right] = j;
					num_right += !(less(pivot, *(last - j)));
					j++;
					indexR[num_right] = j;
					num_right += !(less(pivot, *(last - j)));
					j++;
					indexR[num_right] = j;
					num_right += !(less(pivot, *(last - j)));
					j++;
				}
			}
			//rearrange elements
			num = std::min(num_left, num_right);
			if (num != 0)
			{
				*hole = std::move(*(begin + indexL[start_left]));
				*(begin + indexL[start_left]) = std::move(*(last - indexR[start_right]));
				for (j = 1; j < num; j++)
				{
					*(last - indexR[start_right + j - 1]) = std::move(*(begin + indexL[start_left + j]));
					*(begin + indexL[start_left + j]) = std::move(*(last - indexR[start_right + j]));
				}
				hole = (last - indexR[start_right + num - 1]);
			}
			num_left -= num;
			num_right -= num;
			start_left += num;
			start_right += num;
			begin += (num_left == 0) ? BLOCKSIZE : 0;
			last -= (num_right == 0) ? BLOCKSIZE : 0;
		}//end main loop

		 //Compare and store in buffers final iteration
		int shiftR = 0, shiftL = 0;
		if (num_right == 0 && num_left == 0) {	//for small arrays or in the unlikely case that both buffers are empty
			shiftL = (int)((last - begin) + 1) / 2;
			shiftR = (int)(last - begin) + 1 - shiftL;
			assert(shiftL >= 0); assert(shiftL <= BLOCKSIZE);
			assert(shiftR >= 0); assert(shiftR <= BLOCKSIZE);
			start_left = 0; start_right = 0;
			for (j = 0; j < shiftL; j++) {
				indexL[num_left] = j;
				num_left += (!less(begin[j], pivot));
				indexR[num_right] = j;
				num_right += !less(pivot, *(last - j));
			}
			if (shiftL < shiftR)
			{
				assert(shiftL + 1 == shiftR);
				indexR[num_right] = j;
				num_right += !less(pivot, *(last - j));
			}
		}
		else if ((last - begin) + 1 - BLOCKSIZE <= 2 * BLOCKSIZE - (start_right + num_right + start_left + num_left) && (num_right + num_left) < BLOCKSIZE / 3) {
			int upper_right = start_right + num_right;
			int upper_left = start_left + num_left;
			assert((last - begin) - BLOCKSIZE + 1 > 0);
			shiftL = (int)(((last - begin) + 1 - BLOCKSIZE) / 2); // +2*(num_right + num_left)  //- num_left
			shiftR = (int)(last - begin) - BLOCKSIZE + 1 - shiftL;
			if (shiftL > BLOCKSIZE - upper_left)
			{
				shiftR += shiftL - (BLOCKSIZE - upper_left);
				shiftL = BLOCKSIZE - upper_left;
			}
			else if (shiftL < 0)
			{
				shiftR -= shiftL;
				shiftL = 0;
			}
			if (shiftR > BLOCKSIZE - upper_right)
			{
				shiftL += shiftR - (BLOCKSIZE - upper_right);
				shiftR = BLOCKSIZE - upper_right;
			}
			else if (shiftR < 0)
			{
				shiftL -= shiftR;
				shiftR = 0;
			}

			assert(shiftL + shiftR + BLOCKSIZE == (last - begin) + 1);
			assert(shiftL >= 0); assert(shiftL <= BLOCKSIZE - upper_left);
			assert(shiftR >= 0); assert(shiftR <= BLOCKSIZE - upper_right);

			int j_L = 0;
			int j_R = 0;
			if (num_left != 0) {
				shiftL += BLOCKSIZE;
				j_L = BLOCKSIZE;
			}
			if (num_right != 0) {
				shiftR += BLOCKSIZE;
				j_R = BLOCKSIZE;
			}

			for (; j_L < shiftL; j_L++) {
				indexL[upper_left] = j_L;
				upper_left += (!less(begin[j_L], pivot));
			}
			num_left = upper_left - start_left;

			for (; j_R < shiftR; j_R++) {
				indexR[upper_right] = j_R;
				upper_right += !(less(pivot, *(last - j_R)));
			}
			num_right = upper_right - start_right;
		}
		else if (num_right != 0) {
			shiftL = (int)(last - begin) - BLOCKSIZE + 1;
			shiftR = BLOCKSIZE;
			assert(shiftL >= 0); assert(shiftL <= BLOCKSIZE); assert(num_left == 0);
			start_left = 0;
			for (j = 0; j < shiftL; j++) {
				indexL[num_left] = j;
				num_left += (!less(begin[j], pivot));
			}
		}
		else {
			shiftL = BLOCKSIZE;
			shiftR = (int)(last - begin) - BLOCKSIZE + 1;
			assert(shiftR >= 0); assert(shiftR <= BLOCKSIZE); assert(num_right == 0);
			start_right = 0;
			for (j = 0; j < shiftR; j++) {
				indexR[num_right] = j;
				num_right += !(less(pivot, *(last - j)));
			}
		}

		//rearrange final iteration
		num = std::min(num_left, num_right);
		if (num != 0)
		{
			*hole = std::move(*(begin + indexL[start_left]));
			*(begin + indexL[start_left]) = std::move(*(last - indexR[start_right]));
			for (j = 1; j < num; j++)
			{
				*(last - indexR[start_right + j - 1]) = std::move(*(begin + indexL[start_left + j]));
				*(begin + indexL[start_left + j]) = std::move(*(last - indexR[start_right + j]));
			}
			hole = (last - indexR[start_right + num - 1]);
		}
		num_left -= num;
		num_right -= num;
		start_left += num;
		start_right += num;

		if (num_left == 0)
			begin += shiftL;
		if (num_right == 0)
			last -= shiftR;

		/*	std::cout << "Partition check" << std::endl;
		for (iter it = bbegin; it != begin;  it++)
		if(*it > pivot)
		std::cout << "vorne" << begin - it << ", " << it->value() << std::endl;
		for (iter it = last + 1; it != eend ;  it++)
		if (*it < pivot)
		std::cout <<"hinten" << it-last << ", " << it->value() << std::endl;
		;*/

		//rearrange remaining elements
		if (num_left != 0)
		{
			assert(num_right == 0);
			int lowerI = start_left + num_left - 1;
			int upper = (int)(last - begin);
			while (lowerI >= start_left && indexL[lowerI] == upper)
			{
				upper--; lowerI--;
			}
			temp = std::move(*(begin + upper));
			while (lowerI >= start_left)
			{
				*(begin + upper) = std::move(*(begin + indexL[lowerI]));
				*(begin + indexL[lowerI]) = std::move(*(begin + (--upper)));
				lowerI--;
			}
			*(begin + upper) = std::move(temp);
			*hole = std::move(*(begin + upper + 1));

			*(begin + upper + 1) = std::move(pivot); // fetch the pivot 
			return begin + upper + 1;
			
		}
		else if (num_right != 0) {
			assert(num_left == 0);
			int lowerI = start_right + num_right - 1;
			int upper = (int)(last - begin);
			while (lowerI >= start_right && indexR[lowerI] == upper)
			{
				upper--; lowerI--;
			}
			*hole = std::move(*(last - upper));
			while (lowerI >= start_right)
			{
				*(last - upper) = std::move(*(last - indexR[lowerI]));
				*(last - indexR[lowerI--]) = std::move(*(last - (--upper)));
			}
			
			*(last - upper) = std::move(pivot); // fetch the pivot 
			return last - upper;
		
		}
		else { //no remaining elements
			assert(last + 1 == begin);
			*hole = std::move(*begin);
			*begin = std::move(pivot); // fetch the pivot 
			return begin;
		}
	}

	// with check for elements equal to pivot -- requires that *(begin - 1) <= *pivot_pos <= *end   (in particular these positions must exist)
	template< typename iter, typename Compare>
	inline iter hoare_block_partition_unroll_loop(iter begin, iter end, iter pivot_pos, Compare less, int & pivot_length) {
		using t = typename std::iterator_traits<iter>::value_type;
		using index = typename std::iterator_traits<iter>::difference_type;
		iter last = end - 1;
		iter temp_begin = begin;
		int indexL[BLOCKSIZE], indexR[BLOCKSIZE];

		bool double_pivot_check = ((!less(*pivot_pos, *end)) || (!(less(*(begin - 1), *pivot_pos))));
		pivot_length = 1;

		t pivot = std::move(*pivot_pos);
		*pivot_pos = std::move(*last);
		iter hole = last;
		t temp;
		last--;

		int num_left = 0;
		int num_right = 0;
		int start_left = 0;
		int start_right = 0;
		int j;
		int num;

		bool small_array = (last - begin + 1 <= 2 * BLOCKSIZE) && ((last - begin) > 48);
		//main loop
		while (last - begin + 1 > 2 * BLOCKSIZE)
		{
			//Compare and store in buffers
			if (num_left == 0) {
				start_left = 0;
				for (j = 0; j < BLOCKSIZE; ) {
					indexL[num_left] = j;
					num_left += (!(less(begin[j], pivot)));
					j++;
					indexL[num_left] = j;
					num_left += (!(less(begin[j], pivot)));
					j++;
					indexL[num_left] = j;
					num_left += (!(less(begin[j], pivot)));
					j++;
					indexL[num_left] = j;
					num_left += (!(less(begin[j], pivot)));
					j++;
				}
			}
			if (num_right == 0) {
				start_right = 0;
				for (j = 0; j < BLOCKSIZE; ) {
					indexR[num_right] = j;
					num_right += !(less(pivot, *(last - j)));
					j++;
					indexR[num_right] = j;
					num_right += !(less(pivot, *(last - j)));
					j++;
					indexR[num_right] = j;
					num_right += !(less(pivot, *(last - j)));
					j++;
					indexR[num_right] = j;
					num_right += !(less(pivot, *(last - j)));
					j++;
				}
			}
			//rearrange elements
			num = std::min(num_left, num_right);
			if (num != 0)
			{
				*hole = std::move(*(begin + indexL[start_left]));
				*(begin + indexL[start_left]) = std::move(*(last - indexR[start_right]));
				for (j = 1; j < num; j++)
				{
					*(last - indexR[start_right + j - 1]) = std::move(*(begin + indexL[start_left + j]));
					*(begin + indexL[start_left + j]) = std::move(*(last - indexR[start_right + j]));
				}
				hole = (last - indexR[start_right + num - 1]);
			}
			num_left -= num;
			num_right -= num;
			start_left += num;
			start_right += num;
			begin += (num_left == 0) ? BLOCKSIZE : 0;
			last -= (num_right == 0) ? BLOCKSIZE : 0;
		}//end main loop

		if (num_left == 0) start_left = 0;
		if (num_right == 0) start_right = 0;

		 //Compare and store in buffers final iteration
		int shiftR = 0, shiftL = 0;
		if (num_right == 0 && num_left == 0) {	//for small arrays or in the unlikely case that both buffers are empty
			shiftL = (int)((last - begin) + 1) / 2;
			shiftR = (int)(last - begin) + 1 - shiftL;
			assert(shiftL >= 0); assert(shiftL <= BLOCKSIZE);
			assert(shiftR >= 0); assert(shiftR <= BLOCKSIZE);
			start_left = 0; start_right = 0;
			for (j = 0; j < shiftL; j++) {
				indexL[num_left] = j;
				num_left += (!less(begin[j], pivot));
				indexR[num_right] = j;
				num_right += !less(pivot, *(last - j));
			}
			if (shiftL < shiftR)
			{
				assert(shiftL + 1 == shiftR);
				indexR[num_right] = j;
				num_right += !less(pivot, *(last - j));
			}
		}
		else if (num_right != 0) {
			shiftL = (int)(last - begin) - BLOCKSIZE + 1;
			shiftR = BLOCKSIZE;
			assert(shiftL >= 0); assert(shiftL <= BLOCKSIZE); assert(num_left == 0);
			start_left = 0;
			for (j = 0; j < shiftL; j++) {
				indexL[num_left] = j;
				num_left += (!less(begin[j], pivot));
			}
		}
		else {
			shiftL = BLOCKSIZE;
			shiftR = (int)(last - begin) - BLOCKSIZE + 1;
			assert(shiftR >= 0); assert(shiftR <= BLOCKSIZE); assert(num_right == 0);
			start_right = 0;
			for (j = 0; j < shiftR; j++) {
				indexR[num_right] = j;
				num_right += !(less(pivot, *(last - j)));
			}
		}

		//rearrange final iteration
		num = std::min(num_left, num_right);
		if (num != 0)
		{
			*hole = std::move(*(begin + indexL[start_left]));
			*(begin + indexL[start_left]) = std::move(*(last - indexR[start_right]));
			for (j = 1; j < num; j++)
			{
				*(last - indexR[start_right + j - 1]) = std::move(*(begin + indexL[start_left + j]));
				*(begin + indexL[start_left + j]) = std::move(*(last - indexR[start_right + j]));
			}
			hole = (last - indexR[start_right + num - 1]);
		}
		num_left -= num;
		num_right -= num;
		start_left += num;
		start_right += num;

		if (num_left == 0)
			begin += shiftL;
		if (num_right == 0)
			last -= shiftR;

		/*	std::cout << "Partition check" << std::endl;
		for (iter it = bbegin; it != begin;  it++)
		if(*it > pivot)
		std::cout << "vorne" << begin - it << ", " << it->value() << std::endl;
		for (iter it = last + 1; it != eend ;  it++)
		if (*it < pivot)
		std::cout <<"hinten" << it-last << ", " << it->value() << std::endl;
		;*/

		//rearrange remaining elements
		if (num_left != 0)
		{
			assert(num_right == 0);
			int lowerI = start_left + num_left - 1;
			int upper = (int)(last - begin);
			while (lowerI >= start_left && indexL[lowerI] == upper)
			{
				upper--; lowerI--;
			}
			temp = std::move(*(begin + upper));
			while (lowerI >= start_left)
			{
				*(begin + upper) = std::move(*(begin + indexL[lowerI]));
				*(begin + indexL[lowerI]) = std::move(*(begin + (--upper)));
				lowerI--;
			}
			*(begin + upper) = std::move(temp);
			*hole = std::move(*(begin + upper + 1));

			//check for double elements if the pivot sample has repetitions or a small array is partitioned very unequal
			if (double_pivot_check || (small_array && num_left >= (15 * shiftL) / 16)) {
				iter begin_lomuto = begin + upper + 1;
				iter q = begin_lomuto + 1;

				//check at least 4 elements whether they are equal to the pivot using Elmasry, Katajainen and Stenmark's Lomuto partitioner
				unsigned int count_swaps = 1;
				unsigned int count_steps = 0;
				while (q < end && (count_swaps << 2) > count_steps) { //continue as long as there are many elements equal to pivot
					typename std::iterator_traits<iter>::value_type x = std::move(*q);
					bool smaller = !less(pivot, x);
					begin_lomuto += smaller; // smaller = 1 ? begin++ : begin
					count_swaps += smaller;
					index delta = smaller * (q - begin_lomuto);
					iter s = begin_lomuto + delta; // smaller = 1 => s = q : s = begin
					iter y = q - delta; // smaller = 1 => y = begin : y = q
					*s = std::move(*begin_lomuto);
					*y = std::move(x);
					++q;
					count_steps++;
				}

				pivot_length = begin_lomuto + 1 - (begin + upper + 1);

			//	std::cout << "check for double elements left" << pivot_length << " of " << num_left << " array size " << end - temp_begin << std::endl;
			}
			*(begin + upper + 1) = std::move(pivot); // fetch the pivot 
			return begin + upper + 1;

		}
		else if (num_right != 0) {
			assert(num_left == 0);
			int lowerI = start_right + num_right - 1;
			int upper = (int)(last - begin);
			while (lowerI >= start_right && indexR[lowerI] == upper)
			{
				upper--; lowerI--;
			}
			*hole = std::move(*(last - upper));
			while (lowerI >= start_right)
			{
				*(last - upper) = std::move(*(last - indexR[lowerI]));
				*(last - indexR[lowerI--]) = std::move(*(last - (--upper)));
			}

			//check for double elements if the pivot sample has repetitions or a small array is partitioned very unequal
			if (double_pivot_check || (small_array && num_right >= (15 * shiftR) / 16)) {
				iter begin_lomuto = last - upper;
				iter q = begin_lomuto - 1;

				//check at least 4 elements whether they are equal to the pivot using Elmasry, Katajainen and Stenmark's Lomuto partitioner
				unsigned int count_swaps = 1;
				unsigned int count_steps = 0;
				while (q > temp_begin && (count_swaps << 2) > count_steps) { //continue as long as there are many elements equal to pivot
					typename std::iterator_traits<iter>::value_type x = std::move(*q);
					bool smaller = !less(x, pivot);
					begin_lomuto -= smaller; // smaller = 1 ? begin++ : begin
					count_swaps += smaller;
					index delta = smaller * (q - begin_lomuto);
					iter s = begin_lomuto + delta; // smaller = 1 => s = q : s = begin
					iter y = q - delta; // smaller = 1 => y = begin : y = q
					*s = std::move(*begin_lomuto);
					*y = std::move(x);
					--q;
					count_steps++;
				}

				pivot_length = (last - upper) + 1 - begin_lomuto;
				*(last - upper) = std::move(pivot); // fetch the pivot 
				return begin_lomuto;
			}
			else
			{
				*(last - upper) = std::move(pivot); // fetch the pivot 
				return last - upper;
			}


		}
		else { //no remaining elements
			assert(last + 1 == begin);
			*hole = std::move(*begin);
			*begin = std::move(pivot); // fetch the pivot 
			return begin;
		}
	}


	template<typename iter, typename Compare>
	struct Hoare_block_partition_hoare_finish {
		static inline iter partition(iter begin, iter end, Compare less) {
			typedef typename std::iterator_traits<iter>::value_type t;
			iter last = end - 1;
			iter mid = begin + ((end - begin) / 2);
			unsigned char indexL[BLOCKSIZE], indexR[BLOCKSIZE];
			if (less(*mid, *begin)) {
				if (less(*last, *begin)) {
					if (less(*mid, *last)) {
						std::swap(*begin, *last);
					}
					else {
						t temp = std::move(*mid);
						*mid = std::move(*last);
						*last = std::move(*begin);
						*begin = std::move(temp);
					}
				}
			}
			else { // mid > begin 
				if (less(*last, *begin)) { // mid > begin > last 
					std::swap(*mid, *last);
				}
				else {
					if (less(*mid, *last)) { // begin < mid < last 
						std::swap(*begin, *mid);
					}
					else { // begin < mid, mid > last
						t temp = std::move(*mid);
						*mid = std::move(*begin);
						*begin = std::move(*last);
						*last = std::move(temp);
					}
				}
			}

			t q = std::move(*begin);
			mid = begin++;
			t temp;
			last--;
			int iL = 0;
			int iR = 0;
			int sL = 0;
			int sR = 0;
			int j;
			int num;
			while (last - begin + 1 > 2 * BLOCKSIZE) {
				if (iL == 0) {
					sL = 0;
					for (j = 0; j < BLOCKSIZE; j++) {
						indexL[iL] = j;
						iL += ! less(begin[j], q);
					}
				}
				if (iR == 0) {
					sR = 0;
					for (j = 0; j < BLOCKSIZE; j++) {
						indexR[iR] = j;
						iR += ! less(q, (*(last - j)));
					}
				}
				num = std::min(iL, iR);
				if (num != 0) {
					temp = std::move(*(begin + indexL[sL]));
					*(begin + indexL[sL]) = std::move(*(last - indexR[sR]));
					for (j = 1; j < num; j++) {
						*(last - indexR[sR + j - 1]) = std::move(*(begin + indexL[sL + j]));
						*(begin + indexL[sL + j]) = std::move(*(last - indexR[sR + j]));
					}
					*(last - indexR[sR + num - 1]) = std::move(temp);
				}
				iL -= num;
				iR -= num;
				sL += num;
				sR += num;
				if (iL == 0)
					begin += BLOCKSIZE;
				if (iR == 0)
					last -= BLOCKSIZE;
			}
			begin--;
			last++;
		loop:
			do {; } while (less(*(++begin), q));
			do {; } while (less(q, *(--last)));
			if (begin <= last) {
				std::swap(*begin, *last);
				goto loop;
			}
			std::swap(*mid, *last);
			return last;
		}
	};


	template<typename iter, typename Compare>
	struct Hoare_block_partition {
		static inline iter partition(iter begin, iter end, Compare less) {
			//choose pivot
			iter mid = median::median_of_3(begin, end, less);
			//partition
			return hoare_block_partition_unroll_loop(begin + 1, end - 1, mid, less);
		}

		//with duplicate check
		static inline iter partition(iter begin, iter end, Compare less, int & pivot_length){
			//choose pivot
			iter mid = median::median_of_3(begin, end, less);
			//partition
			return hoare_block_partition_unroll_loop(begin + 1, end - 1, mid, less, pivot_length);
		}
		static inline iter partition(iter begin, iter end, iter pivot, Compare less) {
			//partition
			return hoare_block_partition_unroll_loop(begin + 1, end - 1, pivot, less);
		}
	};

	template<typename iter, typename Compare>
	struct Hoare_block_partition_simple {
		static inline iter partition(iter begin, iter end, Compare less) {
			//choose pivot
			iter mid = median::median_of_3(begin, end, less);
			//partition
			return hoare_block_partition_simple(begin + 1, end - 1, mid, less);
		}
		static inline iter partition(iter begin, iter end, iter pivot, Compare less) {
			//partition
			return hoare_block_partition_simple(begin + 1, end - 1, pivot, less);
		}
	};


	template<typename iter, typename Compare>
	struct Hoare_block_partition_Mo5 {
		static inline iter partition(iter begin, iter end, Compare less) {
			//choose pivot
			iter mid;
			if (end - begin > 30)
				mid = median::median_of_5(begin, end, less);
			else
				mid = median::median_of_3(begin, end, less);
			//partition
			return hoare_block_partition_unroll_loop(begin + 1, end - 1, mid, less);
		}
	};

	template<typename iter, typename Compare>
	struct Hoare_block_partition_median_of_3_medians_of_3 {
		static inline iter partition(iter begin, iter end, Compare less) {
			//choose pivot
			iter mid;
			if (end - begin > 70)
				mid = median::median_of_3_medians_of_3(begin, end, less);
			else
				mid = median::median_of_3(begin, end, less);
			//partition
			return hoare_block_partition_unroll_loop(begin + 1, end - 1, mid, less);
		}
	};

	template<typename iter, typename Compare>
	struct Hoare_block_partition_macro_pivot {
		static inline iter partition(iter begin, iter end, Compare less) {
			//choose pivot
			iter mid;
			if (end - begin > MO3_THRESH)
			{
				mid = median::median_of_k<PIVOTSAMPLESIZE>(begin, end, less);
				return hoare_block_partition_unroll_loop(begin + PIVOTSAMPLESIZE / 2, end - PIVOTSAMPLESIZE / 2, mid, less);
			}
			else {
				mid = median::median_of_3(begin, end, less);
				return hoare_block_partition_unroll_loop(begin + 1, end - 1, mid, less);
			}		
		}
	};

	
	template<typename iter, typename Compare>
	struct Hoare_block_partition_median_of_3_medians_of_5 {
		static inline iter partition(iter begin, iter end, Compare less) {
			//choose pivot
			iter mid;
			if (end - begin > 200)
				mid = median::median_of_3_medians_of_5(begin, end, less);
			else
				mid = median::median_of_3(begin, end, less);
			//partition
			return hoare_block_partition_unroll_loop(begin + 1, end - 1, mid, less);
		}
	};

	template<typename iter, typename Compare>
	struct Hoare_block_partition_median_of_5_medians_of_5 {
		static inline iter partition(iter begin, iter end, Compare less) {
			//choose pivot
			iter mid;
			if (end - begin > 1000)
				mid = median::median_of_5_medians_of_5(begin, end, less);
			else if (end - begin > 100)
				mid = median::median_of_3_medians_of_3(begin, end, less);
			else
				mid = median::median_of_3(begin, end, less);
			//partition
			return hoare_block_partition_unroll_loop(begin + 1, end - 1, mid, less);
			
			
		}
	};

	template<typename iter, typename Compare>
	struct Hoare_block_partition_mosqrt {
		static inline iter partition(iter begin, iter end, Compare less) {
			iter mid;
			if (end - begin > 20000)
			{
				unsigned int pivot_sample_size = sqrt(end - begin);
				pivot_sample_size += (1 - (pivot_sample_size % 2));//make it an odd number
				mid = median::median_of_k(begin, end, less, pivot_sample_size); //choose pivot as median of sqrt(n)
				//partition
				return hoare_block_partition_unroll_loop(begin + pivot_sample_size / 2, end - pivot_sample_size / 2, mid, less);
			}
			else
			{
				if (end - begin > 800)
					mid = median::median_of_5_medians_of_5(begin, end, less);
				else if (end - begin > 100)
					mid = median::median_of_3_medians_of_3(begin, end, less);
				else
					mid = median::median_of_3(begin, end, less);
				//partition
				return hoare_block_partition_unroll_loop(begin + 1, end - 1, mid, less);
			}
			

		}

		//with duplicate check
		static inline iter partition(iter begin, iter end, Compare less, int & pivot_length) {
			iter mid;
			if (end - begin > 20000)
			{
				unsigned int pivot_sample_size = sqrt(end - begin);
				pivot_sample_size += (1 - (pivot_sample_size % 2));//make it an odd number
				mid = median::median_of_k(begin, end, less, pivot_sample_size);//choose pivot as median of sqrt(n)
				//partition
				return hoare_block_partition_unroll_loop(begin + pivot_sample_size / 2, end - pivot_sample_size / 2, mid, less, pivot_length);
			}
			else
			{
				if (end - begin > 800)
					mid = median::median_of_5_medians_of_5(begin, end, less);
				else if (end - begin > 100)
					mid = median::median_of_3_medians_of_3(begin, end, less);
				else
					mid = median::median_of_3(begin, end, less);
				//mid = median::median_of_3(begin , end , less);
				return hoare_block_partition_unroll_loop(begin + 1, end - 1, mid, less, pivot_length);
			}
		}
	};


	template<typename iter, typename Compare>
	struct Hoare_partition {
		static inline iter partition(iter begin, iter end, Compare less) {
			using t = typename std::iterator_traits<iter>::value_type;
			iter last = end - 1;
			iter mid = begin + ((end - begin) / 2);

			leanswap(begin, last, less);
			leanswap(mid, last, less);
			leanswap(mid, begin, less);

			t q = std::move(*begin);
			mid = begin;
		loop:
			do{; } while (less(*(++begin), q));
			do{; } while (less(q , *(--last)));
			if (begin <= last) {
				std::swap(*begin, *last);
				goto loop;
			}
			std::swap(*mid, *last);
			return last;
		}
	};

	template<typename iter>
	void printArray(iter begin, iter end,  const std::string &desc) {
		int i = 0;

		if((end - begin) < 0) {
			std::cout << desc << " printing failed because begin was above end" << std::endl;
			
			return;
		}
		iter begin2 = begin;
		iter end2 = end;
		std::cout << "Printing array: " << desc << std::endl;
		while (begin2 != end2) {
			
			std::cout << *begin2 << " ";
			begin2++;
			i++;
			
		}
		std::cout << std::endl;

	}


	template<typename iter, typename Compare>
	inline void multi_pivot_2_block_partition_simple(iter begin, iter end, iter* pivot_positions, Compare less, iter* ret1, iter* ret2) {
		typedef typename std::iterator_traits<iter>::difference_type index;		
		int block = 2 * BLOCKSIZE;

		index R[block], L[block];

		iter last = end-1;
		//Moving pivots to the last positions
		std::iter_swap(pivot_positions[0], begin);
		std::iter_swap(pivot_positions[1], last);
		const typename std::iterator_traits<iter>::value_type & p1 = *begin;
		const typename std::iterator_traits<iter>::value_type & p2 = *last;
		pivot_positions[0] = begin;
		pivot_positions[1] = last;
		last--;
		begin++;

		int num_ll = 0;
		int num_lr = 0;
		int num_rl = 0;
		int num_rr = 0;
		iter middle = begin;
		int delta = BLOCKSIZE;
		int start_ll = 0;
		int start_lr = 0;
		int start_rl = 0;
		int start_rr = 0;
		int num;
		index i, j;
		while (last - begin + 1 > 2 * BLOCKSIZE) {
			if(num_ll == 0 && num_lr == 0) {
				start_ll = 0;
				start_lr = 0;
				for (i = 0; i < BLOCKSIZE; i++) {
					L[num_ll] = i;
					L[delta + num_lr] = i;

					//Is it in the right partition
					num_lr += less(p2, begin[i]);
					//Is it in the left partition
					num_ll += less(begin[i], p1);
				}
			}

			if(num_rl == 0 && num_rr == 0) {
				start_rl = 0;
				start_rr = 0;
				for (j = 0; j < BLOCKSIZE; j++) {
					R[num_rl] = j;
					R[delta + num_rr] = j;
					
					//Current index
					auto leftIndex = (last - j);
					//Is it in the left partition
					int b1 = (less(*leftIndex, p1));
					num_rl += b1;
					//Is it in the middle partition
					num_rr += (!b1 && !less(p2, *leftIndex));
				}
			}

			//Rearrange the elements
			//Swap lr with rr
			
			num = std::min(num_lr, num_rr);
			for (int k = 0; k < num; k++) {
				std::iter_swap(begin + L[start_lr + delta + k], last-R[start_rr + delta + k]);
			}
			start_lr += num;
			start_rr += num;
			num_lr -= num;
			num_rr -= num;

			//Edge case where we have empty LL elements that are already in place
			
			if(middle == begin) {
				int ll = 0;
				while (*(L + start_ll) == ll && num_ll > 0 ){
					++start_ll;
					--num_ll;
					++ll;
					middle++;
				}
			}

			//lr <-> rl
			num = std::min(num_lr, num_rl);
			for (int k = 0; k < num; k++) {
				//Ensuring our middle partition is large enough
				while (*(L + start_ll) < *(L + start_lr + delta + k) && num_ll > 0) {
					std::iter_swap(middle, begin + L[start_ll]);
					++start_ll;
					--num_ll;
					++middle;
				}
				rotations::rotate3(*(begin + L[start_lr + delta + k]), *middle, *(last - R[start_rl + k]));
				middle++;
			} 
			
			start_rl += num;
			start_lr += num;
			num_rl -= num;
			num_lr -= num;
			
			//Empty ll
			if(num_lr == 0) { 
				for(int k = 0; k < num_ll; k++) {
					std::iter_swap(begin + L[start_ll + k], middle);
					middle++;
				}
				start_ll += num_ll;
				num_ll = 0;
			}		

			begin += (num_ll == 0 && num_lr == 0) ? BLOCKSIZE : 0;
			last -= (num_rl == 0 && num_rr == 0) ? BLOCKSIZE : 0;
		}
		
		//Compare and store in buffers final iteration
		index shiftR = 0;
		index shiftL = 0;
		if(num_ll == 0 && num_lr == 0 && num_rl == 0 && num_rr == 0) {
			shiftL = ((last-begin) + 1) / 2; 
			shiftR = (last - begin) + 1 - shiftL;
			start_ll = 0;
			start_lr = 0;
			start_rr = 0;
			start_rl = 0;
			for (index j = 0; j < shiftL; j++) {
				L[num_ll] = j;
				L[delta + num_lr] = j;
				//Is it in the right partition
				num_lr += less(p2, begin[j]);
				//Is it in the left partition
				num_ll += less(begin[j], p1);
				R[num_rl] = j;
				R[delta + num_rr] = j;
				
				//Current index
				auto leftIndex = (last - j);
				//Is it in the left partition
				int b1 = (less(*leftIndex, p1));
				num_rl += b1;
				//Is it in the middle partition
				num_rr += (!b1 && !(less(p2, *leftIndex)));
			}
			
			if(shiftL < shiftR) {
				auto subIndex = (last - shiftR + 1);
				int b1 = (less(*subIndex, p1));
				R[num_rl] = shiftR - 1;
				R[delta + num_rr] = shiftR - 1;
				num_rl += b1;
				num_rr += !b1 && !less(p2, *subIndex);
			}
		}
		else if(num_rl != 0 || num_rr != 0) {
			shiftL = (last - begin) - BLOCKSIZE + 1;
			shiftR = BLOCKSIZE;
			start_ll = 0;
			start_lr = 0;
			for(index j = 0; j < shiftL; j++) {
				L[num_ll] = j;
				L[delta + num_lr] = j;
				num_ll += (less(begin[j], p1));
				num_lr += less(p2, begin[j]);
			}
		} else {
			shiftL = BLOCKSIZE;
			shiftR = (last - begin) - BLOCKSIZE + 1;
			start_rl = 0;
			start_rr = 0;
			for (index j = 0; j < shiftR; j++) {
				R[num_rl] = j;
				R[delta + num_rr] = j;
				bool b1 = less(*(last-j), p1);
				num_rl += b1;
				num_rr += !b1 && !less(p2, *(last-j));
			}
		}

		//rearrange final iteration
		//Swap lr with rr
		num = std::min(num_lr, num_rr);
		for (int k = 0; k < num; k++) {
			std::iter_swap(begin + L[start_lr + delta + k], last-R[start_rr + delta + k]);
		}
		start_lr += num;
		start_rr += num;
		num_lr -= num;
		num_rr -= num;

		//Edge case where we have empty LL elements that are already in place
		int ll = 0;
		if(middle == begin) {
			while (*(L + start_ll) == ll && num_ll > 0 ){
				++start_ll;
				--num_ll;
				++ll;
				middle++;
			}
		}

		//lr <-> rl
		num = std::min(num_lr, num_rl);
		for (int k = 0; k < num; k++) {
			//Ensuring our middle partition is large enough
			while (*(L + start_ll) < *(L + start_lr + delta + k) && num_ll > 0) {
				std::iter_swap(middle, begin + L[start_ll]);
				++start_ll;
				--num_ll;
				++middle;
			}
			rotations::rotate3(*(begin + L[start_lr + delta + k]), *middle, *(last - R[start_rl + k]));
			middle++;
		} 
		
		start_rl += num;
		start_lr += num;
		num_rl -= num;
		num_lr -= num;
		
		//Empty ll
		if(num_lr == 0) { 
			for(int k = 0; k < num_ll; k++) {
				std::iter_swap(begin + L[start_ll + k], middle);
				middle++;
			}
			start_ll += num_ll;
			num_ll = 0;
		}		

		begin += (num_ll == 0 && num_lr == 0) ? shiftL : 0;
		last -= (num_rl == 0 && num_rr == 0) ? shiftR : 0;
		
		iter right = last+1;
		
		int k = 0;
		int l = last - begin;
		//TODO: More elegant way of detecting this?
		bool rightLoop = num_rl != 0 || num_rr != 0;
		
		//If left side is empty, and right side has elements	
		while(num_rl != 0 || num_rr != 0){
			//search from left to right 
			while((( num_rr != 0 && l == R[start_rr + delta + num_rr - 1] ) || (num_rl != 0 && l == R[start_rl + num_rl - 1])) && l > k ){
				bool b = num_rl != 0 && (l == R[start_rl + num_rl - 1]); // 1 if left 
				std::iter_swap(last - l, middle);
				middle += b;
				num_rr -= !b;
				num_rl -= b;
				l--;
			}
			
			//search from right to left
			while((num_rr == 0 || k != R[delta + start_rr]) && (num_rl == 0 || k != R[start_rl]) && l > k) 
			{  
				k++;
			}
			
			bool mid = num_rr != 0 && k == R[delta + start_rr]; //Is 1 if element is a mid element, false no mid elements
			bool left = num_rl != 0 && k == R[start_rl]; 
			//Rotate last middle begin or last begin begin
			rotations::rotate3(*(last - k) , *(last - l),   *((last - l) - (left * ((last - l) - middle))));
			
			l -= l > k;
			k += l > k;	
			middle += left;
			start_rl += left;
			num_rl -= left;
			num_rr -= mid;
			start_rr += mid;
			assert(num_rr > -1); assert(num_rl > -1);
		}
		
		
		//If we executed the right loop and emptied the blocks we cannot 
		//guarantee that the right pointer was moved all the way so we just move it by
		//the difference between k and l.
		if(rightLoop)
			right = (last - l + (*(last - l) < p2)); //TODO: Can there be a more elegant way?

		//Should ensure the edge case of middle being ahead of begin
		k += middle - (begin + k);

		//If right side is empty and left side has elements
		while(num_ll != 0 || num_lr != 0) {

			while(((num_lr != 0 && k != L[start_lr + delta]) || (num_ll != 0 && num_lr == 0)) && l > k) {
				bool b = num_ll != 0 && (k == L[start_ll]);	
				std::iter_swap(begin + k,  middle);	
				middle += b;
				num_ll -= b;
				start_ll += b;
				k++;
			}


			while( num_lr != 0 && l == L[start_lr + num_lr + delta - 1] && l > 0 && l > k) {
				num_lr--;
				l--;
				right--;
			}
			bool b = num_ll != 0 && (l == L[start_ll + num_ll - 1]);
			//Rotate last middle begin or last begin begin
			rotations::rotate3(*(begin + l), *(begin + k), *((begin + k) - (b * ((begin + k) - middle))));
			middle += b;
			num_ll -= b;
			
			bool isRight = num_lr != 0;
			num_lr -= isRight;
			start_lr += isRight;
			
			l--;
			k++;
			right -= isRight;
			assert(num_lr > -1); assert(num_ll > -1);
		}

		std::iter_swap(pivot_positions[0], middle - 1);
		std::iter_swap(pivot_positions[1], right);
		*ret1 = (middle - 1);
		*ret2 = right;
	}

	//Multi-Pivot part 
	template< typename iter, typename Compare>
	struct Multi_Pivot_Hoare_Block_partition_simple {
		static inline void partition(iter begin, iter end, iter* p1, iter* p2, Compare less) {
			iter* pivots = median::mp_tertiles_of_3_pivots_2(begin, end-1, less);
			multi_pivot_2_block_partition_simple(begin, end, pivots, less, p1, p2);
		}
	};

	template< typename iter, typename Compare>
	struct Multi_Pivot_Hoare_Block_partition_simple_mo5 {
		static inline void partition(iter begin, iter end, iter* p1, iter* p2, Compare less) {
			iter* pivots = median::mp_tertiles_of_5_pivots_2(begin, end-1, less);
			multi_pivot_2_block_partition_simple(begin, end, pivots, less, p1, p2);
		}
	};	



	//Lomuto based part

	template<typename iter, typename Compare>
	inline iter lomuto_block_partition_simple(iter begin, iter end, iter pivot_position, Compare less) {
		typedef typename std::iterator_traits<iter>::difference_type index;
		index indexL[BLOCKSIZE];
		iter last = end - 1;
		std::iter_swap(pivot_position, last);
		const typename std::iterator_traits<iter>::value_type & pivot = *last;
		pivot_position = last;
		iter counter = begin;
		last--;

		int num_left = 0;
		int start_left = 0;
		//main loop
		iter offset = begin;

		while (last - counter + 1 > 0)//BLOCKSIZE)
		{
			int t = (last-counter+1);
			int limit = std::min(BLOCKSIZE, t);
			for (index j = 0; j < limit; j++) {
					indexL[num_left] = j;
					num_left += less(counter[j], pivot);				
			}
			//rearrange elements
			
			for (int j = 0; j < num_left; j++){
				std::iter_swap(offset, counter+indexL[start_left+j]);
				offset++;
			}

			num_left = 0;//num;
			start_left =0;// num;
			counter += limit;

		}//end main loop

		std::iter_swap(pivot_position, offset);// fetch the pivot 
		return offset;
	}

	template<typename iter, typename Compare>
	inline iter lomuto_block_partition(iter begin, iter end, iter pivot_position, Compare less) {
		typedef typename std::iterator_traits<iter>::difference_type index;
		index indexL[BLOCKSIZE];
		iter last = end - 1;
		std::iter_swap(pivot_position, last);
		const typename std::iterator_traits<iter>::value_type & pivot = *last;
		pivot_position = last;
		iter counter = begin;
		last--;

		int num_left = 0;
		int start_left = 0;
		//main loop
		iter offset = begin;

		while (last - counter + 1 > BLOCKSIZE)//BLOCKSIZE)
		{
			//Compare and store in buffers
			for (index j = 0; j < BLOCKSIZE; j++) {
					indexL[num_left] = j;
					num_left += less(counter[j], pivot);				
			}
			//rearrange elements
			
			for (int j = 0; j < num_left; j++){
				std::iter_swap(offset, counter+indexL[start_left+j]);
				offset++;
			}

			num_left = 0;//num;
			start_left =0;// num;
			counter += BLOCKSIZE;

		}//end main loop
		
		int t = (last-counter+1);
		//int limit = std::min(BLOCKSIZE, t);
		for (index j = 0; j < t; j++) {
				indexL[num_left] = j;
				num_left += less(counter[j], pivot);				
		}
		//rearrange elements
		
		for (int j = 0; j < num_left; j++){
			std::iter_swap(offset, counter+indexL[start_left+j]);
			offset++;
		}

	//	num_left = 0;//num;
	//	start_left =0;// num;
	//	counter += t;

		std::iter_swap(pivot_position, offset);// fetch the pivot 
		return offset;
	}

	template<typename iter, typename Compare>
	inline iter lomuto_block_partition_less(iter begin, iter end, iter pivot_position, Compare less) {
		typedef typename std::iterator_traits<iter>::difference_type index;
		index indexL[BLOCKSIZE];
		iter last = end - 1;
		std::iter_swap(pivot_position, last);
		const typename std::iterator_traits<iter>::value_type & pivot = *last;
		pivot_position = last;
		iter counter = begin;
		last--;

		int num_left = 0;
		int start_left = 0;
		//main loop
		iter offset = begin;

		while (last - counter + 1 > BLOCKSIZE)//BLOCKSIZE)
		{
			//Compare and store in buffers
			for (index j = 0; j < BLOCKSIZE; j++) {
					indexL[num_left] = j;
					num_left += less(counter[j], pivot);				
			}
			//rearrange elements
			
			for (int j = 0; j < num_left; j++){
				std::iter_swap(offset+j, counter+indexL[start_left+j]);
				//offset++;
			}
			offset += num_left;
			num_left = 0;//num;
			start_left =0;// num;
			counter += BLOCKSIZE;

		}//end main loop
		
		int t = (last-counter+1);
		//int limit = std::min(BLOCKSIZE, t);
		for (index j = 0; j < t; j++) {
				indexL[num_left] = j;
				num_left += less(counter[j], pivot);				
		}
		//rearrange elements
		
		for (int j = 0; j < num_left; j++){
			std::iter_swap(offset+j, counter+indexL[start_left+j]);
			//offset++;
		}
		offset+= num_left;
	//	num_left = 0;//num;
	//	start_left =0;// num;
		//counter += t;

		std::iter_swap(pivot_position, offset);// fetch the pivot 
		return offset;
	}

	template<typename iter, typename Compare>
	struct Lomuto_block_partition_simple {
		static inline iter partition(iter begin, iter end, Compare less) {
			//choose pivot
			iter mid = median::median_of_3(begin, end, less);
			//partition
			return lomuto_block_partition_simple(begin, end, mid, less);
		}
		static inline iter partition(iter begin, iter end, iter pivot, Compare less) {
			//partition
			return lomuto_block_partition_simple(begin, end, pivot, less);
		}
	};

	template<typename iter, typename Compare>
	struct Lomuto_block_partition {
		static inline iter partition(iter begin, iter end, Compare less) {
			//choose pivot
			iter mid = median::median_of_3(begin, end, less);
			//partition
			return lomuto_block_partition(begin, end, mid, less);
		}
		static inline iter partition(iter begin, iter end, iter pivot, Compare less) {
			//partition
			return lomuto_block_partition(begin, end, pivot, less);
		}
	};

	template<typename iter, typename Compare>
	struct Lomuto_block_partition_less {
		static inline iter partition(iter begin, iter end, Compare less) {
			//choose pivot
			iter mid = median::median_of_3(begin, end, less);
			//partition
			return lomuto_block_partition(begin, end, mid, less);
		}
		static inline iter partition(iter begin, iter end, iter pivot, Compare less) {
			//partition
			return lomuto_block_partition(begin, end, pivot, less);
		}
	};


	
	template<typename iter, typename Compare>
	inline void dual_lomuto_block_partition_simple(iter begin, iter end, iter* pivot_positions, Compare less, iter* ret1, iter* ret2) {
		typedef typename std::iterator_traits<iter>::difference_type index;		
		index block1[BLOCKSIZE], block2[BLOCKSIZE];
		//index R[block], L[block];

		iter last = end-1;
		//Moving pivots to the last positions
		//std::cout << "pivots: " << *pivot_positions[0] << " " << *pivot_positions[1] << std::endl;
		
		std::iter_swap(pivot_positions[0], last);
		const typename std::iterator_traits<iter>::value_type & p1 = *last;
		pivot_positions[0] = last;
		last--;
		std::iter_swap(pivot_positions[1], last);
		const typename std::iterator_traits<iter>::value_type & p2 = *last;
		pivot_positions[1] = last;
		last--;
		//printArray(begin, end, "Start array");

		int num1 = 0;
		int num2 = 0;
		int start1 = 0;
		int start2 = 0;
		iter counter = begin;
		iter offset1 = begin;
		iter offset2 = begin;
		int num;
		while (last - counter + 1 > 0) {

			//Seg fault issue here, look at simple
			int t = (last-counter+1);
			int limit = std::min(BLOCKSIZE, t);
			for (index j = 0; j < limit; j++) {
					block1[num1] = j;
					int o = less(counter[j], p1);
					num1 += o;	
					block2[num2] = j;
					num2 += less(counter[j], p2) - o;
			}
			//Rearrange the elements
			//Find the first element we have stored in a block
			num = num1+num2;
			int k = 0; 
			while(k < num){
				int res = (block2[start2] < block1[start1]) ? block2[start2] : block1[start1];
				int b = (res == block2[start2] && num2 != 0);
				auto difference = (offset2-offset1);
				rotations::rotate3(*(counter+res), *offset2, *(offset1 + (difference * b)));
				offset2++;
				offset1 += 1-b;
				start1 += 1-b;
				start2 += b;
				num1 -= 1-b;
				num2 -= b;
				k++;
			

			}
			start1 = 0;
			start2 = 0;
			num1 = 0;
			num2 = 0;
			counter += limit;

		}

		/*std::cout << "offset1: " << *offset1 << std::endl;
		std::cout << "offset2: " << *offset2 << std::endl;	
		printArray(begin, end, " End array");*/
		rotations::rotate3(*offset2, *offset1, *(end-1));
		//std::iter_swap(www, offset2);
		//std::iter_swap(offset2, offset1);
		offset2++;
		std::iter_swap(end-2, offset2);
		//
		*ret1 = offset1;
		*ret2 = offset2;
	}

	template<typename iter, typename Compare>
	inline void dual_lomuto_block_partition_merge(iter begin, iter end, iter* pivot_positions, Compare less, iter* ret1, iter* ret2) {
		
		typedef typename std::iterator_traits<iter>::difference_type index;		
		index block1[BLOCKSIZE+1], block2[BLOCKSIZE+1];
		//An index should never become less than 0. 
		block1[BLOCKSIZE] = INFINITY;
		block2[BLOCKSIZE] = INFINITY;
		//index R[block], L[block];

		iter last = end-1;
		//Moving pivots to the last positions
		//std::cout << "Pivots: " << *pivot_positions[0] << ", " << *pivot_positions[1] << std::endl;
		//printArray(begin, end, "Start array");
		std::iter_swap(pivot_positions[0], last);
		const typename std::iterator_traits<iter>::value_type & p1 = *last;
		pivot_positions[0] = last;
		last--;
		std::iter_swap(pivot_positions[1], last);
		const typename std::iterator_traits<iter>::value_type & p2 = *last;
		pivot_positions[1] = last;
		last--;

		int num1 = 0;
		int num2 = 0;
		int start1 = 0;
		int start2 = 0;
		iter counter = begin;
		iter offset1 = begin;
		iter offset2 = begin;
		int num;
		while (last - counter + 1 > 0) {

			int t = (last-counter+1);
			int limit = std::min(BLOCKSIZE, t);
			for (index j = 0; j < limit; j++) {
					block1[num1] = j;
					int o = less(counter[j], p1);
					num1 += o;	
					block2[num2] = j;
					num2 += less(counter[j], p2) - o;
			}
			
			//Rearrange the elements
			//Find the first element we have stored in a block
			num = num1+num2;
			//std::cout << "I found elements: " << num << std::endl;
			start1 += (num1 == 0) * (BLOCKSIZE-start1);
			start2 += (num2 == 0) * (BLOCKSIZE-start2);
			for(int k = 0; k < num; k++){
				if(block1[start1] < block2[start2] ) {
					rotations::rotate3(*(counter+block1[start1]), *offset2, *offset1);
					offset1++;
					offset2++;
					start1++;
					num1--;
					//Move start1 to the end of the array
					
					start1 += (num1 == 0) * (BLOCKSIZE-start1);
				}
				else{
					std::iter_swap(offset2, counter+block2[start2]);
					start2++;
					offset2++;
					num2--;
					start2 += (num2 == 0) * (BLOCKSIZE-start2);
				}
			}
			start1 = 0;
			start2 = 0;
			num1 = 0;
			num2 = 0;
			counter += limit;

		}
		rotations::rotate3(*offset2, *offset1, *(end-1));
		offset2++;
		std::iter_swap(end-2, offset2);
		*ret1 = offset1;
		*ret2 = offset2;
	}


	template<typename iter, typename Compare>
	inline void dual_lomuto_block_partition_while(iter begin, iter end, iter* pivot_positions, Compare less, iter* ret1, iter* ret2) {
		typedef typename std::iterator_traits<iter>::difference_type index;		
		index block1[BLOCKSIZE+1], block2[BLOCKSIZE+1];
		//An index should never become less than 0. 
		block1[BLOCKSIZE] = INFINITY;
		block2[BLOCKSIZE] = INFINITY;
		//index R[block], L[block];

		iter last = end-1;
		//Moving pivots to the last positions
		std::iter_swap(pivot_positions[0], last);
		const typename std::iterator_traits<iter>::value_type & p1 = *last;
		pivot_positions[0] = last;
		last--;
		std::iter_swap(pivot_positions[1], last);
		const typename std::iterator_traits<iter>::value_type & p2 = *last;
		pivot_positions[1] = last;
		last--;

		int num1 = 0;
		int num2 = 0;
		int start1 = 0;
		int start2 = 0;
		iter counter = begin;
		iter offset1 = begin;
		iter offset2 = begin;
		int num;
		while (last - counter + 1 > 0) {

			//Seg fault issue here, look at simple
			int t = (last-counter+1);
			int limit = std::min(BLOCKSIZE, t);
			for (index j = 0; j < limit; j++) {
					block1[num1] = j;
					int o = less(counter[j], p1);
					num1 += o;	
					block2[num2] = j;
					num2 += less(counter[j], p2) - o;
			}
			//Rearrange the elements
			//Find the first element we have stored in a block
			num = num1+num2;
			int k = 0; 
			start1 += (num1 == 0) * (BLOCKSIZE-start1);
			start2 += (num2 == 0) * (BLOCKSIZE-start2);
			while(k < num){

				//Look into how merge is done properly according to branchless merge sort
				while(block1[start1] < block2[start2] ){
					//double check rotations
					rotations::rotate3(*(counter+block1[start1]), *offset2, *offset1);
					offset1++;
					offset2++;
					start1++;
					num1--;
					k++;
					start1 += (num1 == 0) * (BLOCKSIZE-start1);
				}
				while(block2[start2] < block1[start1]){
					std::iter_swap(offset2, counter+block2[start2]);
					start2++;
					offset2++;
					num2--;
					k++;
					start2 += (num2 == 0) * (BLOCKSIZE-start2);
				}

			}
			start1 = 0;
			start2 = 0;
			num1 = 0;
			num2 = 0;
			counter += limit;

		}

		rotations::rotate3(*offset2, *offset1, *(end-1));
		offset2++;
		std::iter_swap(end-2, offset2);
		*ret1 = offset1;
		*ret2 = offset2;
	}


	template<typename iter, typename Compare>
	inline void dual_lomuto_block_partition_simple_elements(iter begin, iter end, iter* pivot_positions, Compare less, iter* ret1, iter* ret2) {
		typedef typename std::iterator_traits<iter>::difference_type index;		
		typedef typename std::iterator_traits<iter>::value_type val;
		val block1[BLOCKSIZE];
		val block2[BLOCKSIZE];
		val block3[BLOCKSIZE];
		//index R[block], L[block];

		iter last = end-1;
		//Moving pivots to the last positions
		
		std::iter_swap(pivot_positions[0], last);
		const typename std::iterator_traits<iter>::value_type & p1 = *last;
		pivot_positions[0] = last;
		last--;
		std::iter_swap(pivot_positions[1], last);
		const typename std::iterator_traits<iter>::value_type & p2 = *last;
		pivot_positions[1] = last;
		last--;

		int num1 = 0;
		int num2 = 0;
		int num3 = 0;
		iter counter = begin;
		iter offset1 = begin;
		iter offset2 = begin;
		while (last - counter + 1 > 0) {
			int t = (last-counter+1);
			int limit = std::min(BLOCKSIZE, t);
			for (index j = 0; j < limit; j++) {
					//Should be copied and not a reference
					block1[num1] = *(counter+j);
					int o = less(counter[j], p1);
					num1 += o;	
					block2[num2] = *(counter+j);
					int s = less(counter[j], p2);
					num2 +=  s - o;
					block3[num3] = *(counter+j);
					num3 += 1 - s;
			}
			
			for(index p = 0; p < num1; p++){
				*counter = block1[p];
				rotations::rotate3(*counter, *offset2, *offset1);
				offset1++;
				offset2++;
				counter++;
			}

			for(index q = 0; q < num2; q++){
				*counter = block2[q];
				std::iter_swap(offset2, counter);
				counter++;
				offset2++;
			}
			for(index p = 0; p < num3; p++){
				*counter = block3[p];
				counter++;
			}
			num1 = 0;
			num2 = 0;
			num3 = 0;

		}
		
		rotations::rotate3(*offset2, *offset1, *(end-1));
		offset2++;
		std::iter_swap(end-2, offset2);
		*ret1 = offset1;
		*ret2 = offset2;
	}

	template<typename iter, typename Compare>
	inline void dual_lomuto_block_partition_elements(iter begin, iter end, iter* pivot_positions, Compare less, iter* ret1, iter* ret2) {
		typedef typename std::iterator_traits<iter>::difference_type index;		
		typedef typename std::iterator_traits<iter>::value_type val;
		val block1[BLOCKSIZE];
		val block2[BLOCKSIZE];
		val block3[BLOCKSIZE];
		//index R[block], L[block];

		iter last = end-1;
		//Moving pivots to the last positions
		
		std::iter_swap(pivot_positions[0], last);
		const typename std::iterator_traits<iter>::value_type & p1 = *last;
		pivot_positions[0] = last;
		last--;
		std::iter_swap(pivot_positions[1], last);
		const typename std::iterator_traits<iter>::value_type & p2 = *last;
		pivot_positions[1] = last;
		last--;

		int num1 = 0;
		int num2 = 0;
		int num3 = 0;
		iter counter = begin;
		iter offset1 = begin;
		iter offset2 = begin;
		while (last - counter + 1 > BLOCKSIZE) {
			for (index j = 0; j < BLOCKSIZE; j++) {
					//Should be copied and not a reference
					block1[num1] = *(counter+j);
					int o = less(counter[j], p1);
					num1 += o;	
					block2[num2] = *(counter+j);
					int s = less(counter[j], p2);
					num2 +=  s - o;
					block3[num3] = *(counter+j);
					num3 += 1 - s;
			}
			
			for(index p = 0; p < num1; p++){
				*counter = block1[p];
				rotations::rotate3(*counter, *offset2, *offset1);
				offset1++;
				offset2++;
				counter++;
			}

			for(index q = 0; q < num2; q++){
				*counter = block2[q];
				std::iter_swap(offset2, counter);
				counter++;
				offset2++;
			}
			for(index p = 0; p < num3; p++){
				*counter = block3[p];
				counter++;
			}
			num1 = 0;
			num2 = 0;
			num3 = 0;

		}
		for (index j = 0; j < (last-counter+1); j++) {
				//Should be copied and not a reference
				block1[num1] = *(counter+j);
				int o = less(counter[j], p1);
				num1 += o;	
				block2[num2] = *(counter+j);
				int s = less(counter[j], p2);
				num2 +=  s - o;
				block3[num3] = *(counter+j);
				num3 += 1 - s;
		}
		
		for(index p = 0; p < num1; p++){
			*counter = block1[p];
			rotations::rotate3(*counter, *offset2, *offset1);
			offset1++;
			offset2++;
			counter++;
		}

		for(index q = 0; q < num2; q++){
			*counter = block2[q];
			std::iter_swap(offset2, counter);
			counter++;
			offset2++;
		}
		for(index p = 0; p < num3; p++){
			*counter = block3[p];
			counter++;
		}
		
		rotations::rotate3(*offset2, *offset1, *(end-1));
		offset2++;
		std::iter_swap(end-2, offset2);
		*ret1 = offset1;
		*ret2 = offset2;
	}



	//Bulk move has to be rethought, it does not work in the current state of the idea
	template<typename iter, typename Compare>
	inline void dual_lomuto_block_partition_simple_elements_move(iter begin, iter end, iter* pivot_positions, Compare less, iter* ret1, iter* ret2) {
		typedef typename std::iterator_traits<iter>::difference_type index;		
		typedef typename std::iterator_traits<iter>::value_type val;
		val block1[BLOCKSIZE];
		val block2[BLOCKSIZE];
		val block3[BLOCKSIZE];
		//index R[block], L[block];

		iter last = end-1;
		//Moving pivots to the last positions
		
		std::iter_swap(pivot_positions[0], last);
		const typename std::iterator_traits<iter>::value_type & p1 = *last;
		pivot_positions[0] = last;
		last--;
		std::iter_swap(pivot_positions[1], last);
		const typename std::iterator_traits<iter>::value_type & p2 = *last;
		pivot_positions[1] = last;
		last--;

		int num1 = 0;
		int num2 = 0;
		int num3 = 0;
		iter counter = begin;
		iter offset1 = begin;
		iter offset2 = begin;
		while (last - counter + 1 > 0) {
			int t = (last-counter+1);
			int limit = std::min(BLOCKSIZE, t);
			for (index j = 0; j < limit; j++) {
					//Should be copied and not a reference
					block1[num1] = *(counter+j);
					int o = less(counter[j], p1);
					num1 += o;	
					block2[num2] = *(counter+j);
					int s = less(counter[j], p2);
					num2 +=  s - o;
					block3[num3] = *(counter+j);
					num3 += 1 - s;
			}

			//std::move(block1, block1+num1, counter);
			for(index p = 0; p < num1; p++){
				*counter = block1[p];
				rotations::rotate3(*counter, *offset2, *offset1);
				offset1++;
				offset2++;
				counter++;
			}

			for(index q = 0; q < num2; q++){
				*counter = block2[q];
				std::iter_swap(offset2, counter);
				counter++;
				offset2++;
			}
			//Should be able to bulk move this

//			std::copy(block3, (block3+num3), counter);
			//std::memcpy(static_cast<void *>(counter), &block3, sizeof(iter) * num3);std::move
			std::move(block3, (block3+num3), counter);
			counter+= num3;
			/*for(index p = 0; p < num3; p++){
				*counter = block3[p];
				counter++;
			}*/
			num1 = 0;
			num2 = 0;
			num3 = 0;

		}
		
		rotations::rotate3(*offset2, *offset1, *(end-1));
		offset2++;
		std::iter_swap(end-2, offset2);
		*ret1 = offset1;
		*ret2 = offset2;
	}

	//Multi-Pivot part 
	template< typename iter, typename Compare>
	struct Dual_Lomuto_Block_partition {
		static inline void partition(iter begin, iter end, iter* p1, iter* p2, Compare less) {
			iter* pivots = median::mp_tertiles_of_3_pivots_2(begin, end-1, less);
			dual_lomuto_block_partition_simple(begin, end, pivots, less, p1, p2);
		}
	};

	template< typename iter, typename Compare>
	struct Dual_Lomuto_Block_partition_mo5 {
		static inline void partition(iter begin, iter end, iter* p1, iter* p2, Compare less) {
			iter* pivots = median::mp_tertiles_of_5_pivots_2(begin, end-1, less);
			dual_lomuto_block_partition_simple(begin, end, pivots, less, p1, p2);
		}
	};

	template< typename iter, typename Compare>
	struct Dual_Lomuto_Block_partition_merge {
		static inline void partition(iter begin, iter end, iter* p1, iter* p2, Compare less) {
			iter* pivots = median::mp_tertiles_of_3_pivots_2(begin, end-1, less);
			dual_lomuto_block_partition_merge(begin, end, pivots, less, p1, p2);
		}
	};		

	template< typename iter, typename Compare>
	struct Dual_Lomuto_Block_partition_while {
		static inline void partition(iter begin, iter end, iter* p1, iter* p2, Compare less) {
			iter* pivots = median::mp_tertiles_of_3_pivots_2(begin, end-1, less);
			dual_lomuto_block_partition_while(begin, end, pivots, less, p1, p2);
		}
	};	

	template< typename iter, typename Compare>
	struct Dual_Lomuto_Block_partition_elements {
		static inline void partition(iter begin, iter end, iter* p1, iter* p2, Compare less) {
			iter* pivots = median::mp_tertiles_of_3_pivots_2(begin, end-1, less);
			dual_lomuto_block_partition_simple_elements(begin, end, pivots, less, p1, p2);
		}
	};		

	template< typename iter, typename Compare>
	struct Dual_Lomuto_Block_partition_elements_optimized {
		static inline void partition(iter begin, iter end, iter* p1, iter* p2, Compare less) {
			iter* pivots = median::mp_third_fifth_element(begin, end-1, less);
			dual_lomuto_block_partition_elements(begin, end, pivots, less, p1, p2);
		}
	};		


	template< typename iter, typename Compare>
	struct Dual_Lomuto_Block_partition_elements_move {
		static inline void partition(iter begin, iter end, iter* p1, iter* p2, Compare less) {
			iter* pivots = median::mp_tertiles_of_3_pivots_2(begin, end-1, less);
			dual_lomuto_block_partition_simple_elements_move(begin, end, pivots, less, p1, p2);
		}
	};

	//Three different pivot selection strategies

	//Third and fifth
	//*******************************************
	template< typename iter, typename Compare>
	struct Dual_Lomuto_Block_partition_third_fifth {
		static inline void partition(iter begin, iter end, iter* p1, iter* p2, Compare less) {
			iter* pivots = median::mp_third_fifth_element(begin, end-1, less);
			dual_lomuto_block_partition_simple(begin, end, pivots, less, p1, p2);
		}
	};

	template< typename iter, typename Compare>
	struct Dual_Lomuto_Block_partition_merge_third_fifth {
		static inline void partition(iter begin, iter end, iter* p1, iter* p2, Compare less) {
			iter* pivots = median::mp_third_fifth_element(begin, end-1, less);
			dual_lomuto_block_partition_merge(begin, end, pivots, less, p1, p2);
		}
	};		

	template< typename iter, typename Compare>
	struct Dual_Lomuto_Block_partition_while_third_fifth {
		static inline void partition(iter begin, iter end, iter* p1, iter* p2, Compare less) {
			iter* pivots = median::mp_third_fifth_element(begin, end-1, less);
			dual_lomuto_block_partition_while(begin, end, pivots, less, p1, p2);
		}
	};	

	template< typename iter, typename Compare>
	struct Dual_Lomuto_Block_partition_elements_third_fifth {
		static inline void partition(iter begin, iter end, iter* p1, iter* p2, Compare less) {
			iter* pivots = median::mp_third_fifth_element(begin, end-1, less);
			dual_lomuto_block_partition_simple_elements(begin, end, pivots, less, p1, p2);
		}
	};		


	template< typename iter, typename Compare>
	struct Dual_Lomuto_Block_partition_elements_move_third_fifth {
		static inline void partition(iter begin, iter end, iter* p1, iter* p2, Compare less) {
			iter* pivots = median::mp_third_fifth_element(begin, end-1, less);
			dual_lomuto_block_partition_simple_elements_move(begin, end, pivots, less, p1, p2);
		}
	};


	//Second and forth
	//*******************************************
	template< typename iter, typename Compare>
	struct Dual_Lomuto_Block_partition_second_forth {
		static inline void partition(iter begin, iter end, iter* p1, iter* p2, Compare less) {
			iter* pivots = median::mp_second_forth_element(begin, end-1, less);
			dual_lomuto_block_partition_simple(begin, end, pivots, less, p1, p2);
		}
	};

	template< typename iter, typename Compare>
	struct Dual_Lomuto_Block_partition_merge_second_forth {
		static inline void partition(iter begin, iter end, iter* p1, iter* p2, Compare less) {
			iter* pivots = median::mp_second_forth_element(begin, end-1, less);
			dual_lomuto_block_partition_merge(begin, end, pivots, less, p1, p2);
		}
	};		

	template< typename iter, typename Compare>
	struct Dual_Lomuto_Block_partition_while_second_forth {
		static inline void partition(iter begin, iter end, iter* p1, iter* p2, Compare less) {
			iter* pivots = median::mp_second_forth_element(begin, end-1, less);
			dual_lomuto_block_partition_while(begin, end, pivots, less, p1, p2);
		}
	};	

	template< typename iter, typename Compare>
	struct Dual_Lomuto_Block_partition_elements_second_forth {
		static inline void partition(iter begin, iter end, iter* p1, iter* p2, Compare less) {
			iter* pivots = median::mp_second_forth_element(begin, end-1, less);
			dual_lomuto_block_partition_simple_elements(begin, end, pivots, less, p1, p2);
		}
	};		


	template< typename iter, typename Compare>
	struct Dual_Lomuto_Block_partition_elements_move_second_forth {
		static inline void partition(iter begin, iter end, iter* p1, iter* p2, Compare less) {
			iter* pivots = median::mp_second_forth_element(begin, end-1, less);
			dual_lomuto_block_partition_simple_elements_move(begin, end, pivots, less, p1, p2);
		}
	};

	//First and forth
	//*******************************************
	template< typename iter, typename Compare>
	struct Dual_Lomuto_Block_partition_first_forth {
		static inline void partition(iter begin, iter end, iter* p1, iter* p2, Compare less) {
			iter* pivots = median::mp_first_forth_element(begin, end-1, less);
			dual_lomuto_block_partition_simple(begin, end, pivots, less, p1, p2);
		}
	};

	template< typename iter, typename Compare>
	struct Dual_Lomuto_Block_partition_merge_first_forth {
		static inline void partition(iter begin, iter end, iter* p1, iter* p2, Compare less) {
			iter* pivots = median::mp_first_forth_element(begin, end-1, less);
			dual_lomuto_block_partition_merge(begin, end, pivots, less, p1, p2);
		}
	};		

	template< typename iter, typename Compare>
	struct Dual_Lomuto_Block_partition_while_first_forth {
		static inline void partition(iter begin, iter end, iter* p1, iter* p2, Compare less) {
			iter* pivots = median::mp_first_forth_element(begin, end-1, less);
			dual_lomuto_block_partition_while(begin, end, pivots, less, p1, p2);
		}
	};	

	template< typename iter, typename Compare>
	struct Dual_Lomuto_Block_partition_elements_first_forth {
		static inline void partition(iter begin, iter end, iter* p1, iter* p2, Compare less) {
			iter* pivots = median::mp_first_forth_element(begin, end-1, less);
			dual_lomuto_block_partition_simple_elements(begin, end, pivots, less, p1, p2);
		}
	};		


	template< typename iter, typename Compare>
	struct Dual_Lomuto_Block_partition_elements_move_first_forth {
		static inline void partition(iter begin, iter end, iter* p1, iter* p2, Compare less) {
			iter* pivots = median::mp_first_forth_element(begin, end-1, less);
			dual_lomuto_block_partition_simple_elements_move(begin, end, pivots, less, p1, p2);
		}
	};

	//First and seventh
	//*******************************************
	template< typename iter, typename Compare>
	struct Dual_Lomuto_Block_partition_first_seventh {
		static inline void partition(iter begin, iter end, iter* p1, iter* p2, Compare less) {
			iter* pivots = median::mp_first_seventh_element(begin, end-1, less);
			dual_lomuto_block_partition_simple(begin, end, pivots, less, p1, p2);
		}
	};

	template< typename iter, typename Compare>
	struct Dual_Lomuto_Block_partition_merge_first_seventh {
		static inline void partition(iter begin, iter end, iter* p1, iter* p2, Compare less) {
			iter* pivots = median::mp_first_seventh_element(begin, end-1, less);
			dual_lomuto_block_partition_merge(begin, end, pivots, less, p1, p2);
		}
	};		

	template< typename iter, typename Compare>
	struct Dual_Lomuto_Block_partition_while_first_seventh {
		static inline void partition(iter begin, iter end, iter* p1, iter* p2, Compare less) {
			iter* pivots = median::mp_first_seventh_element(begin, end-1, less);
			dual_lomuto_block_partition_while(begin, end, pivots, less, p1, p2);
		}
	};	

	template< typename iter, typename Compare>
	struct Dual_Lomuto_Block_partition_elements_first_seventh {
		static inline void partition(iter begin, iter end, iter* p1, iter* p2, Compare less) {
			iter* pivots = median::mp_first_seventh_element(begin, end-1, less);
			dual_lomuto_block_partition_simple_elements(begin, end, pivots, less, p1, p2);
		}
	};		


	template< typename iter, typename Compare>
	struct Dual_Lomuto_Block_partition_elements_move_first_seventh {
		static inline void partition(iter begin, iter end, iter* p1, iter* p2, Compare less) {
			iter* pivots = median::mp_first_seventh_element(begin, end-1, less);
			dual_lomuto_block_partition_simple_elements_move(begin, end, pivots, less, p1, p2);
		}
	};

template<typename iter, typename Compare>
inline void lomuto_2_partition(iter begin, iter end, iter* pivot_positions, Compare less,iter* ret1, iter* ret2){
	typedef typename std::iterator_traits<iter>::difference_type index;
	typedef typename std::iterator_traits<iter>::value_type val;
	val block1[BLOCKSIZE];
	int num1 = 0;
	val block2[BLOCKSIZE];
	int num2 = 0;
	val block3[BLOCKSIZE];
	int num3 = 0;
	iter last = end-1;
	std::iter_swap(pivot_positions[0], last);
	const typename std::iterator_traits<iter>::value_type & p1 = *last;
	pivot_positions[0] = last;
	last--;
	iter offset1 = begin;
	std::iter_swap(pivot_positions[1], last);
	const typename std::iterator_traits<iter>::value_type & p2 = *last;
	pivot_positions[1] = last;
	last--;
	iter offset2 = begin;
	iter counter = begin;
	while (last - counter + 1 > BLOCKSIZE) {
		for(index j = 0;j < BLOCKSIZE; j++) {
			block1[num1] = *(counter+j);
			int o1 = less(counter[j], p1);
			num1+= o1;
			block2[num2] = *(counter+j);
			int o2 = less(counter[j], p2);
			num2 += o2 - o1;
			block3[num3] = *(counter+j);
			num3 += 1 - o2;
		}
		for(index p = 0; p < num1; p++){
			*counter = block1[p];
			rotations::rotate3(*counter, *offset2, *offset1);
			offset2++;
			offset1++;
			counter++;
		}
		num1 = 0;
		for(index p = 0; p < num2; p++){
			*counter = block2[p];
			std::iter_swap(offset2, counter);
			offset2++;
			counter++;
		}
		num2 = 0;
		for(index p = 0; p < num3; p++){
			*counter = block3[p];
			counter++;
		}
		num3 = 0;
	}
		for(index j = 0;j < (last-counter+1); j++) {
			block1[num1] = *(counter+j);
			int o1 = less(counter[j], p1);
			num1+= o1;
			block2[num2] = *(counter+j);
			int o2 = less(counter[j], p2);
			num2 += o2 - o1;
			block3[num3] = *(counter+j);
			num3 += 1 - o2;
		}
		for(index p = 0; p < num1; p++){
			*counter = block1[p];
			rotations::rotate3(*counter, *offset2, *offset1);
			offset2++;
			offset1++;
			counter++;
		}
		for(index p = 0; p < num2; p++){
			*counter = block2[p];
			std::iter_swap(offset2, counter);
			offset2++;
			counter++;
		}
		for(index p = 0; p < num3; p++){
			*counter = block3[p];
			counter++;
		}
	rotations::rotate3(*offset2, *offset1,  *(end-1));
	offset2++;
	std::iter_swap(end-2, offset2);
	*ret1 = offset1;
	*ret2 = offset2;
}

	template< typename iter, typename Compare>
	struct Dual_Lomuto_Block_partition_elements_generated_test {
		static inline void partition(iter begin, iter end, iter* p1, iter* p2, Compare less) {
			iter* pivots = median::mp_first_seventh_element(begin, end-1, less);
			lomuto_2_partition(begin, end, pivots, less, p1, p2);
		}
	};

	//Multi-Pivot part 
	template< typename iter, typename Compare>
	struct Dual_Pivot_Inline_Hoare_Block_partition_simple {
		static inline void partition(iter begin, iter end, iter* p1, iter* p2, Compare less) {
			iter* pivots = median::mp_tertiles_of_3_pivots_2(begin, end-1, less);
			dual_pivot_inline_block_partition_simple(begin, end, pivots, less, p1, p2);
		}
	};

/*
	template<typename iter, typename Compare>
	inline iter hoare_block_partition_simple_elements(iter begin, iter end, iter pivot_position, Compare less) {
		typedef typename std::iterator_traits<iter>::difference_type index;
		typedef typename std::iterator_traits<iter>::value_type val;
		val indexL[BLOCKSIZE], indexR[BLOCKSIZE];

		
		iter last = end - 1;
		std::iter_swap(pivot_position, last);
		const typename std::iterator_traits<iter>::value_type & pivot = *last;
		pivot_position = last;
		last--;

		int num_left = 0;
		int num_right = 0;
		int start_left = 0;
		int start_right = 0;
		int num;
		//main loop
		while (last - begin + 1 > 2 * BLOCKSIZE)
		{
			//Compare and store in buffers
			if (num_left == 0) {
				start_left = 0;
				for (index j = 0; j < BLOCKSIZE; j++) {
					indexL[num_left] = begin[j];
					num_left += (!(less(begin[j], pivot)));				
				}
			}
			if (num_right == 0) {
				start_right = 0;
				for (index j = 0; j < BLOCKSIZE; j++) {
					indexR[num_right] = *(last-j);
					num_right += !(less(pivot, *(last - j)));				
				}
			}

			//Here We simply need to clean the blocks instead
			//But how do we do that??
			for(int j = 0; j < num_left){
				continue;
			}
			//rearrange elements
			num = std::min(num_left, num_right);
			for (int j = 0; j < num; j++)
				std::iter_swap(begin + indexL[start_left + j], last - indexR[start_right + j]);

			num_left -= num;
			num_right -= num;
			start_left += num;
			start_right += num;
			begin += (num_left == 0) ? BLOCKSIZE : 0;
			last -= (num_right == 0) ? BLOCKSIZE : 0;

		}//end main loop

		//Compare and store in buffers final iteration
		index shiftR = 0, shiftL = 0;
		if (num_right == 0 && num_left == 0) {	//for small arrays or in the unlikely case that both buffers are empty
			shiftL = ((last - begin) + 1) / 2;
			shiftR = (last - begin) + 1 - shiftL;
			assert(shiftL >= 0); assert(shiftL <= BLOCKSIZE);
			assert(shiftR >= 0); assert(shiftR <= BLOCKSIZE);
			start_left = 0; start_right = 0;
			for (index j = 0; j < shiftL; j++) {
				indexL[num_left] = j;
				num_left += (!less(begin[j], pivot));
				indexR[num_right] = j;
				num_right += !less(pivot, *(last - j));
			}
			if (shiftL < shiftR)
			{
				assert(shiftL + 1 == shiftR);
				indexR[num_right] = shiftR - 1;
				num_right += !less(pivot, *(last - shiftR + 1));
			}
		}
		else if (num_right != 0) {
			shiftL = (last - begin) - BLOCKSIZE + 1;
			shiftR = BLOCKSIZE;
			assert(shiftL >= 0); assert(shiftL <= BLOCKSIZE); assert(num_left == 0);
			start_left = 0;
			for (index j = 0; j < shiftL; j++) {
				indexL[num_left] = j;
				num_left += (!less(begin[j], pivot));
			}
		}
		else {
			shiftL = BLOCKSIZE;
			shiftR = (last - begin) - BLOCKSIZE + 1;
			assert(shiftR >= 0); assert(shiftR <= BLOCKSIZE); assert(num_right == 0);
			start_right = 0;
			for (index j = 0; j < shiftR; j++) {
				indexR[num_right] = j;
				num_right += !(less(pivot, *(last - j)));
			}
		}

		//rearrange final iteration
		num = std::min(num_left, num_right);
		for (int j = 0; j < num; j++)
			std::iter_swap(begin + indexL[start_left + j], last - indexR[start_right + j]);

		num_left -= num;
		num_right -= num;
		start_left += num;
		start_right += num;
		begin += (num_left == 0) ? shiftL : 0;
		last -= (num_right == 0) ? shiftR : 0;			
		//end final iteration


		//rearrange elements remaining in buffer
		if (num_left != 0)
		{
			
			assert(num_right == 0);
			int lowerI = start_left + num_left - 1;
			index upper = last - begin;
			//search first element to be swapped
			while (lowerI >= start_left && indexL[lowerI] == upper) {
				upper--; lowerI--;
			}
			while (lowerI >= start_left)
				std::iter_swap(begin + upper--, begin + indexL[lowerI--]);

			std::iter_swap(pivot_position, begin + upper + 1); // fetch the pivot 
			return begin + upper + 1;
		}
		else if (num_right != 0) {
			assert(num_left == 0);
			int lowerI = start_right + num_right - 1;
			index upper = last - begin;
			//search first element to be swapped
			while (lowerI >= start_right && indexR[lowerI] == upper) {
				upper--; lowerI--;
			}
			
			while (lowerI >= start_right)
				std::iter_swap(last - upper--, last - indexR[lowerI--]);

			std::iter_swap(pivot_position, last - upper);// fetch the pivot 
			return last - upper;
		}
		else { //no remaining elements
			assert(last + 1 == begin);
			std::iter_swap(pivot_position, begin);// fetch the pivot 
			return begin;
		}
	}
	
	template<typename iter, typename Compare>
	struct Hoare_block_partition_simple_elements {
		static inline iter partition(iter begin, iter end, Compare less) {
			//choose pivot
			iter mid = median::median_of_3(begin, end, less);
			//partition
			return hoare_block_partition_simple_elements(begin + 1, end - 1, mid, less);
		}
		static inline iter partition(iter begin, iter end, iter pivot, Compare less) {
			//partition
			return hoare_block_partition_simple_elements(begin + 1, end - 1, pivot, less);
		}
	};

*/
}
