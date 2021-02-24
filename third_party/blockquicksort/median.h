/******************************************************************************
* median.h
*
* Different pivot selection strategies
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
#define WORST_CASE_RANGE 6


namespace median {
	template<typename iter, typename Compare>
	inline void sort_pair(iter i1, iter i2, Compare less) {
		typedef typename std::iterator_traits<iter>::value_type T;
		bool smaller = less(*i2, *i1);
		T temp = std::move(smaller ? *i1 : temp);
		*i1 = std::move(smaller ? *i2 : *i1);
		*i2 = std::move(smaller ? temp : *i2);
	}

	template<typename iter, typename Compare>
	inline iter median_of_3(iter i1, iter i2, iter i3, Compare less) {
		sort_pair(i1, i2, less);
		sort_pair(i2, i3, less);
		sort_pair(i1, i2, less);
		return i2;
	}

	template<typename iter, typename Compare>
	inline iter median_of_3(iter begin, iter end, Compare less) {
		iter mid = begin + ((end - begin) / 2);
		sort_pair(begin, mid, less);
		sort_pair(mid, end - 1, less);
		sort_pair(begin, mid, less);
		return mid;
	}

	template<typename iter, typename Compare>
	inline iter median_of_5(iter begin, iter end, Compare less) {
		iter left = begin + (end - begin) / 4;
		iter mid = begin + (end - begin) / 2;
		iter right = begin + (3 * (end - begin)) / 4;
		iter last = end - 1;

		sort_pair(begin, left, less);
		sort_pair(right, last, less);
		sort_pair(begin, right, less);
		sort_pair(left, last, less);
		sort_pair(mid, right, less);
		sort_pair(left, mid, less);
		sort_pair(mid, right, less);
		return mid;
	}
	template<typename iter, typename Compare>
	inline iter median_of_5(iter i1, iter i2, iter i3 , iter i4, iter i5, Compare less) {
		sort_pair(i1, i2, less);
		sort_pair(i4, i5, less);
		sort_pair(i1, i4, less);
		sort_pair(i2, i5, less);
		sort_pair(i3, i4, less);
		sort_pair(i2, i3, less);
		sort_pair(i3, i4, less);
		return i3;
	}
	template<typename iter, typename Compare>
	inline iter median_of_5(iter begin, Compare less) {
		sort_pair(begin + 0, begin + 1, less);
		sort_pair(begin + 3, begin + 4, less);
		sort_pair(begin + 0, begin + 3, less);
		sort_pair(begin + 1, begin + 4, less);
		sort_pair(begin + 2, begin + 3, less);
		sort_pair(begin + 1, begin + 2, less);
		sort_pair(begin + 2, begin + 3, less);
		return begin + 2;
	}

	template<typename iter, typename Compare>
	inline iter median_of_3_medians_of_5(iter begin, iter end, Compare less) {
		assert(end - begin > 35);
		iter first = median_of_5(begin, less);
		iter middle = median_of_5(begin + (end - begin) / 2, less);
		iter last = median_of_5(end - 5, less);
		std::iter_swap(begin, first);
		std::iter_swap(last, end - 1);
		return median_of_3(begin, middle, end - 1, less);
	}

	template<typename iter, typename Compare>
	inline iter median_of_3_medians_of_3(iter begin, iter end, Compare less) {
		assert(end - begin > 30);
		iter first = median_of_3(begin, begin + 3, less);
		iter middle = median_of_3(begin + (end - begin) / 2, begin + (end - begin) / 2 + 3, less);
		iter last = median_of_3(end - 3, end, less);
		std::iter_swap(begin, first);
		std::iter_swap(last, end - 1);
		return median_of_3(begin, middle, end - 1, less);
	}

	template<typename iter, typename Compare>
	inline iter median_of_5_medians_of_5(iter begin, iter end, Compare less) {
		assert(end - begin > 70);
		iter left = begin + (end - begin) / 4 - 1;
		iter mid = begin + (end - begin) / 2 - 2;
		iter right = begin + (3 * (end - begin)) / 4 - 3;
		iter last = end - 5;
		iter first = median_of_5(begin, less);
		left = median_of_5(left, less);
		mid = median_of_5(mid, less);
		right = median_of_5(right, less);
		last = median_of_5(last, less);
		std::iter_swap(begin, first);
		std::iter_swap(last, end - 1);
		return median_of_5(begin, left, mid, right, end - 1, less);
	}


	template< typename iter, typename Compare>
	iter median_of_k(iter begin, iter end, Compare less, unsigned int k) {
		if (end - begin < k + 3)
		{
			return median_of_3(begin, end, less);
		}
		unsigned int step = (end - begin) / (k + 3);


		iter searchit_left = begin + step;
		iter searchit_right = end - step;
		iter placeit = begin;
		for (unsigned int j = 0; j < k / 2; j++) {
			std::iter_swap(placeit, searchit_left);
			placeit++;
			std::iter_swap(placeit, searchit_right);
			placeit++;
			searchit_left += step;
			searchit_right -= step;
		}
		std::iter_swap(placeit, begin + (end - begin) / 2);
		++placeit;
		//	std::cout << "searching median of : " << placeit - begin << ", elements: " << std::endl;
		iter middle = begin + (placeit - begin) / 2;
		std::nth_element(begin, middle, placeit, less);
		std::swap_ranges(middle + 1, placeit, end - k / 2);
		return middle;
	}

	template<unsigned int k, typename iter, typename Compare>
	iter median_of_k_medians_of_3(iter begin, iter end, Compare less) {
		if (end - begin < 3*(k + 3))
		{
			return median_of_k<k>(begin, end, less);
		}
		unsigned int step = (end - begin) / (k + 3);


		iter searchit_left = begin + step;
		iter searchit_right = end - step;
		iter placeit = begin;
		for (unsigned int j = 0; j < k / 2; j++) {
			std::iter_swap(placeit, median_of_3(searchit_left, searchit_left + 2, less));
			placeit++;
			std::iter_swap(placeit, median_of_3(searchit_right - 2, searchit_right, less));
			placeit++;
			searchit_left += step;
			searchit_right -= step;
		}
		iter middle = begin + (placeit - begin) / 2;
		std::iter_swap(placeit, median_of_3(middle - 1, middle + 1, less));
		++placeit;
		//	std::cout << "searching median of : " << placeit - begin << ", elements: " << std::endl;
		std::nth_element(begin, middle, placeit, less);
		std::swap_ranges(middle + 1, placeit, end - k / 2);
		return middle;
	}


	template<unsigned int k, typename iter, typename Compare>
	iter median_of_k(const iter& begin, const iter& end, Compare less) {
		if (end - begin < k)
		{
			return median_of_3(begin, end, less);
		}
	
		unsigned int step = (end - begin) / k;
		iter searchit_left = begin;
		iter searchit_right = end - 1;
		iter placeit = begin + 1;
		std::iter_swap(placeit, searchit_right);
		placeit++;
		searchit_left += step;
		searchit_right -= step;
		for (unsigned int j = 1; j < (k - 1)/2; j++) {
			std::iter_swap(placeit, searchit_left);
			placeit++;
			std::iter_swap(placeit, searchit_right);
			placeit++;
			searchit_left += step;
			searchit_right -= step;
		}
		std::iter_swap(placeit, begin + (end - begin) / 2);
		++placeit;
		assert(placeit == begin + k);
		
		//	std::cout << "searching median of : " << placeit - begin << ", elements: "  << std::endl;
		iter middle = begin + (placeit - begin) / 2;
		std::nth_element(begin, middle, placeit, less);		
		iter middle_p1 = middle + 1;
		assert(middle != begin + 1);
		assert(!(less(*middle , *begin )));
		std::swap_ranges(middle_p1, placeit, end - k/2);
		assert(!(less(*(end - 1), *middle)));
		return middle;
	}


	template<typename iter, typename Compare>
	inline iter* mp_random(iter begin, iter end, Compare less){
		iter sek1 = begin;
		iter sek2 = begin+1;
		sort_pair(sek1, sek2, less);
		
		/*
		int tmp = (end-begin)/4;
		iter sek1 = begin+tmp;
		iter sek2 = begin+(2*tmp);
		iter sek3 = begin+(3*tmp);
		sort_pair(begin, sek1, less);
		sort_pair(sek3, end, less);
		sort_pair(sek2, end, less);
		sort_pair(sek2, sek3, less);
		sort_pair(begin, sek3, less);
		sort_pair(begin, sek2, less);
		sort_pair(sek1, end, less);
		sort_pair(sek1, sek3, less);
		sort_pair(sek1, sek2, less);*/
		iter* ret = new iter[2];
		ret[0] = sek1;
		ret[1] = sek2; 
		return ret;
	}

	template<typename iter, typename Compare>
	inline iter* mp_tertiles_of_3_pivots_2(iter begin, iter end, Compare less){
		int tmp = (end-begin)/4;
		iter sek1 = begin+tmp;
		iter sek2 = begin+(2*tmp);
		iter sek3 = begin+(3*tmp);
		sort_pair(begin, sek1, less);
		sort_pair(sek3, end, less);
		sort_pair(sek2, end, less);
		sort_pair(sek2, sek3, less);
		sort_pair(begin, sek3, less);
		sort_pair(begin, sek2, less);
		sort_pair(sek1, end, less);
		sort_pair(sek1, sek3, less);
		sort_pair(sek1, sek2, less);
		iter* ret = new iter[2];
		ret[0] = sek1;
		ret[1] = sek3; 
		return ret;
	}
	template<typename iter, typename Compare>
	inline iter* mp_tertiles_of_5_pivots_2(iter begin, iter end, Compare less){
		int tmp = (end-begin)/6;
		iter sek1 = begin+tmp;
		iter sek2 = begin+(2*tmp);
		iter sek3 = begin+(3*tmp);
		iter sek4 = begin+(4*tmp);
		iter sek5 = begin+(5*tmp);

		sort_pair(sek1, sek2, less);
		sort_pair(sek4, sek5, less);
		sort_pair(sek3, sek5, less);
		sort_pair(sek3, sek4, less);
		sort_pair(sek1, sek4, less);	
		sort_pair(sek1, sek3, less);
		sort_pair(sek2, sek5, less);
		sort_pair(sek2, sek4, less);
		sort_pair(sek2, sek3, less);
		assert(sek2 != sek4);
		iter* ret = new iter[2];
		ret[0] = sek2;
		ret[1] = sek4; 
		return ret;
	}

	template<typename iter, typename Compare>
	inline iter* mp_third_fifth_element(iter begin, iter end, Compare less){
		//Based on: http://jgamble.ripco.net/cgi-bin/nw.cgi?inputs=7&algorithm=best&output=macro
		int tmp = (end-begin)/6;
		iter sek0 = begin;
		iter sek1 = begin+tmp;
		iter sek2 = begin+(tmp*2);
		iter sek3 = begin+(tmp*3);
		iter sek4 = begin+(tmp*4);
		iter sek5 = begin+(tmp*5);
		iter sek6 = end-1;

		sort_pair(sek1, sek2, less);
		sort_pair(sek0, sek2, less);
		sort_pair(sek0, sek1, less);
		sort_pair(sek3, sek4, less);
		sort_pair(sek5, sek6, less);
		sort_pair(sek3, sek5, less);
		sort_pair(sek4, sek6, less);
		sort_pair(sek4, sek5, less);
		sort_pair(sek0, sek4, less);
		sort_pair(sek0, sek3, less);
		sort_pair(sek1, sek5, less);
		sort_pair(sek2, sek6, less);
		sort_pair(sek2, sek5, less);
		sort_pair(sek1, sek3, less);
		sort_pair(sek2, sek4, less);
		sort_pair(sek2, sek3, less);
		iter* ret = new iter[2];
		ret[0] = sek2;
		ret[1] = sek4; 
		return ret;
	}

	template<typename iter, typename Compare>
	inline iter* mp_second_forth_element(iter begin, iter end, Compare less){
		//Based on: http://jgamble.ripco.net/cgi-bin/nw.cgi?inputs=7&algorithm=best&output=macro
		int tmp = (end-begin)/6;
		iter sek0 = begin;
		iter sek1 = begin+tmp;
		iter sek2 = begin+(tmp*2);
		iter sek3 = begin+(tmp*3);
		iter sek4 = begin+(tmp*4);
		iter sek5 = begin+(tmp*5);
		iter sek6 = end-1;

		sort_pair(sek1, sek2, less);
		sort_pair(sek0, sek2, less);
		sort_pair(sek0, sek1, less);
		sort_pair(sek3, sek4, less);
		sort_pair(sek5, sek6, less);
		sort_pair(sek3, sek5, less);
		sort_pair(sek4, sek6, less);
		sort_pair(sek4, sek5, less);
		sort_pair(sek0, sek4, less);
		sort_pair(sek0, sek3, less);
		sort_pair(sek1, sek5, less);
		sort_pair(sek2, sek6, less);
		sort_pair(sek2, sek5, less);
		sort_pair(sek1, sek3, less);
		sort_pair(sek2, sek4, less);
		sort_pair(sek2, sek3, less);
		iter* ret = new iter[2];
		ret[0] = sek1;
		ret[1] = sek3; 
		return ret;
	}


	template<typename iter, typename Compare>
	inline iter* mp_first_forth_element(iter begin, iter end, Compare less){
		//Based on: http://jgamble.ripco.net/cgi-bin/nw.cgi?inputs=7&algorithm=best&output=macro
		int tmp = (end-begin)/6;
		iter sek0 = begin;
		iter sek1 = (begin+tmp);
		iter sek2 = begin+(tmp*2);
		iter sek3 = begin+(tmp*3);
		iter sek4 = begin+(tmp*4);
		iter sek5 = begin+(tmp*5);
		iter sek6 = end-1;

		sort_pair(sek1, sek2, less);
		sort_pair(sek0, sek2, less);
		sort_pair(sek0, sek1, less);
		sort_pair(sek3, sek4, less);
		sort_pair(sek5, sek6, less);
		sort_pair(sek3, sek5, less);
		sort_pair(sek4, sek6, less);
		sort_pair(sek4, sek5, less);
		sort_pair(sek0, sek4, less);
		sort_pair(sek0, sek3, less);
		sort_pair(sek1, sek5, less);
		sort_pair(sek2, sek6, less);
		sort_pair(sek2, sek5, less);
		sort_pair(sek1, sek3, less);
		sort_pair(sek2, sek4, less);
		sort_pair(sek2, sek3, less);
		iter* ret = new iter[2];
		ret[0] = sek0;
		ret[1] = sek3; 
		return ret;
	}


	//Something wrong here
	//Might be the last element?
	template<typename iter, typename Compare>
	inline iter* mp_first_seventh_element(iter begin, iter end, Compare less){
		//Based on: http://jgamble.ripco.net/cgi-bin/nw.cgi?inputs=7&algorithm=best&output=macro
		int tmp = (end-begin)/6;
		iter sek0 = begin;
		iter sek1 = begin+tmp;
		iter sek2 = begin+(tmp*2);
		iter sek3 = begin+(tmp*3);
		iter sek4 = begin+(tmp*4);
		iter sek5 = begin+(tmp*5);
		iter sek6 = end-1;
		std::cout << "End: " << *end << std::endl;
		std::cout << "Sek0: " << *sek0 << std::endl;
		std::cout << "Sek1: " << *sek1 << std::endl;
		std::cout << "Sek2: " << *sek2 << std::endl;
		std::cout << "Sek3: " << *sek3 << std::endl;
		std::cout << "Sek4: " << *sek4 << std::endl;
		std::cout << "Sek5: " << *sek5 << std::endl;
		std::cout << "Sek6: " << *sek6 << std::endl;

		sort_pair(sek1, sek2, less);
		sort_pair(sek0, sek2, less);
		sort_pair(sek0, sek1, less);
		sort_pair(sek3, sek4, less);
		sort_pair(sek5, sek6, less);
		sort_pair(sek3, sek5, less);
		sort_pair(sek4, sek6, less);
		sort_pair(sek4, sek5, less);
		sort_pair(sek0, sek4, less);
		sort_pair(sek0, sek3, less);
		sort_pair(sek1, sek5, less);
		sort_pair(sek2, sek6, less);
		sort_pair(sek2, sek5, less);
		sort_pair(sek1, sek3, less);
		sort_pair(sek2, sek4, less);
		sort_pair(sek2, sek3, less);
		iter* ret = new iter[2];
		std::cout << "End: SECOND  " << *end << std::endl;
		std::cout << "Sek0: " << *sek0 << std::endl;
		std::cout << "Sek1: " << *sek1 << std::endl;
		std::cout << "Sek2: " << *sek2 << std::endl;
		std::cout << "Sek3: " << *sek3 << std::endl;
		std::cout << "Sek4: " << *sek4 << std::endl;
		std::cout << "Sek5: " << *sek5 << std::endl;
		std::cout << "Sek6: " << *sek6 << std::endl;
		ret[0] = sek0;
		ret[1] = sek6; 
		std::cout << "Sek0: " << *sek0 << std::endl;
		std::cout << "Sek6: " << *sek6 << std::endl;
		return ret;
	}
}