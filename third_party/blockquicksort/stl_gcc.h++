/******************************************************************************
* std_gcc.h
*
* std::sort from stl_algo.h (GCC Version 4.7.2)
*
******************************************************************************
* replaced macros and changed some variable names
* Copyright (C) 2016 Armin Weiﬂ <armin.weiss@fmi.uni-stuttgart.de>
*/

// Copyright (C) 2001-2013 Free Software Foundation, Inc.
//
// This file is part of the GNU ISO C++ Library.  This library is free
// software; you can redistribute it and/or modify it under the
// terms of the GNU General Public License as published by the
// Free Software Foundation; either version 3, or (at your option)
// any later version.

// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// Under Section 7 of GPL version 3, you are granted additional
// permissions described in the GCC Runtime Library Exception, version
// 3.1, as published by the Free Software Foundation.

// You should have received a copy of the GNU General Public License and
// a copy of the GCC Runtime Library Exception along with this program;
// see the files COPYING3 and COPYING.RUNTIME respectively.  If not, see
// <http://www.gnu.org/licenses/>.

/*
*
* Copyright (c) 1994
* Hewlett-Packard Company
*
* Permission to use, copy, modify, distribute and sell this software
* and its documentation for any purpose is hereby granted without fee,
* provided that the above copyright notice appear in all copies and
* that both that copyright notice and this permission notice appear
* in supporting documentation.  Hewlett-Packard Company makes no
* representations about the suitability of this software for any
* purpose.  It is provided "as is" without express or implied warranty.
*
*
* Copyright (c) 1996
* Silicon Graphics Computer Systems, Inc.
*
* Permission to use, copy, modify, distribute and sell this software
* and its documentation for any purpose is hereby granted without fee,
* provided that the above copyright notice appear in all copies and
* that both that copyright notice and this permission notice appear
* in supporting documentation.  Silicon Graphics makes no
* representations about the suitability of this software for any
* purpose.  It is provided "as is" without express or implied warranty.
*/


#pragma once
#include <Iterator>
namespace stl_gcc {


	/// Swaps the median value of *a, *b and *c to *a
	template<typename _Iterator>
	void
		move_median_first(_Iterator a, _Iterator b, _Iterator c)
	{

			if (*a < *b)
			{
				if (*b < *c)
					std::iter_swap(a, b);
				else if (*a < *c)
					std::iter_swap(a, c);
			}
			else if (*a < *c)
				return;
			else if (*b < *c)
				std::iter_swap(a, c);
			else
				std::iter_swap(a, b);
	}

	/// Swaps the median value of *a, *b and *c under comp to *a
	template<typename _Iterator, typename _Compare>
	void
		move_median_first(_Iterator a, _Iterator b, _Iterator c,
			_Compare comp)
	{

			if (comp(*a, *b))
			{
				if (comp(*b, *c))
					std::iter_swap(a, b);
				else if (comp(*a, *c))
					std::iter_swap(a, c);
			}
			else if (comp(*a, *c))
				return;
			else if (comp(*b, *c))
				std::iter_swap(a, c);
			else
				std::iter_swap(a, b);
	}


	/// This is a helper function for the sort routine.
	template<typename _RandomAccessIterator>
	void
		unguarded_linear_insert(_RandomAccessIterator last)
	{
		typename std::iterator_traits<_RandomAccessIterator>::value_type
			val = std::move(*last);
		_RandomAccessIterator next = last;
		--next;
		while (val < *next)
		{
			*last = std::move(*next);
			last = next;
			--next;
		}
		*last = std::move(val);
	}

	/// This is a helper function for the sort routine.
	template<typename _RandomAccessIterator, typename _Compare>
	void
		unguarded_linear_insert(_RandomAccessIterator last,
			_Compare comp)
	{
		typename std::iterator_traits<_RandomAccessIterator>::value_type
			val = std::move(*last);
		_RandomAccessIterator next = last;
		--next;
		while (comp(val, *next))
		{
			*last = std::move(*next);
			last = next;
			--next;
		}
		*last = std::move(val);
	}

	/// This is a helper function for the sort routine.
	template<typename _RandomAccessIterator>
	void
		insertion_sort(_RandomAccessIterator first,
			_RandomAccessIterator last)
	{
		if (first == last)
			return;

		for (_RandomAccessIterator i = first + 1; i != last; ++i)
		{
			if (*i < *first)
			{
				typename std::iterator_traits<_RandomAccessIterator>::value_type
					val = std::move(*i);
				std::move_backward(first, i, i + 1);
				*first = std::move(val);
			}
			else
				unguarded_linear_insert(i);
		}
	}

	/// This is a helper function for the sort routine.
	template<typename _RandomAccessIterator, typename _Compare>
	void
		insertion_sort(_RandomAccessIterator first,
			_RandomAccessIterator last, _Compare comp)
	{
		if (first == last) return;

		for (_RandomAccessIterator i = first + 1; i != last; ++i)
		{
			if (comp(*i, *first))
			{
				typename std::iterator_traits<_RandomAccessIterator>::value_type
					val = std::move(*i);
				std::move_backward(first, i, i + 1);
				*first = std::move(val);
			}
			else
				unguarded_linear_insert(i, comp);
		}
	}

	/// This is a helper function for the sort routine.
	template<typename _RandomAccessIterator>
	inline void
		unguarded_insertion_sort(_RandomAccessIterator first,
			_RandomAccessIterator last)
	{
		typedef typename std::iterator_traits<_RandomAccessIterator>::value_type
			_ValueType;

		for (_RandomAccessIterator i = first; i != last; ++i)
			unguarded_linear_insert(i);
	}

	/// This is a helper function for the sort routine.
	template<typename _RandomAccessIterator, typename _Compare>
	inline void
		unguarded_insertion_sort(_RandomAccessIterator first,
			_RandomAccessIterator last, _Compare comp)
	{
		typedef typename std::iterator_traits<_RandomAccessIterator>::value_type
			_ValueType;

		for (_RandomAccessIterator i = first; i != last; ++i)
			unguarded_linear_insert(i, comp);
	}

	/**
	*  @doctodo
	*  This controls some aspect of the sort routines.
	*/
	enum { _S_threshold = 16 };

	/// This is a helper function for the sort routine.
	template<typename _RandomAccessIterator>
	void
		final_insertion_sort(_RandomAccessIterator first,
			_RandomAccessIterator last)
	{
		if (last - first > int(_S_threshold))
		{
			insertion_sort(first, first + int(_S_threshold));
			unguarded_insertion_sort(first + int(_S_threshold), last);
		}
		else
			insertion_sort(first, last);
	}

	/// This is a helper function for the sort routine.
	template<typename _RandomAccessIterator, typename _Compare>
	void
		final_insertion_sort(_RandomAccessIterator first,
			_RandomAccessIterator last, _Compare comp)
	{
		if (last - first > int(_S_threshold))
		{
			insertion_sort(first, first + int(_S_threshold), comp);
			unguarded_insertion_sort(first + int(_S_threshold), last,
				comp);
		}
		else
			insertion_sort(first, last, comp);
	}

	/// This is a helper function...
	template<typename _RandomAccessIterator, typename _Tp>
	_RandomAccessIterator
		unguarded_partition(_RandomAccessIterator first,
			_RandomAccessIterator last, const _Tp& pivot)
	{
		while (true)
		{
			while (*first < pivot)
				++first;
			--last;
			while (pivot < *last)
				--last;
			if (!(first < last))
				return first;
			std::iter_swap(first, last);
			++first;
		}
	}

	/// This is a helper function...
	template<typename _RandomAccessIterator, typename _Tp, typename _Compare>
	_RandomAccessIterator
		unguarded_partition(_RandomAccessIterator first,
			_RandomAccessIterator last,
			const _Tp& pivot, _Compare comp)
	{
		while (true)
		{
			while (comp(*first, pivot))
				++first;
			--last;
			while (comp(pivot, *last))
				--last;
			if (!(first < last))
				return first;
			std::iter_swap(first, last);
			++first;
		}
	}

	/// This is a helper function...
	template<typename _RandomAccessIterator>
	inline _RandomAccessIterator
		unguarded_partition_pivot(_RandomAccessIterator first,
			_RandomAccessIterator last)
	{
		_RandomAccessIterator mid = first + (last - first) / 2;
		move_median_first(first, mid, (last - 1));
		return unguarded_partition(first + 1, last, *first);
	}


	/// This is a helper function...
	template<typename _RandomAccessIterator, typename _Compare>
	inline _RandomAccessIterator
		unguarded_partition_pivot(_RandomAccessIterator first,
			_RandomAccessIterator last, _Compare comp)
	{
		_RandomAccessIterator mid = first + (last - first) / 2;
		move_median_first(first, mid, (last - 1), comp);
		return unguarded_partition(first + 1, last, *first, comp);
	}

	/// This is a helper function for the sort routine.
	template<typename _RandomAccessIterator, typename _Size>
	void
		introsort_loop(_RandomAccessIterator first,
			_RandomAccessIterator last,
			_Size depth_limit)
	{
		while (last - first > int(_S_threshold))
		{
			if (depth_limit == 0)
			{
				std::partial_sort(first, last, last);
				return;
			}
			--depth_limit;
			_RandomAccessIterator cut =
				unguarded_partition_pivot(first, last);
			introsort_loop(cut, last, depth_limit);
			last = cut;
		}
	}

	/// This is a helper function for the sort routine.
	template<typename _RandomAccessIterator, typename _Size, typename _Compare>
	void
		introsort_loop(_RandomAccessIterator first,
			_RandomAccessIterator last,
			_Size depth_limit, _Compare comp)
	{
		while (last - first > int(_S_threshold))
		{
			if (depth_limit == 0)
			{
			//	std::cout<<"partial sort_gcc"<<std::endl;
				std::partial_sort(first, last, last, comp);
				return;
			}
			--depth_limit;
			_RandomAccessIterator cut =
				unguarded_partition_pivot(first, last, comp);
			introsort_loop(cut, last, depth_limit, comp);
			last = cut;
		}
	}

	


	/**
	*  @brief Sort the elements of a sequence.
	*  @ingroup sorting_algorithms
	*  @param  first   An iterator.
	*  @param  last    Another iterator.
	*  @return  Nothing.
	*
	*  Sorts the elements in the range @p [first,last) in ascending order,
	*  such that for each iterator @e i in the range @p [first,last-1),
	*  *(i+1)<*i is false.
	*
	*  The relative ordering of equivalent elements is not preserved, use
	*  @p stable_sort() if this is needed.
	*/
	template<typename _RandomAccessIterator>
	inline void
		sort(_RandomAccessIterator first, _RandomAccessIterator last)
	{
		typedef typename std::iterator_traits<_RandomAccessIterator>::value_type
			_ValueType;

	

		if (first != last)
		{
			introsort_loop(first, last,
				lg(last - first) * 2);
			final_insertion_sort(first, last);
		}
	}

	/**
	*  @brief Sort the elements of a sequence using a predicate for comparison.
	*  @ingroup sorting_algorithms
	*  @param  first   An iterator.
	*  @param  last    Another iterator.
	*  @param  comp    A comparison functor.
	*  @return  Nothing.
	*
	*  Sorts the elements in the range @p [first,last) in ascending order,
	*  such that @p comp(*(i+1),*i) is false for every iterator @e i in the
	*  range @p [first,last-1).
	*
	*  The relative ordering of equivalent elements is not preserved, use
	*  @p stable_sort() if this is needed.
	*/
	template<typename _RandomAccessIterator, typename _Compare>
	inline void
		sort(_RandomAccessIterator first, _RandomAccessIterator last,
			_Compare comp)
	{
		typedef typename std::iterator_traits<_RandomAccessIterator>::value_type
			_ValueType;

		if (first != last)
		{
			introsort_loop(first, last,
				ilogb(last - first) * 2, comp);
			final_insertion_sort(first, last, comp);
		}
	}
	template<typename T>
	void sort(std::vector<T> &v) {
		stl_gcc::sort(v.begin(), v.end(), std::less<T>());
	}


}