/******************************************************************************
* insertionsort.h
*
* Routines for insertionsort from stl_algo.h (GCC Version 4.7.2)
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
#include <iterator>


namespace insertionsort {

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
	template<typename _RandomAccessIterator, typename _Compare>
	void
		insertion_sort_tuned(_RandomAccessIterator first,
			_RandomAccessIterator last, _Compare comp)
	{
		if (last - first <  2) return;

		leanswap(first, first + 1, comp);
		for (_RandomAccessIterator i = first + 2; i != last; ++i)
		{
			_RandomAccessIterator middle = first + (i - first) / 2;
			if (comp(*i, *middle))
			{
				if (comp(*i, *first))
				{
					typename std::iterator_traits<_RandomAccessIterator>::value_type
						val = std::move(*i);
					std::move_backward(first, i, i + 1);
					*first = std::move(val);
				}
				else
				{
					typename std::iterator_traits<_RandomAccessIterator>::value_type
						val = std::move(*i);
					std::move_backward(middle, i, i + 1);
					*middle = std::move(val);
					unguarded_linear_insert(middle, comp);
				}
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

		for (_RandomAccessIterator i = first; i != last; ++i)
			unguarded_linear_insert(i);
	}

	/// This is a helper function for the sort routine.
	template<typename _RandomAccessIterator, typename _Compare>
	inline void
		unguarded_insertion_sort(_RandomAccessIterator first,
			_RandomAccessIterator last, _Compare comp)
	{

		for (_RandomAccessIterator i = first; i != last; ++i)
			unguarded_linear_insert(i, comp);
	}

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

}