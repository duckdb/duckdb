//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/blockquicksort.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cmath>

namespace duckdb {

#define BLOCKSIZE 128
#define IS_THRESH 20

/**
 * BlockQuickSort
 * Variant of QuickSort with less branch mispredictions.
 * From: https://arxiv.org/abs/1604.06697
 */
class BlockQuickSort {
private:
	/// This is a helper function for the sort routine.
	template <typename RandomAccessIterator>
	static void unguarded_linear_insert(RandomAccessIterator last) {
		typename std::iterator_traits<RandomAccessIterator>::value_type val = std::move(*last);
		RandomAccessIterator next = last;
		--next;
		while (val < *next) {
			*last = std::move(*next);
			last = next;
			--next;
		}
		*last = std::move(val);
	}

	/// This is a helper function for the sort routine.
	template <typename RandomAccessIterator, typename Compare>
	static void unguarded_linear_insert(RandomAccessIterator last, Compare comp) {
		typename std::iterator_traits<RandomAccessIterator>::value_type val = std::move(*last);
		RandomAccessIterator next = last;
		--next;
		while (comp(val, *next)) {
			*last = std::move(*next);
			last = next;
			--next;
		}
		*last = std::move(val);
	}

	/// This is a helper function for the sort routine.
	template <typename RandomAccessIterator>
	static void insertion_sort(RandomAccessIterator first, RandomAccessIterator last) {
		if (first == last) {
			return;
		}

		for (RandomAccessIterator i = first + 1; i != last; ++i) {
			if (*i < *first) {
				typename std::iterator_traits<RandomAccessIterator>::value_type val = std::move(*i);
				std::move_backward(first, i, i + 1);
				*first = std::move(val);
			} else {
				unguarded_linear_insert(i);
			}
		}
	}

	/// This is a helper function for the sort routine.
	template <typename RandomAccessIterator, typename Compare>
	static void insertion_sort(RandomAccessIterator first, RandomAccessIterator last, Compare comp) {
		if (first == last) {
			return;
		}

		for (RandomAccessIterator i = first + 1; i != last; ++i) {
			if (comp(*i, *first)) {
				typename std::iterator_traits<RandomAccessIterator>::value_type val = std::move(*i);
				std::move_backward(first, i, i + 1);
				*first = std::move(val);
			} else {
				unguarded_linear_insert(i, comp);
			}
		}
	}

	template <typename iter, typename Compare>
	static inline void sort_pair(iter i1, iter i2, Compare less) {
		typedef typename std::iterator_traits<iter>::value_type T;
		bool smaller = less(*i2, *i1);
		T temp = std::move(smaller ? *i1 : temp);
		*i1 = std::move(smaller ? *i2 : *i1);
		*i2 = std::move(smaller ? temp : *i2);
	}

	template <typename iter, typename Compare>
	static inline iter median_of_3(iter i1, iter i2, iter i3, Compare less) {
		sort_pair(i1, i2, less);
		sort_pair(i2, i3, less);
		sort_pair(i1, i2, less);
		return i2;
	}

	template <typename iter, typename Compare>
	static inline iter median_of_3(iter begin, iter end, Compare less) {
		iter mid = begin + ((end - begin) / 2);
		sort_pair(begin, mid, less);
		sort_pair(mid, end - 1, less);
		sort_pair(begin, mid, less);
		return mid;
	}

	template <typename iter, typename Compare>
	static inline iter median_of_5(iter i1, iter i2, iter i3, iter i4, iter i5, Compare less) {
		sort_pair(i1, i2, less);
		sort_pair(i4, i5, less);
		sort_pair(i1, i4, less);
		sort_pair(i2, i5, less);
		sort_pair(i3, i4, less);
		sort_pair(i2, i3, less);
		sort_pair(i3, i4, less);
		return i3;
	}

	template <typename iter, typename Compare>
	static inline iter median_of_5(iter begin, Compare less) {
		sort_pair(begin + 0, begin + 1, less);
		sort_pair(begin + 3, begin + 4, less);
		sort_pair(begin + 0, begin + 3, less);
		sort_pair(begin + 1, begin + 4, less);
		sort_pair(begin + 2, begin + 3, less);
		sort_pair(begin + 1, begin + 2, less);
		sort_pair(begin + 2, begin + 3, less);
		return begin + 2;
	}

	template <typename iter, typename Compare>
	static inline iter median_of_3_medians_of_3(iter begin, iter end, Compare less) {
		D_ASSERT(end - begin > 30);
		iter first = median_of_3(begin, begin + 3, less);
		iter middle = median_of_3(begin + (end - begin) / 2, begin + (end - begin) / 2 + 3, less);
		iter last = median_of_3(end - 3, end, less);
		std::iter_swap(begin, first);
		std::iter_swap(last, end - 1);
		return median_of_3(begin, middle, end - 1, less);
	}

	template <typename iter, typename Compare>
	static inline iter median_of_5_medians_of_5(iter begin, iter end, Compare less) {
		D_ASSERT(end - begin > 70);
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

	template <typename iter, typename Compare>
	static iter median_of_k(iter begin, iter end, Compare less, unsigned int k) {
		if (end - begin < k + 3) {
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

		iter middle = begin + (placeit - begin) / 2;
		std::nth_element(begin, middle, placeit, less);
		std::swap_ranges(middle + 1, placeit, end - k / 2);
		return middle;
	}

	template <unsigned int k, typename iter, typename Compare>
	static iter median_of_k(const iter &begin, const iter &end, Compare less) {
		if (end - begin < k) {
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
		for (unsigned int j = 1; j < (k - 1) / 2; j++) {
			std::iter_swap(placeit, searchit_left);
			placeit++;
			std::iter_swap(placeit, searchit_right);
			placeit++;
			searchit_left += step;
			searchit_right -= step;
		}
		std::iter_swap(placeit, begin + (end - begin) / 2);
		++placeit;
		D_ASSERT(placeit == begin + k);

		iter middle = begin + (placeit - begin) / 2;
		std::nth_element(begin, middle, placeit, less);
		iter middle_p1 = middle + 1;
		D_ASSERT(middle != begin + 1);
		D_ASSERT(!(less(*middle, *begin)));
		std::swap_ranges(middle_p1, placeit, end - k / 2);
		D_ASSERT(!(less(*(end - 1), *middle)));
		return middle;
	}

	template <typename iter, typename Compare>
	static inline iter hoare_block_partition_unroll_loop(iter begin, iter end, iter pivot_pos, Compare less) {
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
		// main loop
		while (last - begin + 1 > 2 * BLOCKSIZE) {
			// Compare and store in buffers
			if (num_left == 0) {
				start_left = 0;
				for (j = 0; j < BLOCKSIZE;) {
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
				for (j = 0; j < BLOCKSIZE;) {
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
			// rearrange elements
			num = std::min(num_left, num_right);
			if (num != 0) {
				*hole = std::move(*(begin + indexL[start_left]));
				*(begin + indexL[start_left]) = std::move(*(last - indexR[start_right]));
				for (j = 1; j < num; j++) {
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
		} // end main loop

		// Compare and store in buffers final iteration
		int shiftR = 0, shiftL = 0;
		if (num_right == 0 && num_left == 0) { // for small arrays or in the unlikely case that both buffers are empty
			shiftL = (int)((last - begin) + 1) / 2;
			shiftR = (int)(last - begin) + 1 - shiftL;
			D_ASSERT(shiftL >= 0);
			D_ASSERT(shiftL <= BLOCKSIZE);
			D_ASSERT(shiftR >= 0);
			D_ASSERT(shiftR <= BLOCKSIZE);
			start_left = 0;
			start_right = 0;
			for (j = 0; j < shiftL; j++) {
				indexL[num_left] = j;
				num_left += (!less(begin[j], pivot));
				indexR[num_right] = j;
				num_right += !less(pivot, *(last - j));
			}
			if (shiftL < shiftR) {
				D_ASSERT(shiftL + 1 == shiftR);
				indexR[num_right] = j;
				num_right += !less(pivot, *(last - j));
			}
		} else if ((last - begin) + 1 - BLOCKSIZE <=
		               2 * BLOCKSIZE - (start_right + num_right + start_left + num_left) &&
		           (num_right + num_left) < BLOCKSIZE / 3) {
			int upper_right = start_right + num_right;
			int upper_left = start_left + num_left;
			D_ASSERT((last - begin) - BLOCKSIZE + 1 > 0);
			shiftL = (int)(((last - begin) + 1 - BLOCKSIZE) / 2); // +2*(num_right + num_left)  //- num_left
			shiftR = (int)(last - begin) - BLOCKSIZE + 1 - shiftL;
			if (shiftL > BLOCKSIZE - upper_left) {
				shiftR += shiftL - (BLOCKSIZE - upper_left);
				shiftL = BLOCKSIZE - upper_left;
			} else if (shiftL < 0) {
				shiftR -= shiftL;
				shiftL = 0;
			}
			if (shiftR > BLOCKSIZE - upper_right) {
				shiftL += shiftR - (BLOCKSIZE - upper_right);
				shiftR = BLOCKSIZE - upper_right;
			} else if (shiftR < 0) {
				shiftL -= shiftR;
				shiftR = 0;
			}

			D_ASSERT(shiftL + shiftR + BLOCKSIZE == (last - begin) + 1);
			D_ASSERT(shiftL >= 0);
			D_ASSERT(shiftL <= BLOCKSIZE - upper_left);
			D_ASSERT(shiftR >= 0);
			D_ASSERT(shiftR <= BLOCKSIZE - upper_right);

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
		} else if (num_right != 0) {
			shiftL = (int)(last - begin) - BLOCKSIZE + 1;
			shiftR = BLOCKSIZE;
			D_ASSERT(shiftL >= 0);
			D_ASSERT(shiftL <= BLOCKSIZE);
			D_ASSERT(num_left == 0);
			start_left = 0;
			for (j = 0; j < shiftL; j++) {
				indexL[num_left] = j;
				num_left += (!less(begin[j], pivot));
			}
		} else {
			shiftL = BLOCKSIZE;
			shiftR = (int)(last - begin) - BLOCKSIZE + 1;
			D_ASSERT(shiftR >= 0);
			D_ASSERT(shiftR <= BLOCKSIZE);
			D_ASSERT(num_right == 0);
			start_right = 0;
			for (j = 0; j < shiftR; j++) {
				indexR[num_right] = j;
				num_right += !(less(pivot, *(last - j)));
			}
		}

		// rearrange final iteration
		num = std::min(num_left, num_right);
		if (num != 0) {
			*hole = std::move(*(begin + indexL[start_left]));
			*(begin + indexL[start_left]) = std::move(*(last - indexR[start_right]));
			for (j = 1; j < num; j++) {
				*(last - indexR[start_right + j - 1]) = std::move(*(begin + indexL[start_left + j]));
				*(begin + indexL[start_left + j]) = std::move(*(last - indexR[start_right + j]));
			}
			hole = (last - indexR[start_right + num - 1]);
		}
		num_left -= num;
		num_right -= num;
		start_left += num;
		start_right += num;

		if (num_left == 0) {
			begin += shiftL;
		}
		if (num_right == 0) {
			last -= shiftR;
		}

		// rearrange remaining elements
		if (num_left != 0) {
			D_ASSERT(num_right == 0);
			int lowerI = start_left + num_left - 1;
			int upper = (int)(last - begin);
			while (lowerI >= start_left && indexL[lowerI] == upper) {
				upper--;
				lowerI--;
			}
			temp = std::move(*(begin + upper));
			while (lowerI >= start_left) {
				*(begin + upper) = std::move(*(begin + indexL[lowerI]));
				*(begin + indexL[lowerI]) = std::move(*(begin + (--upper)));
				lowerI--;
			}
			*(begin + upper) = std::move(temp);
			*hole = std::move(*(begin + upper + 1));

			*(begin + upper + 1) = std::move(pivot); // fetch the pivot
			return begin + upper + 1;

		} else if (num_right != 0) {
			D_ASSERT(num_left == 0);
			int lowerI = start_right + num_right - 1;
			int upper = (int)(last - begin);
			while (lowerI >= start_right && indexR[lowerI] == upper) {
				upper--;
				lowerI--;
			}
			*hole = std::move(*(last - upper));
			while (lowerI >= start_right) {
				*(last - upper) = std::move(*(last - indexR[lowerI]));
				*(last - indexR[lowerI--]) = std::move(*(last - (--upper)));
			}

			*(last - upper) = std::move(pivot); // fetch the pivot
			return last - upper;

		} else { // no remaining elements
			D_ASSERT(last + 1 == begin);
			*hole = std::move(*begin);
			*begin = std::move(pivot); // fetch the pivot
			return begin;
		}
	}

	// with check for elements equal to pivot -- requires that *(begin - 1) <= *pivot_pos <= *end   (in particular these
	// positions must exist)
	template <typename iter, typename Compare>
	inline iter hoare_block_partition_unroll_loop(iter begin, iter end, iter pivot_pos, Compare less,
	                                              int &pivot_length) {
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
		// main loop
		while (last - begin + 1 > 2 * BLOCKSIZE) {
			// Compare and store in buffers
			if (num_left == 0) {
				start_left = 0;
				for (j = 0; j < BLOCKSIZE;) {
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
				for (j = 0; j < BLOCKSIZE;) {
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
			// rearrange elements
			num = std::min(num_left, num_right);
			if (num != 0) {
				*hole = std::move(*(begin + indexL[start_left]));
				*(begin + indexL[start_left]) = std::move(*(last - indexR[start_right]));
				for (j = 1; j < num; j++) {
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
		} // end main loop

		if (num_left == 0) {
			start_left = 0;
		}
		if (num_right == 0) {
			start_right = 0;
		}

		// Compare and store in buffers final iteration
		int shiftR = 0, shiftL = 0;
		if (num_right == 0 && num_left == 0) { // for small arrays or in the unlikely case that both buffers are empty
			shiftL = (int)((last - begin) + 1) / 2;
			shiftR = (int)(last - begin) + 1 - shiftL;
			D_ASSERT(shiftL >= 0);
			D_ASSERT(shiftL <= BLOCKSIZE);
			D_ASSERT(shiftR >= 0);
			D_ASSERT(shiftR <= BLOCKSIZE);
			start_left = 0;
			start_right = 0;
			for (j = 0; j < shiftL; j++) {
				indexL[num_left] = j;
				num_left += (!less(begin[j], pivot));
				indexR[num_right] = j;
				num_right += !less(pivot, *(last - j));
			}
			if (shiftL < shiftR) {
				D_ASSERT(shiftL + 1 == shiftR);
				indexR[num_right] = j;
				num_right += !less(pivot, *(last - j));
			}
		} else if (num_right != 0) {
			shiftL = (int)(last - begin) - BLOCKSIZE + 1;
			shiftR = BLOCKSIZE;
			D_ASSERT(shiftL >= 0);
			D_ASSERT(shiftL <= BLOCKSIZE);
			D_ASSERT(num_left == 0);
			start_left = 0;
			for (j = 0; j < shiftL; j++) {
				indexL[num_left] = j;
				num_left += (!less(begin[j], pivot));
			}
		} else {
			shiftL = BLOCKSIZE;
			shiftR = (int)(last - begin) - BLOCKSIZE + 1;
			D_ASSERT(shiftR >= 0);
			D_ASSERT(shiftR <= BLOCKSIZE);
			D_ASSERT(num_right == 0);
			start_right = 0;
			for (j = 0; j < shiftR; j++) {
				indexR[num_right] = j;
				num_right += !(less(pivot, *(last - j)));
			}
		}

		// rearrange final iteration
		num = std::min(num_left, num_right);
		if (num != 0) {
			*hole = std::move(*(begin + indexL[start_left]));
			*(begin + indexL[start_left]) = std::move(*(last - indexR[start_right]));
			for (j = 1; j < num; j++) {
				*(last - indexR[start_right + j - 1]) = std::move(*(begin + indexL[start_left + j]));
				*(begin + indexL[start_left + j]) = std::move(*(last - indexR[start_right + j]));
			}
			hole = (last - indexR[start_right + num - 1]);
		}
		num_left -= num;
		num_right -= num;
		start_left += num;
		start_right += num;

		if (num_left == 0) {
			begin += shiftL;
		}
		if (num_right == 0) {
			last -= shiftR;
		}

		// rearrange remaining elements
		if (num_left != 0) {
			D_ASSERT(num_right == 0);
			int lowerI = start_left + num_left - 1;
			int upper = (int)(last - begin);
			while (lowerI >= start_left && indexL[lowerI] == upper) {
				upper--;
				lowerI--;
			}
			temp = std::move(*(begin + upper));
			while (lowerI >= start_left) {
				*(begin + upper) = std::move(*(begin + indexL[lowerI]));
				*(begin + indexL[lowerI]) = std::move(*(begin + (--upper)));
				lowerI--;
			}
			*(begin + upper) = std::move(temp);
			*hole = std::move(*(begin + upper + 1));

			// check for double elements if the pivot sample has repetitions or a small array is partitioned very
			// unequal
			if (double_pivot_check || (small_array && num_left >= (15 * shiftL) / 16)) {
				iter begin_lomuto = begin + upper + 1;
				iter q = begin_lomuto + 1;

				// check at least 4 elements whether they are equal to the pivot using Elmasry, Katajainen and
				// Stenmark's Lomuto partitioner
				unsigned int count_swaps = 1;
				unsigned int count_steps = 0;
				while (q < end &&
				       (count_swaps << 2) > count_steps) { // continue as long as there are many elements equal to pivot
					typename std::iterator_traits<iter>::value_type x = std::move(*q);
					bool smaller = !less(pivot, x);
					begin_lomuto += smaller; // smaller = 1 ? begin++ : begin
					count_swaps += smaller;
					index delta = smaller * (q - begin_lomuto);
					iter s = begin_lomuto + delta; // smaller = 1 => s = q : s = begin
					iter y = q - delta;            // smaller = 1 => y = begin : y = q
					*s = std::move(*begin_lomuto);
					*y = std::move(x);
					++q;
					count_steps++;
				}

				pivot_length = begin_lomuto + 1 - (begin + upper + 1);
			}
			*(begin + upper + 1) = std::move(pivot); // fetch the pivot
			return begin + upper + 1;

		} else if (num_right != 0) {
			D_ASSERT(num_left == 0);
			int lowerI = start_right + num_right - 1;
			int upper = (int)(last - begin);
			while (lowerI >= start_right && indexR[lowerI] == upper) {
				upper--;
				lowerI--;
			}
			*hole = std::move(*(last - upper));
			while (lowerI >= start_right) {
				*(last - upper) = std::move(*(last - indexR[lowerI]));
				*(last - indexR[lowerI--]) = std::move(*(last - (--upper)));
			}

			// check for double elements if the pivot sample has repetitions or a small array is partitioned very
			// unequal
			if (double_pivot_check || (small_array && num_right >= (15 * shiftR) / 16)) {
				iter begin_lomuto = last - upper;
				iter q = begin_lomuto - 1;

				// check at least 4 elements whether they are equal to the pivot using Elmasry, Katajainen and
				// Stenmark's Lomuto partitioner
				unsigned int count_swaps = 1;
				unsigned int count_steps = 0;
				while (q > temp_begin &&
				       (count_swaps << 2) > count_steps) { // continue as long as there are many elements equal to pivot
					typename std::iterator_traits<iter>::value_type x = std::move(*q);
					bool smaller = !less(x, pivot);
					begin_lomuto -= smaller; // smaller = 1 ? begin++ : begin
					count_swaps += smaller;
					index delta = smaller * (q - begin_lomuto);
					iter s = begin_lomuto + delta; // smaller = 1 => s = q : s = begin
					iter y = q - delta;            // smaller = 1 => y = begin : y = q
					*s = std::move(*begin_lomuto);
					*y = std::move(x);
					--q;
					count_steps++;
				}

				pivot_length = (last - upper) + 1 - begin_lomuto;
				*(last - upper) = std::move(pivot); // fetch the pivot
				return begin_lomuto;
			} else {
				*(last - upper) = std::move(pivot); // fetch the pivot
				return last - upper;
			}

		} else { // no remaining elements
			D_ASSERT(last + 1 == begin);
			*hole = std::move(*begin);
			*begin = std::move(pivot); // fetch the pivot
			return begin;
		}
	}

	template <typename iter, typename Compare>
	struct Hoare_block_partition_mosqrt {
		static inline iter Partition(iter begin, iter end, Compare less) {
			iter mid;
			if (end - begin > 20000) {
				unsigned int pivot_sample_size = sqrt(end - begin);
				pivot_sample_size += (1 - (pivot_sample_size % 2));     // make it an odd number
				mid = median_of_k(begin, end, less, pivot_sample_size); // choose pivot as median of sqrt(n)
				// partition
				return hoare_block_partition_unroll_loop(begin + pivot_sample_size / 2, end - pivot_sample_size / 2,
				                                         mid, less);
			} else {
				if (end - begin > 800) {
					mid = median_of_5_medians_of_5(begin, end, less);
				} else if (end - begin > 100) {
					mid = median_of_3_medians_of_3(begin, end, less);
				} else {
					mid = median_of_3(begin, end, less);
				}
				// partition
				return hoare_block_partition_unroll_loop(begin + 1, end - 1, mid, less);
			}
		}

		// with duplicate check
		static inline iter Partition(iter begin, iter end, Compare less, int &pivot_length) {
			iter mid;
			if (end - begin > 20000) {
				unsigned int pivot_sample_size = sqrt(end - begin);
				pivot_sample_size += (1 - (pivot_sample_size % 2));     // make it an odd number
				mid = median_of_k(begin, end, less, pivot_sample_size); // choose pivot as median of sqrt(n)
				// partition
				return hoare_block_partition_unroll_loop(begin + pivot_sample_size / 2, end - pivot_sample_size / 2,
				                                         mid, less, pivot_length);
			} else {
				if (end - begin > 800) {
					mid = median_of_5_medians_of_5(begin, end, less);
				} else if (end - begin > 100) {
					mid = median_of_3_medians_of_3(begin, end, less);
				} else {
					mid = median_of_3(begin, end, less);
				}
				// mid = median_of_3(begin , end , less);
				return hoare_block_partition_unroll_loop(begin + 1, end - 1, mid, less, pivot_length);
			}
		}
	};

	template <template <class, class> class Partitioner, typename iter, typename Compare>
	static inline void Qsort(iter begin, iter end, Compare less) {
		const int depth_limit = 2 * std::ilogb((double)(end - begin)) + 3;
		iter stack[80];
		iter *s = stack;
		int depth_stack[40];
		int depth = 0;
		int *d_s_top = depth_stack;
		*s = begin;
		*(s + 1) = end;
		s += 2;
		*d_s_top = 0;
		++d_s_top;
		do {
			if (depth < depth_limit && (end - begin > IS_THRESH)) {
				iter pivot = Partitioner<iter, Compare>::Partition(begin, end, less);
				// push large side to stack and continue on small side
				if (pivot - begin > end - pivot) {
					*s = begin;
					*(s + 1) = pivot;
					begin = pivot + 1;
				} else {
					*s = pivot + 1;
					*(s + 1) = end;
					end = pivot;
				}
				s += 2;
				depth++;
				*d_s_top = depth;
				++d_s_top;
			} else {
				if (end - begin > IS_THRESH) { // if recursion depth limit exceeded
					std::partial_sort(begin, end, end);
				} else {
					insertion_sort(begin, end, less); // copy of std::__insertion_sort (GCC 4.7.2)
				}
				// pop new subarray from stack
				s -= 2;
				begin = *s;
				end = *(s + 1);
				--d_s_top;
				depth = *d_s_top;
			}
		} while (s != stack);
	}

public:
	template <class RandomAccessIterator, class Compare>
	static void Sort(RandomAccessIterator first, RandomAccessIterator last, Compare comp) {
		Qsort<Hoare_block_partition_mosqrt>(first, last, comp);
	}

	template <class RANDOM_IT>
	static void Sort(RANDOM_IT first, RANDOM_IT last) {
		Sort(first, last, std::less<RANDOM_IT>());
	}
};

} // namespace duckdb
