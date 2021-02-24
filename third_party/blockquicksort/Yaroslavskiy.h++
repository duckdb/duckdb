/*
* Code due to Vladimir Yaroslavskiy
* Retrieved from http://codeblab.com/wp-content/uploads/2009/09/DualPivotQuicksort.pdf
* Converted to C++ by Armin Weiß <armin.weiss@fmi.uni-stuttgart.de>
* Converted to sort inputs larger than 2^32 by Michael Axtmann <michael.axtmann@kit.edu>
*/

#include <iterator>

namespace Yaroslavskiy {
	/*
	private static void swap(long[] a, long i, long j) {
	 long temp = a[i];
	 a[i] = a[j];
	 a[j] = temp;
	}*/
  template<typename iter, typename comparator>
  void dualPivotQuicksort(iter left, iter right, long div, comparator comp) {
    using t = typename std::iterator_traits<iter>::value_type;
	 long len = right - left;
	 if (len < 27) { // insertion sort for tiny array
	 for (iter i = left + 1; i <= right; i++) {
		 for (iter j = i; j > left && comp(*j , *(j - 1)); j--) {
			std::iter_swap(j, j - 1);
		 }
	 }
	 return;
	 }
	 long third = len / div;
	 // "medians"
	 iter m1 = left + third;
	 iter m2 = right - third;
	 if (m1 <= left) {
		 m1 = left + 1;
	 }
	 if (m2 >= right) {
		m2 	= right - 1;
	 }
	 if (comp(*m1, *m2)) {
		 std::iter_swap(m1, left);
		 std::iter_swap(m2, right);
	 }
	 else {
		 std::iter_swap(m1, right);
		 std::iter_swap(m2, left);
	 }
	 // pivots
	 t pivot1 = *left;
	 t pivot2 = *right;
	 // pointers
	 iter less = left + 1;
	 iter great = right - 1;
	 // sorting
	 for (iter k = less; k <= great; k++) {
		 if (comp(*k, pivot1)) {
			std::iter_swap(k, less++);
		 }
		 else if (comp(pivot2 , *k)) {
			 while (k < great && comp(pivot2, *great)) {
				great--;
			 }
			 std::iter_swap(k, great--);
			 if (comp(*k , pivot1)) {
				std::iter_swap(k, less++);
			 }
		 }
	 }
	 // swaps
	 long dist = great - less;
	 if (dist < 13) {
		div++;
	 }
	 std::iter_swap(less - 1, left);
	 std::iter_swap(great + 1, right);
	 // subarrays
	 dualPivotQuicksort(left, less - 2, div, comp );
	 dualPivotQuicksort(great + 2, right, div, comp );
	 
	 // equal elements
	 if (dist > len - 13 && pivot1 != pivot2) {
		 for (iter k = less; k <= great; k++) {
			 if (!comp(pivot1, *k )) {
				std::iter_swap(k, less++);
			 }
			 else if (!comp(*k ,pivot2)) {
				 std::iter_swap(k, great--);
				 if (!comp(pivot1, *k )) {
					std::iter_swap(k, less++);
				 }
			 }
		 }
	 }
	 // subarray
	 if (pivot1 < pivot2) {
		dualPivotQuicksort(less, great, div,comp );
	 }
	}


	template<typename iter, typename comparator>
	void sort(iter begin, iter end, comparator less) {
	 dualPivotQuicksort(begin, end - 1, 3, less);
	}

}
