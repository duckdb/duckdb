/******************************************************************************
* blocked_simple.h++
*
* interface for BlockQuicksort simple with median-of-three
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
#include <random>
#include <ctime>
#include <cmath>
#include <assert.h>


#include "insertionsort.h"
#include "median.h"
#include "partition.h"
#include "quicksort.h"


namespace lomuto_blocked_simple {
	template<typename iter, typename Compare>
	void sort(iter begin, iter end, Compare less) {
		quicksort::qsort<partition::Lomuto_block_partition_simple>(begin, end, less);
	}
	template<typename T>
	void sort(std::vector<T> &v) {
		typename std::vector<T>::iterator begin = v.begin();
		typename std::vector<T>::iterator end = v.end();
		quicksort::qsort<partition::Lomuto_block_partition_simple>(begin, end, std::less<T>());
	}
}