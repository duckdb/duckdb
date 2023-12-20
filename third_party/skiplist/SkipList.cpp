//
//  SkipList.cpp
//  SkipList
//
//  Created by Paul Ross on 19/12/2015.
//  Copyright (c) 2017 Paul Ross. All rights reserved.
//

#include <cstdlib>
#ifdef SKIPLIST_THREAD_SUPPORT
#include <mutex>
#endif
#include <string>

#include "SkipList.h"

namespace duckdb_skiplistlib {
namespace skip_list {

/** Tosses a virtual coin, returns true if 'heads'.
 *
 * No heads, ever:
 * @code
 * return false;
 * @endcode
 *
 * 6.25% heads:
 * @code
 * return rand() < RAND_MAX / 16;
 * @endcode
 *
 * 12.5% heads:
 * @code
 * return rand() < RAND_MAX / 8;
 * @endcode
 *
 * 25% heads:
 * @code
 * return rand() < RAND_MAX / 4;
 * @endcode
 *
 * Fair coin:
 * @code
 * return rand() < RAND_MAX / 2;
 * @endcode
 *
 * 75% heads:
 * @code
 * return rand() < RAND_MAX - RAND_MAX / 4;
 * @endcode
 *
 * 87.5% heads:
 * @code
 * return rand() < RAND_MAX - RAND_MAX / 8;
 * @endcode
 *
 * 93.75% heads:
 * @code
 * @return rand() < RAND_MAX - RAND_MAX / 16;
 * @endcode
 */
bool tossCoin() {
    return rand() < RAND_MAX / 2;
}

void seedRand(unsigned seed) {
    srand(seed);
}

// This throws an IndexError when the index value >= size.
// If possible the error will have an informative message.
#ifdef INCLUDE_METHODS_THAT_USE_STREAMS
void _throw_exceeds_size(size_t index) {
    std::ostringstream oss;
    oss << "Index out of range 0 <= index < " << index;
    std::string err_msg = oss.str();
#else
void _throw_exceeds_size(size_t /* index */) {
    std::string err_msg = "Index out of range.";
#endif
    throw IndexError(err_msg);
}

#ifdef SKIPLIST_THREAD_SUPPORT
    std::mutex gSkipListMutex;
#endif


} // namespace SkipList
} // namespace OrderedStructs
