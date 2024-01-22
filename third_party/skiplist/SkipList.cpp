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
