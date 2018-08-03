//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// common/helper.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <sstream>

template <typename T, typename... Args>
std::unique_ptr<T> make_unique(Args &&... args) {
	return std::unique_ptr<T>(new T(std::forward<Args>(args)...));
}

template <class T> inline bool in_bounds(int64_t value) {
	return value >= std::numeric_limits<T>::min() &&
	       value <= std::numeric_limits<T>::max();
}
