//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// common/limits.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types.hpp"

namespace duckdb {

//! Returns the minimum value that can be stored in a given type
template <class T> int64_t MinimumValue();
//! Returns the maximum value that can be stored in a given type
template <class T> int64_t MaximumValue();

//! Returns the minimum value that can be stored in a given type
int64_t MinimumValue(TypeId type);
//! Returns the maximum value that can be stored in a given type
int64_t MaximumValue(TypeId type);
//! Returns the minimal type that guarantees an integer value from not
//! overflowing
TypeId MinimalType(int64_t value);

} // namespace duckdb
