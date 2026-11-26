//===----------------------------------------------------------------------===//
//                         DuckDB
//
// tpcds_errors.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

namespace tpcds {

//! Errors raised by the generator, reported to DuckDB with their error type
[[noreturn]] void ThrowInternalError(const std::string &message);
[[noreturn]] void ThrowOutOfMemoryError(const std::string &message);
[[noreturn]] void ThrowInvalidInputError(const std::string &message);

} // namespace tpcds
