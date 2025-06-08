#pragma once

#include <memory>

#define DUCKDB_WRAP_STD

namespace duckdb_wrapped {
namespace std {
using ::std::make_shared;
using ::std::shared_ptr;
using ::std::unique_ptr;
// using ::std::make_unique;
} // namespace std
} // namespace duckdb_wrapped
