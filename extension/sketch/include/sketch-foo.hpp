//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sketch-sum.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb {

  class SketchFoo {
    public:
      static void RegisterFunction(ClientContext &context);
  };
} // namespace duckdb
