//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sketch-hll.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

// #include "duckdb.hpp"

namespace duckdb {

  class ClientContext; 

  class SketchHll {
      public:
          static void RegisterFunction(ClientContext &context);
  };

} // namespace duckdb
