//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sketch-hll.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

class ClientContext;

class SketchHll {
public:
	static void RegisterFunction(ClientContext &context);
};

} // namespace duckdb
