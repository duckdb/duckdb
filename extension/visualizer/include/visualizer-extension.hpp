//===----------------------------------------------------------------------===//
//                         DuckDB
//
// visualizer-extension.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb {

class VisualizerExtension : public Extension {
public:
    void Load(DuckDB &db) override;
};

} // namespace duckdb
