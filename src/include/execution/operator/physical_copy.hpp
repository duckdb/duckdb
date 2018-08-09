//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// execution/operation/physical_copy.hpp
//
// Author: Pedro Holanda
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/physical_operator.hpp"

namespace duckdb {

//! Physically copy file into a table
    class PhysicalCopy : public PhysicalOperator {
    public:
        PhysicalCopy(std::shared_ptr<TableCatalogEntry> table,std::string file_path)
                : PhysicalOperator(PhysicalOperatorType::COPY), table(table), file_path(file_path) {}

        virtual void InitializeChunk(DataChunk &chunk) override;
        virtual void GetChunk(DataChunk &chunk,
                              PhysicalOperatorState *state) override;

        virtual std::unique_ptr<PhysicalOperatorState> GetOperatorState() override;

        std::shared_ptr<TableCatalogEntry> table;
        std::string file_path;
    };

} // namespace duckdb
