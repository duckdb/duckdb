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
#include <fstream>

namespace duckdb {

//! Physically copy file into a table
    class PhysicalCopy : public PhysicalOperator {
    public:
        PhysicalCopy(std::shared_ptr<TableCatalogEntry> table,std::string file_path, bool is_from, char delimiter,char quote,char escape)
                : PhysicalOperator(PhysicalOperatorType::COPY), table(table), file_path(file_path),is_from(is_from), delimiter(delimiter), quote(quote), escape(escape)  {}

        std::vector<TypeId> GetTypes() override;
        virtual void GetChunk(DataChunk &chunk,
                              PhysicalOperatorState *state) override;

        virtual std::unique_ptr<PhysicalOperatorState> GetOperatorState(ExpressionExecutor *executor) override;

        std::shared_ptr<TableCatalogEntry> table;
        std::string file_path;
        bool is_from;

        char delimiter = ',';
        char quote = '"';
        char escape = '"';

    };
} // namespace duckdb
