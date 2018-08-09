//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// planner/operator/logical_insert.hpp
//
// Author: Pedro Holanda
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

    class LogicalCopy : public LogicalOperator {
    public:
        LogicalCopy(std::shared_ptr<TableCatalogEntry> table,
                    std::string file_path)
                : LogicalOperator(LogicalOperatorType::COPY), table(table), file_path(file_path) {}

        virtual void Accept(LogicalOperatorVisitor *v) override { v->Visit(*this); }

        std::shared_ptr<TableCatalogEntry> table;

        std::string file_path;
    };
} // namespace duckdb
