//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// planner/operator/logical_copy.hpp
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
                    std::string file_path,  bool is_from, char delimiter, char quote, char escape)
                : LogicalOperator(LogicalOperatorType::COPY), table(table), file_path(file_path),is_from(is_from), delimiter(delimiter), quote(quote), escape(escape) {}

        virtual void Accept(LogicalOperatorVisitor *v) override { v->Visit(*this); }

        std::shared_ptr<TableCatalogEntry> table;

        std::string file_path;

        bool is_from;

        char delimiter = ',';
        char quote = '"';
        char escape = '"';
    };
} // namespace duckdb
