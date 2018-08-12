//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/statement/copy_statement.hpp
//
// Author: Pedro Holanda
//
//===----------------------------------------------------------------------===//
#pragma once

#include <vector>

#include "catalog/catalog.hpp"
#include "parser/statement/sql_statement.hpp"

#include "parser/expression/abstract_expression.hpp"

namespace duckdb {

    class CopyStatement : public SQLStatement {
      public:
        CopyStatement() : SQLStatement(StatementType::COPY){};
        virtual ~CopyStatement() {}
        virtual std::string ToString() const;
        virtual void Accept(SQLNodeVisitor *v) { v->Visit(*this); }

        std::string table;
        std::string schema;

        std::string file_path;

        // File Format
        ExternalFileFormat format = ExternalFileFormat::CSV;

        // Copy: From CSV (True) To CSV (False)
        bool is_from;


        char delimiter = ',';
        char quote = '"';
        char escape = '"';
    };
}  // namespace duckdb
