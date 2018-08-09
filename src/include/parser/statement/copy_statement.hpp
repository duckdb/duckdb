//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/statement/copy_statement.hpp
//
// Author: Pedro Holanda
//
//===----------------------------------------------------------------------===//
//#pragma once
//
//#include "parser/sql_statement.h"
//#include "parser/table_ref.h"
//#include "expression/constant_value_expression.h"
//#include "common/sql_node_visitor.h"
#pragma once

#include <vector>

#include "catalog/catalog.hpp"
#include "parser/statement/sql_statement.hpp"

#include "parser/expression/abstract_expression.hpp"
#include "parser/expression/basetableref_expression.hpp"

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

        char delimiter;
    };
}  // namespace duckdb
