//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/operator/logical_index.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <storage/index.hpp>
#include "planner/logical_operator.hpp"

namespace duckdb {

//! LogicalIndex represents an Index Scan operation
    class LogicalIndexScan : public LogicalOperator {
    public:
        LogicalIndexScan(TableCatalogEntry &tableref, DataTable &table, Index &index, vector<column_t> column_ids)
        : LogicalOperator(LogicalOperatorType::INDEX_SCAN), tableref(tableref), table(table), index(index),
        column_ids(column_ids) {
        }

        //! The table to scan
        TableCatalogEntry &tableref;
        //! The physical data table to scan
        DataTable &table;
        //! The index to use for the scan
        Index &index;
        //! The column ids to project
        vector<column_t> column_ids;

        //! The value for the query predicate
        Value low_value;
        Value high_value;
        Value equal_value;

        //! If the predicate is low, high or equal
        bool low_index = false;
        bool high_index = false;
        bool equal_index = false;

        //! The expression type (e.g., >, <, >=, <=)
        ExpressionType low_expression_type;
        ExpressionType high_expression_type;


    protected:
        void ResolveTypes() override;
    };

} // namespace duckdb
