//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_recursive_cte.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

    class LogicalRecursiveCTE : public LogicalOperator {
    public:
        LogicalRecursiveCTE(index_t table_index, index_t column_count, unique_ptr<LogicalOperator> top,
                            unique_ptr<LogicalOperator> bottom, LogicalOperatorType type)
                : LogicalOperator(type), table_index(table_index), column_count(column_count) {
            assert(type == LogicalOperatorType::RECURSIVE_CTE);
            children.push_back(move(top));
            children.push_back(move(bottom));
        }

        index_t table_index;
        index_t column_count;

    public:
        vector<ColumnBinding> GetColumnBindings() override {
            return GenerateColumnBindings(table_index, column_count);
        }

    protected:
        void ResolveTypes() override {
            types = children[0]->types;
        }
    };
} // namespace duckdb
