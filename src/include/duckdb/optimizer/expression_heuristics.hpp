//===----------------------------------------------------------------------===//
//                         DuckDB
//
// optimizer/expression_heuristics.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/optimizer.hpp"

namespace duckdb {

    class ExpressionHeuristics : public LogicalOperatorVisitor {
        public:
            ExpressionHeuristics(Optimizer &optimizer) : optimizer(optimizer) {
            }

            Optimizer &optimizer;
            unique_ptr<LogicalOperator> root;

            //! Search for filters to be reordered
            unique_ptr<LogicalOperator> Rewrite(unique_ptr<LogicalOperator> op);
            //! Reorder the expressions of a filter
            void ReorderExpressions(vector<unique_ptr<Expression>> &expressions);
            //! Return the cost of an expression
            index_t Cost(Expression &expr);

            //! Override this function to search for filter operators
            void VisitOperator(LogicalOperator &op) override;
    };
} // namespace duckdb