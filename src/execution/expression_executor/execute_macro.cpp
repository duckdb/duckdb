#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_macro_expression.hpp"

namespace duckdb {
using namespace std;

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(BoundMacroExpression &expr,
                                                                ExpressionExecutorState &root) {
}

void ExpressionExecutor::Execute(BoundMacroExpression &expr, ExpressionState *state, const SelectionVector *sel,
                                 idx_t count, Vector &result) {

}

} // namespace duckdb
