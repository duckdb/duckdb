#include "duckdb/function/scalar/variant_utils.hpp"
#include "duckdb/execution/expression_executor.hpp"

namespace duckdb {

bool VariantBindUtils::GetConstantArgument(ClientContext &context, const Expression &expr, Value &constant_arg) {
	if (!expr.IsFoldable()) {
		return false;
	}
	constant_arg = ExpressionExecutor::EvaluateScalar(context, expr);
	if (!constant_arg.IsNull()) {
		return true;
	}
	return false;
}

} // namespace duckdb