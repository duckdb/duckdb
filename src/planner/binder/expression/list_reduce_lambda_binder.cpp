#include "list_reduce_lambda_binder.hpp"

#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/lambda_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/planner/expression/bound_lambda_expression.hpp"

namespace duckdb {

class ListReduceBindLambdaContext final : public BindLambdaContext {
public:
	explicit ListReduceBindLambdaContext(LogicalType accumulator_type) : accumulator_type(std::move(accumulator_type)) {
	}

	const LogicalType accumulator_type;
};

LogicalType ListReduceBindLambdaOverride(ClientContext &context, const vector<LogicalType> &function_child_types,
                                         const idx_t parameter_idx,
                                         optional_ptr<BindLambdaContext> bind_lambda_context) {
	(void)context;
	if (!bind_lambda_context) {
		throw InternalException("list_reduce bind lambda context is missing");
	}
	auto *context_ptr = dynamic_cast<ListReduceBindLambdaContext *>(bind_lambda_context.get());
	if (!context_ptr) {
		throw InternalException("list_reduce bind lambda context has unexpected type");
	}
	auto list_child_type = function_child_types[0];
	if (list_child_type.id() != LogicalTypeId::SQLNULL && list_child_type.id() != LogicalTypeId::UNKNOWN) {
		if (list_child_type.id() == LogicalTypeId::ARRAY) {
			list_child_type = ArrayType::GetChildType(list_child_type);
		} else if (list_child_type.id() == LogicalTypeId::LIST) {
			list_child_type = ListType::GetChildType(list_child_type);
		} else {
			throw InternalException("The first argument must be a list or array type");
		}
	}
	switch (parameter_idx) {
	case 0:
		return context_ptr->accumulator_type;
	case 1:
		return list_child_type;
	case 2:
		return LogicalType::BIGINT;
	default:
		throw BinderException("This lambda function only supports up to three lambda parameters!");
	}
}

void MaybeRebindListReduceLambda(ClientContext &context, FunctionExpression &function, ScalarFunctionCatalogEntry &func,
                                 idx_t depth, const vector<LogicalType> &function_child_types,
                                 bind_lambda_function_t bind_lambda_function, LambdaExpression &lambda_expr_copy,
                                 BindResult &bind_lambda_result,
                                 optional_ptr<bind_lambda_function_t> &capture_bind_lambda,
                                 vector<LogicalType> &capture_child_types, bind_lambda_function_t &override_bind_lambda,
                                 LogicalType &override_accumulator_type_storage, bool &override_has_accumulator_type,
                                 const bind_lambda_expression_t &bind_lambda_expression,
                                 optional_ptr<BindLambdaContext> bind_lambda_context,
                                 unique_ptr<BindLambdaContext> &override_bind_lambda_context) {
	const bool is_list_reduce = func.name == "list_reduce";
	if (!is_list_reduce) {
		return;
	}

	const bool has_initial = function_child_types.size() == 3;
	auto &bound_lambda_expr = bind_lambda_result.expression->Cast<BoundLambdaExpression>();
	const auto &lambda_return_type = bound_lambda_expr.lambda_expr->return_type;

	auto list_child_type = function_child_types[0];
	if (list_child_type.id() != LogicalTypeId::SQLNULL && list_child_type.id() != LogicalTypeId::UNKNOWN) {
		if (list_child_type.id() == LogicalTypeId::ARRAY) {
			list_child_type = ArrayType::GetChildType(list_child_type);
		} else if (list_child_type.id() == LogicalTypeId::LIST) {
			list_child_type = ListType::GetChildType(list_child_type);
		} else {
			throw BinderException("The first argument must be a list or array type");
		}
	}

	LogicalType accumulator_type;
	if (has_initial) {
		const auto &initial_type = function_child_types[2];
		if (!LogicalType::TryGetMaxLogicalType(context, initial_type, lambda_return_type, accumulator_type)) {
			throw BinderException("No common super type between initial value and lambda return type");
		}
	} else {
		if (!LogicalType::TryGetMaxLogicalType(context, list_child_type, lambda_return_type, accumulator_type)) {
			throw BinderException("No common super type between list element and lambda return type");
		}
	}

	auto &lambda_expr_rebind = lambda_expr_copy.Cast<LambdaExpression>();
	if (has_initial) {
		auto rebind_child_types = function_child_types;
		rebind_child_types[2] = accumulator_type;
		bind_lambda_result = bind_lambda_expression(lambda_expr_rebind, depth, rebind_child_types,
		                                            &bind_lambda_function, bind_lambda_context);
		if (bind_lambda_result.HasError()) {
			bind_lambda_result.error.Throw();
		}
		capture_child_types = std::move(rebind_child_types);
	} else if (accumulator_type != list_child_type) {
		override_bind_lambda = &ListReduceBindLambdaOverride;
		override_bind_lambda_context = make_uniq<ListReduceBindLambdaContext>(accumulator_type);
		bind_lambda_result = bind_lambda_expression(lambda_expr_rebind, depth, function_child_types,
		                                            &override_bind_lambda, *override_bind_lambda_context);
		if (bind_lambda_result.HasError()) {
			bind_lambda_result.error.Throw();
		}
		capture_bind_lambda = &override_bind_lambda;
		override_accumulator_type_storage = accumulator_type;
		override_has_accumulator_type = true;
	}
}

} // namespace duckdb
