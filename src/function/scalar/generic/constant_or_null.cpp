#include "duckdb/function/scalar/generic_common.hpp"
#include "duckdb/function/scalar/generic_functions.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

namespace {

struct ConstantOrNullBindData : public FunctionData {
	explicit ConstantOrNullBindData(Value val) : value(std::move(val)) {
	}

	Value value;

public:
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<ConstantOrNullBindData>(value);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<ConstantOrNullBindData>();
		return value == other.value;
	}
};

static void ConstantOrNullFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.bind_info->Cast<ConstantOrNullBindData>();
	result.Reference(info.value, count_t(args.size()));
	for (idx_t idx = 1; idx < args.ColumnCount(); idx++) {
		switch (args.data[idx].GetVectorType()) {
		case VectorType::FLAT_VECTOR: {
			auto &input_mask = FlatVector::ValidityMutable(args.data[idx]);
			if (input_mask.CanHaveNull()) {
				// there are null values: need to merge them into the result
				result.Flatten(args.size());
				auto &result_mask = FlatVector::ValidityMutable(result);
				result_mask.EnsureWritable();
				result_mask.Combine(input_mask, args.size());
			}
			break;
		}
		case VectorType::CONSTANT_VECTOR: {
			if (ConstantVector::IsNull(args.data[idx])) {
				// input is constant null, return constant null
				auto &result_mask = ConstantVector::Validity(result);
				auto &input_mask = ConstantVector::Validity(args.data[idx]);
				result_mask.Initialize(input_mask);
				ConstantVector::SetNull(result);
				return;
			}
			break;
		}
		default: {
			auto entries = args.data[idx].Validity(args.size());
			if (entries.CanHaveNull()) {
				result.Flatten(args.size());
				auto &result_mask = FlatVector::ValidityMutable(result);
				for (idx_t i = 0; i < args.size(); i++) {
					if (!entries.IsValid(i)) {
						result_mask.SetInvalid(i);
					}
				}
			}
			break;
		}
		}
	}
}

unique_ptr<FunctionData> ConstantOrNullBind(BindScalarFunctionInput &input) {
	auto &context = input.GetClientContext();
	auto &arguments = input.GetArguments();
	auto &function = input.GetBoundFunction();

	if (arguments[0]->HasParameter()) {
		throw ParameterNotResolvedException();
	}
	if (!arguments[0]->IsFoldable()) {
		throw BinderException("ConstantOrNull requires a constant input");
	}
	D_ASSERT(arguments.size() >= 2);
	auto value = ExpressionExecutor::EvaluateScalar(context, *arguments[0]);
	function.SetReturnType(arguments[0]->return_type);
	return make_uniq<ConstantOrNullBindData>(std::move(value));
}

} // namespace

unique_ptr<FunctionData> ConstantOrNull::Bind(Value value) {
	return make_uniq<ConstantOrNullBindData>(std::move(value));
}

bool ConstantOrNull::IsConstantOrNull(BoundFunctionExpression &expr, const Value &val) {
	if (expr.function.name != "constant_or_null") {
		return false;
	}
	D_ASSERT(expr.bind_info);
	auto &bind_data = expr.bind_info->Cast<ConstantOrNullBindData>();
	D_ASSERT(bind_data.value.type() == val.type());
	return bind_data.value == val;
}

ScalarFunction ConstantOrNullFun::GetFunction() {
	auto fun = ScalarFunction("constant_or_null", {LogicalType::ANY, LogicalType::ANY}, LogicalType::ANY,
	                          ConstantOrNullFunction);
	fun.SetBindCallback(ConstantOrNullBind);
	fun.varargs = LogicalType::ANY;
	return fun;
}

} // namespace duckdb
