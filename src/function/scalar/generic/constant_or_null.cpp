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
	result.Reference(info.value);
	for (idx_t idx = 1; idx < args.ColumnCount(); idx++) {
		switch (args.data[idx].GetVectorType()) {
		case VectorType::FLAT_VECTOR: {
			auto &input_mask = FlatVector::Validity(args.data[idx]);
			if (!input_mask.AllValid()) {
				// there are null values: need to merge them into the result
				result.Flatten(args.size());
				auto &result_mask = FlatVector::Validity(result);
				result_mask.Combine(input_mask, args.size());
			}
			break;
		}
		case VectorType::CONSTANT_VECTOR: {
			if (ConstantVector::IsNull(args.data[idx])) {
				// input is constant null, return constant null
				result.SetVectorType(VectorType::CONSTANT_VECTOR);
				auto &result_mask = ConstantVector::Validity(result);
				auto &input_mask = ConstantVector::Validity(args.data[idx]);
				result_mask.Initialize(input_mask);
				ConstantVector::SetNull(result, true);
				return;
			}
			break;
		}
		default: {
			UnifiedVectorFormat vdata;
			args.data[idx].ToUnifiedFormat(args.size(), vdata);
			if (!vdata.validity.AllValid()) {
				result.Flatten(args.size());
				auto &result_mask = FlatVector::Validity(result);
				for (idx_t i = 0; i < args.size(); i++) {
					if (!vdata.validity.RowIsValid(vdata.sel->get_index(i))) {
						result_mask.SetInvalid(i);
					}
				}
			}
			break;
		}
		}
	}
}

unique_ptr<FunctionData> ConstantOrNullBind(ClientContext &context, ScalarFunction &bound_function,
                                            vector<unique_ptr<Expression>> &arguments) {
	if (arguments[0]->HasParameter()) {
		throw ParameterNotResolvedException();
	}
	if (!arguments[0]->IsFoldable()) {
		throw BinderException("ConstantOrNull requires a constant input");
	}
	D_ASSERT(arguments.size() >= 2);
	auto value = ExpressionExecutor::EvaluateScalar(context, *arguments[0]);
	bound_function.SetReturnType(arguments[0]->return_type);
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
	fun.bind = ConstantOrNullBind;
	fun.varargs = LogicalType::ANY;
	return fun;
}

} // namespace duckdb
