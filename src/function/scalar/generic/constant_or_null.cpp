#include "duckdb/function/scalar/generic_common.hpp"
#include "duckdb/function/scalar/generic_functions.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

#include "duckdb/common/vector/struct_vector.hpp"

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
	auto &info = func_expr.BindInfo()->Cast<ConstantOrNullBindData>();
	result.Reference(info.value, count_t(args.size()));

	// the first argument is the constant, the rest are only inspected for NULLs
	vector<reference<Vector>> inputs;
	inputs.push_back(args.data[1]);
	for (auto &packed : StructVector::GetEntries(args.data[2])) {
		inputs.push_back(packed);
	}

	for (auto &input_ref : inputs) {
		auto &input = input_ref.get();
		switch (input.GetVectorType()) {
		case VectorType::FLAT_VECTOR: {
			auto &input_mask = FlatVector::ValidityMutable(input);
			if (input_mask.CanHaveNull()) {
				// there are null values: need to merge them into the result
				result.Flatten();
				auto &result_mask = FlatVector::ValidityMutable(result);
				result_mask.EnsureWritable();
				result_mask.Combine(input_mask, args.size());
			}
			break;
		}
		case VectorType::CONSTANT_VECTOR: {
			if (ConstantVector::IsNull(input)) {
				// input is constant null, return constant null
				auto &result_mask = ConstantVector::Validity(result);
				auto &input_mask = ConstantVector::Validity(input);
				result_mask.Initialize(input_mask);
				ConstantVector::SetNull(result, count_t(args.size()));
				return;
			}
			break;
		}
		default: {
			auto entries = input.Validity();
			if (entries.CanHaveNull()) {
				result.Flatten();
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
	auto &arguments = input.GetArguments();
	auto &function = input.GetBoundFunction();

	auto value = input.GetConstant(0);
	D_ASSERT(arguments.size() == 3);
	function.SetReturnType(arguments[0]->GetReturnType());
	return make_uniq<ConstantOrNullBindData>(std::move(value));
}

} // namespace

unique_ptr<FunctionData> ConstantOrNull::Bind(Value value) {
	return make_uniq<ConstantOrNullBindData>(std::move(value));
}

bool ConstantOrNull::IsConstantOrNull(BoundFunctionExpression &expr, const Value &val) {
	if (expr.Function().GetName() != "constant_or_null") {
		return false;
	}
	D_ASSERT(expr.BindInfo());
	auto &bind_data = expr.BindInfo()->Cast<ConstantOrNullBindData>();
	D_ASSERT(bind_data.value.type() == val.type());
	return bind_data.value == val;
}

ScalarFunction ConstantOrNullFun::GetFunction() {
	FunctionSignature signature;
	signature.AddParameter("arg1", LogicalType::ANY);
	signature.AddParameter("arg2", LogicalType::ANY);
	signature.AddVarPositionalParameter("args", LogicalType::ANY);
	signature.SetReturnType(LogicalType::ANY);
	ScalarFunction fun("constant_or_null", std::move(signature), ConstantOrNullFunction);
	fun.SetBindCallback(ConstantOrNullBind);
	return fun;
}

} // namespace duckdb
