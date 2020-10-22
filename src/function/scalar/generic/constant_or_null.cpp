#include "duckdb/function/scalar/generic_functions.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

using namespace std;

namespace duckdb {

struct ConstantOrNullBindData : public FunctionData {
	ConstantOrNullBindData(Value val) : value(val) {}

	Value value;

public:
	unique_ptr<FunctionData> Copy() override {
		return make_unique<ConstantOrNullBindData>(value);
	}
};

static void constant_or_null(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (ConstantOrNullBindData &)*func_expr.bind_info;
	result.Reference(info.value);
	switch(args.data[0].vector_type) {
	case VectorType::FLAT_VECTOR: {
		result.Normalify(args.size());
		FlatVector::SetNullmask(result, FlatVector::Nullmask(args.data[0]));
		break;
	}
	case VectorType::CONSTANT_VECTOR: {
		if (ConstantVector::IsNull(args.data[0])) {
			ConstantVector::SetNull(result, true);
		}
		break;
	}
	default: {
		result.Normalify(args.size());
		auto &result_mask = FlatVector::Nullmask(result);
		VectorData vdata;
		args.data[0].Orrify(args.size(), vdata);
		for(idx_t i = 0; i < args.size(); i++) {
			result_mask[i] = (*vdata.nullmask)[vdata.sel->get_index(i)];
		}
		break;
	}
	}
}

ScalarFunction ConstantOrNull::GetFunction(LogicalType return_type) {
	return ScalarFunction("constant_or_null", {LogicalType::ANY}, return_type, constant_or_null);
}

unique_ptr<FunctionData> ConstantOrNull::Bind(Value value) {
	return make_unique<ConstantOrNullBindData>(move(value));
}

}
