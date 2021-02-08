#include "duckdb/function/scalar/generic_functions.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

struct ConstantOrNullBindData : public FunctionData {
	explicit ConstantOrNullBindData(Value val) : value(val) {
	}

	Value value;

public:
	unique_ptr<FunctionData> Copy() override {
		return make_unique<ConstantOrNullBindData>(value);
	}
};

static void ConstantOrNullFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (ConstantOrNullBindData &)*func_expr.bind_info;
	result.Reference(info.value);
	for (idx_t idx = 0; idx < args.ColumnCount(); idx++) {
		switch (args.data[idx].vector_type) {
		case VectorType::FLAT_VECTOR: {
			auto &input_mask = FlatVector::Nullmask(args.data[idx]);
			if (input_mask.any()) {
				// there are null values: need to merge them into the result
				result.Normalify(args.size());
				auto &result_mask = FlatVector::Nullmask(result);
				result_mask |= input_mask;
			}
			break;
		}
		case VectorType::CONSTANT_VECTOR: {
			if (ConstantVector::IsNull(args.data[idx])) {
				// input is constant null, return constant null
				result.Reference(info.value);
				ConstantVector::SetNull(result, true);
				return;
			}
			break;
		}
		default: {
			VectorData vdata;
			args.data[idx].Orrify(args.size(), vdata);
			if (vdata.nullmask->any()) {
				result.Normalify(args.size());
				auto &result_mask = FlatVector::Nullmask(result);
				for (idx_t i = 0; i < args.size(); i++) {
					if ((*vdata.nullmask)[vdata.sel->get_index(i)]) {
						result_mask[i] = true;
					}
				}
			}
			break;
		}
		}
	}
}

ScalarFunction ConstantOrNull::GetFunction(LogicalType return_type) {
	return ScalarFunction("constant_or_null", {}, return_type, ConstantOrNullFunction);
}

unique_ptr<FunctionData> ConstantOrNull::Bind(Value value) {
	return make_unique<ConstantOrNullBindData>(move(value));
}

bool ConstantOrNull::IsConstantOrNull(BoundFunctionExpression &expr, const Value &val) {
	if (expr.function.name != "constant_or_null") {
		return false;
	}
	D_ASSERT(expr.bind_info);
	auto &bind_data = (ConstantOrNullBindData &)*expr.bind_info;
	D_ASSERT(bind_data.value.type() == val.type());
	return bind_data.value == val;
}

} // namespace duckdb
