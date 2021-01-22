#include "duckdb/function/scalar/generic_functions.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"

namespace duckdb {

static void null_if_impl(DataChunk &args, ExpressionState &state, Vector &result) {
	if (args.data[0].vector_type == VectorType::CONSTANT_VECTOR &&
	    args.data[1].vector_type == VectorType::CONSTANT_VECTOR) {
		result.vector_type = VectorType::CONSTANT_VECTOR;
		bool comparison_result[STANDARD_VECTOR_SIZE];
		Vector bool_result(LogicalType::BOOLEAN, (data_ptr_t) comparison_result);

		VectorOperations::Equals(args.data[0], args.data[1], bool_result, args.size());

		result.Reference(args.data[0]);
		if (comparison_result[0]) {
			ConstantVector::SetNull(result, true);
		}
	} else {
		args.Normalify();

		bool comparison_result[STANDARD_VECTOR_SIZE];
		Vector bool_result(LogicalType::BOOLEAN, (data_ptr_t) comparison_result);

		VectorOperations::Equals(args.data[0], args.data[1], bool_result, args.size());

		result.Reference(args.data[0]);
		auto &nullmask = FlatVector::Nullmask(result);
		for(idx_t i = 0; i < args.size(); i++) {
			if (comparison_result[i]) {
				nullmask[i] = true;
			}
		}
	}
}

void NullIfFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet null_if("nullif");
	for (auto type : LogicalType::ALL_TYPES) {
		null_if.AddFunction(ScalarFunction({ type, type }, type, null_if_impl));
	}
	set.AddFunction(null_if);
}

} // namespace duckdb
