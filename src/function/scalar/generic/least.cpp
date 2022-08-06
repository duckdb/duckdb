#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/function/scalar/generic_functions.hpp"

namespace duckdb {

template <class OP>
struct LeastOperator {
	template <class T>
	static T Operation(T left, T right) {
		return OP::Operation(left, right) ? left : right;
	}
};

template <class T, class OP, bool IS_STRING = false>
static void LeastGreatestFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	if (args.ColumnCount() == 1) {
		// single input: nop
		result.Reference(args.data[0]);
		return;
	}
	auto result_type = VectorType::CONSTANT_VECTOR;
	for (idx_t col_idx = 0; col_idx < args.ColumnCount(); col_idx++) {
		if (args.data[col_idx].GetVectorType() != VectorType::CONSTANT_VECTOR) {
			// non-constant input: result is not a constant vector
			result_type = VectorType::FLAT_VECTOR;
		}
		if (IS_STRING) {
			// for string vectors we add a reference to the heap of the children
			StringVector::AddHeapReference(result, args.data[col_idx]);
		}
	}

	auto result_data = FlatVector::GetData<T>(result);
	auto &result_mask = FlatVector::Validity(result);
	// copy over the first column
	bool result_has_value[STANDARD_VECTOR_SIZE];
	{
		UnifiedVectorFormat vdata;
		args.data[0].ToUnifiedFormat(args.size(), vdata);
		auto input_data = (T *)vdata.data;
		for (idx_t i = 0; i < args.size(); i++) {
			auto vindex = vdata.sel->get_index(i);
			if (vdata.validity.RowIsValid(vindex)) {
				result_data[i] = input_data[vindex];
				result_has_value[i] = true;
			} else {
				result_has_value[i] = false;
			}
		}
	}
	// now handle the remainder of the columns
	for (idx_t col_idx = 1; col_idx < args.ColumnCount(); col_idx++) {
		if (args.data[col_idx].GetVectorType() == VectorType::CONSTANT_VECTOR &&
		    ConstantVector::IsNull(args.data[col_idx])) {
			// ignore null vector
			continue;
		}

		UnifiedVectorFormat vdata;
		args.data[col_idx].ToUnifiedFormat(args.size(), vdata);

		auto input_data = (T *)vdata.data;
		if (!vdata.validity.AllValid()) {
			// potential new null entries: have to check the null mask
			for (idx_t i = 0; i < args.size(); i++) {
				auto vindex = vdata.sel->get_index(i);
				if (vdata.validity.RowIsValid(vindex)) {
					// not a null entry: perform the operation and add to new set
					auto ivalue = input_data[vindex];
					if (!result_has_value[i] || OP::template Operation<T>(ivalue, result_data[i])) {
						result_has_value[i] = true;
						result_data[i] = ivalue;
					}
				}
			}
		} else {
			// no new null entries: only need to perform the operation
			for (idx_t i = 0; i < args.size(); i++) {
				auto vindex = vdata.sel->get_index(i);

				auto ivalue = input_data[vindex];
				if (!result_has_value[i] || OP::template Operation<T>(ivalue, result_data[i])) {
					result_has_value[i] = true;
					result_data[i] = ivalue;
				}
			}
		}
	}
	for (idx_t i = 0; i < args.size(); i++) {
		if (!result_has_value[i]) {
			result_mask.SetInvalid(i);
		}
	}
	result.SetVectorType(result_type);
}

template <typename T, class OP>
ScalarFunction GetLeastGreatestFunction(const LogicalType &type) {
	return ScalarFunction({type}, type, LeastGreatestFunction<T, OP>, nullptr, nullptr, nullptr, nullptr, type,
	                      FunctionSideEffects::NO_SIDE_EFFECTS, FunctionNullHandling::SPECIAL_HANDLING);
}

template <class OP>
static void RegisterLeastGreatest(BuiltinFunctions &set, const string &fun_name) {
	ScalarFunctionSet fun_set(fun_name);
	fun_set.AddFunction(ScalarFunction({LogicalType::BIGINT}, LogicalType::BIGINT, LeastGreatestFunction<int64_t, OP>,
	                                   nullptr, nullptr, nullptr, nullptr, LogicalType::BIGINT,
	                                   FunctionSideEffects::NO_SIDE_EFFECTS, FunctionNullHandling::SPECIAL_HANDLING));
	fun_set.AddFunction(ScalarFunction(
	    {LogicalType::HUGEINT}, LogicalType::HUGEINT, LeastGreatestFunction<hugeint_t, OP>, nullptr, nullptr, nullptr,
	    nullptr, LogicalType::HUGEINT, FunctionSideEffects::NO_SIDE_EFFECTS, FunctionNullHandling::SPECIAL_HANDLING));
	fun_set.AddFunction(ScalarFunction({LogicalType::DOUBLE}, LogicalType::DOUBLE, LeastGreatestFunction<double, OP>,
	                                   nullptr, nullptr, nullptr, nullptr, LogicalType::DOUBLE,
	                                   FunctionSideEffects::NO_SIDE_EFFECTS, FunctionNullHandling::SPECIAL_HANDLING));
	fun_set.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                                   LeastGreatestFunction<string_t, OP, true>, nullptr, nullptr, nullptr, nullptr,
	                                   LogicalType::VARCHAR, FunctionSideEffects::NO_SIDE_EFFECTS,
	                                   FunctionNullHandling::SPECIAL_HANDLING));

	fun_set.AddFunction(GetLeastGreatestFunction<timestamp_t, OP>(LogicalType::TIMESTAMP));
	fun_set.AddFunction(GetLeastGreatestFunction<time_t, OP>(LogicalType::TIME));
	fun_set.AddFunction(GetLeastGreatestFunction<date_t, OP>(LogicalType::DATE));

	fun_set.AddFunction(GetLeastGreatestFunction<timestamp_t, OP>(LogicalType::TIMESTAMP_TZ));
	fun_set.AddFunction(GetLeastGreatestFunction<time_t, OP>(LogicalType::TIME_TZ));

	set.AddFunction(fun_set);
}

void LeastFun::RegisterFunction(BuiltinFunctions &set) {
	RegisterLeastGreatest<duckdb::LessThan>(set, "least");
}

void GreatestFun::RegisterFunction(BuiltinFunctions &set) {
	RegisterLeastGreatest<duckdb::GreaterThan>(set, "greatest");
}

} // namespace duckdb
