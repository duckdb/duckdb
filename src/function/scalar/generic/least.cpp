#include "duckdb/function/scalar/generic_functions.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"

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
		if (args.data[col_idx].GetVectorType() == VectorType::CONSTANT_VECTOR) {
			if (ConstantVector::IsNull(args.data[col_idx])) {
				// constant NULL: result is constant NULL
				result.SetVectorType(VectorType::CONSTANT_VECTOR);
				ConstantVector::SetNull(result, true);
				return;
			}
		} else {
			// non-constant input: result is not a constant vector
			result_type = VectorType::FLAT_VECTOR;
		}
		if (IS_STRING) {
			// for string vectors we add a reference to the heap of the children
			StringVector::AddHeapReference(result, args.data[col_idx]);
		}
	}

	// we start off performing a binary operation between the first two inputs, where we store the lowest (or highest)
	// directly in the result
	BinaryExecutor::ExecuteGeneric<T, T, T, BinarySingleArgumentOperatorWrapper, LeastOperator<OP>, bool>(
	    args.data[0], args.data[1], result, args.size(), false);

	// now we loop over the other columns and compare it to the stored result
	auto result_data = FlatVector::GetData<T>(result);
	auto &result_mask = FlatVector::Validity(result);
	SelectionVector rsel;
	idx_t rcount = 0;
	// create a selection vector from the mask
	rsel.Initialize();
	for (idx_t i = 0; i < args.size(); i++) {
		if (result_mask.RowIsValid(i)) {
			rsel.set_index(rcount++, i);
		}
	}
	for (idx_t col_idx = 2; col_idx < args.ColumnCount(); col_idx++) {
		VectorData vdata;
		args.data[col_idx].Orrify(args.size(), vdata);

		auto input_data = (T *)vdata.data;
		if (!vdata.validity.AllValid()) {
			// potential new null entries: have to check the null mask
			idx_t new_count = 0;
			for (idx_t i = 0; i < rcount; i++) {
				auto rindex = rsel.get_index(i);
				auto vindex = vdata.sel->get_index(rindex);
				if (!vdata.validity.RowIsValid(vindex)) {
					// new null entry: set nullmask
					result_mask.SetInvalid(rindex);
				} else {
					// not a null entry: perform the operation and add to new set
					auto ivalue = input_data[vindex];
					if (OP::template Operation<T>(ivalue, result_data[rindex])) {
						result_data[rindex] = ivalue;
					}
					rsel.set_index(new_count++, rindex);
				}
			}
			rcount = new_count;
		} else {
			// no new null entries: only need to perform the operation
			for (idx_t i = 0; i < rcount; i++) {
				auto rindex = rsel.get_index(i);
				auto vindex = vdata.sel->get_index(rindex);

				auto ivalue = input_data[vindex];
				if (OP::template Operation<T>(ivalue, result_data[rindex])) {
					result_data[rindex] = ivalue;
				}
			}
		}
	}
	result.SetVectorType(result_type);
}

template <typename T, class OP>
ScalarFunction GetLeastGreatestFunction(const LogicalType &type) {
	return ScalarFunction({type}, type, LeastGreatestFunction<T, OP>, false, nullptr, nullptr, nullptr, nullptr, type);
}

template <class OP>
static void RegisterLeastGreatest(BuiltinFunctions &set, const string &fun_name) {
	ScalarFunctionSet fun_set(fun_name);
	fun_set.AddFunction(ScalarFunction({LogicalType::BIGINT}, LogicalType::BIGINT, LeastGreatestFunction<int64_t, OP>,
	                                   false, nullptr, nullptr, nullptr, nullptr, LogicalType::BIGINT));
	fun_set.AddFunction(ScalarFunction({LogicalType::HUGEINT}, LogicalType::HUGEINT,
	                                   LeastGreatestFunction<hugeint_t, OP>, false, nullptr, nullptr, nullptr, nullptr,
	                                   LogicalType::HUGEINT));
	fun_set.AddFunction(ScalarFunction({LogicalType::DOUBLE}, LogicalType::DOUBLE, LeastGreatestFunction<double, OP>,
	                                   false, nullptr, nullptr, nullptr, nullptr, LogicalType::DOUBLE));
	fun_set.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                                   LeastGreatestFunction<string_t, OP, true>, false, nullptr, nullptr, nullptr,
	                                   nullptr, LogicalType::VARCHAR));

	fun_set.AddFunction(GetLeastGreatestFunction<timestamp_t, OP>(LogicalType::TIMESTAMP));
	fun_set.AddFunction(GetLeastGreatestFunction<time_t, OP>(LogicalType::TIME));
	fun_set.AddFunction(GetLeastGreatestFunction<date_t, OP>(LogicalType::DATE));

	fun_set.AddFunction(GetLeastGreatestFunction<timestamp_t, OP>(LogicalType::TIMESTAMP_TZ));
	fun_set.AddFunction(GetLeastGreatestFunction<time_t, OP>(LogicalType::TIME_TZ));
	fun_set.AddFunction(GetLeastGreatestFunction<date_t, OP>(LogicalType::DATE_TZ));

	set.AddFunction(fun_set);
}

void LeastFun::RegisterFunction(BuiltinFunctions &set) {
	RegisterLeastGreatest<duckdb::LessThan>(set, "least");
}

void GreatestFun::RegisterFunction(BuiltinFunctions &set) {
	RegisterLeastGreatest<duckdb::GreaterThan>(set, "greatest");
}

} // namespace duckdb
