#include "duckdb/function/scalar/string_functions.hpp"

#include <string.h>

namespace duckdb {

static void TemplatedConcatWS(DataChunk &args, const string_t *sep_data, const SelectionVector &sep_sel,
                              const SelectionVector &rsel, idx_t count, Vector &result) {
	vector<idx_t> result_lengths(args.size(), 0);
	vector<bool> has_results(args.size(), false);

	// we overallocate here, but this is important for static analysis
	auto orrified_data = make_unsafe_uniq_array_uninitialized<UnifiedVectorFormat>(args.ColumnCount());

	for (idx_t col_idx = 1; col_idx < args.ColumnCount(); col_idx++) {
		args.data[col_idx].ToUnifiedFormat(args.size(), orrified_data[col_idx - 1]);
	}

	// first figure out the lengths
	for (idx_t col_idx = 1; col_idx < args.ColumnCount(); col_idx++) {
		auto &idata = orrified_data[col_idx - 1];

		auto input_data = UnifiedVectorFormat::GetData<string_t>(idata);
		for (idx_t i = 0; i < count; i++) {
			auto ridx = rsel.get_index(i);
			auto sep_idx = sep_sel.get_index(ridx);
			auto idx = idata.sel->get_index(ridx);
			if (!idata.validity.RowIsValid(idx)) {
				continue;
			}
			if (has_results[ridx]) {
				result_lengths[ridx] += sep_data[sep_idx].GetSize();
			}
			result_lengths[ridx] += input_data[idx].GetSize();
			has_results[ridx] = true;
		}
	}

	// first we allocate the empty strings for each of the values
	auto result_data = FlatVector::GetData<string_t>(result);
	for (idx_t i = 0; i < count; i++) {
		auto ridx = rsel.get_index(i);
		// allocate an empty string of the required size
		result_data[ridx] = StringVector::EmptyString(result, result_lengths[ridx]);
		// we reuse the result_lengths vector to store the currently appended size
		result_lengths[ridx] = 0;
		has_results[ridx] = false;
	}

	// now that the empty space for the strings has been allocated, perform the concatenation
	for (idx_t col_idx = 1; col_idx < args.ColumnCount(); col_idx++) {
		auto &idata = orrified_data[col_idx - 1];
		auto input_data = UnifiedVectorFormat::GetData<string_t>(idata);
		for (idx_t i = 0; i < count; i++) {
			auto ridx = rsel.get_index(i);
			auto sep_idx = sep_sel.get_index(ridx);
			auto idx = idata.sel->get_index(ridx);
			if (!idata.validity.RowIsValid(idx)) {
				continue;
			}
			if (has_results[ridx]) {
				auto sep_size = sep_data[sep_idx].GetSize();
				auto sep_ptr = sep_data[sep_idx].GetData();
				memcpy(result_data[ridx].GetDataWriteable() + result_lengths[ridx], sep_ptr, sep_size);
				result_lengths[ridx] += sep_size;
			}
			auto input_ptr = input_data[idx].GetData();
			auto input_len = input_data[idx].GetSize();
			memcpy(result_data[ridx].GetDataWriteable() + result_lengths[ridx], input_ptr, input_len);
			result_lengths[ridx] += input_len;
			has_results[ridx] = true;
		}
	}
	for (idx_t i = 0; i < count; i++) {
		auto ridx = rsel.get_index(i);
		result_data[ridx].Finalize();
	}
}

static void ConcatWSFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &separator = args.data[0];
	UnifiedVectorFormat vdata;
	separator.ToUnifiedFormat(args.size(), vdata);

	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	for (idx_t col_idx = 0; col_idx < args.ColumnCount(); col_idx++) {
		if (args.data[col_idx].GetVectorType() != VectorType::CONSTANT_VECTOR) {
			result.SetVectorType(VectorType::FLAT_VECTOR);
			break;
		}
	}
	switch (separator.GetVectorType()) {
	case VectorType::CONSTANT_VECTOR: {
		if (ConstantVector::IsNull(separator)) {
			// constant NULL as separator: return constant NULL vector
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(result, true);
			return;
		}
		// no null values
		auto sel = FlatVector::IncrementalSelectionVector();
		TemplatedConcatWS(args, UnifiedVectorFormat::GetData<string_t>(vdata), *vdata.sel, *sel, args.size(), result);
		return;
	}
	default: {
		// default case: loop over nullmask and create a non-null selection vector
		idx_t not_null_count = 0;
		SelectionVector not_null_vector(STANDARD_VECTOR_SIZE);
		auto &result_mask = FlatVector::Validity(result);
		for (idx_t i = 0; i < args.size(); i++) {
			if (!vdata.validity.RowIsValid(vdata.sel->get_index(i))) {
				result_mask.SetInvalid(i);
			} else {
				not_null_vector.set_index(not_null_count++, i);
			}
		}
		TemplatedConcatWS(args, UnifiedVectorFormat::GetData<string_t>(vdata), *vdata.sel, not_null_vector,
		                  not_null_count, result);
		return;
	}
	}
}

static unique_ptr<FunctionData> BindConcatWSFunction(ClientContext &context, ScalarFunction &bound_function,
                                                     vector<unique_ptr<Expression>> &arguments) {
	for (auto &arg : bound_function.arguments) {
		arg = LogicalType::VARCHAR;
	}
	bound_function.varargs = LogicalType::VARCHAR;
	return nullptr;
}

void ConcatWSFun::RegisterFunction(BuiltinFunctions &set) {
	// concat_ws functions similarly to the concat function, except the result is NULL if the separator is NULL
	// if the separator is not NULL, however, NULL values are counted as empty string
	// there is one separate rule: there are no separators added between NULL values,
	// so the NULL value and empty string are different!
	// e.g.:
	// concat_ws(',', NULL, NULL) = ""
	// concat_ws(',', '', '') = ","

	ScalarFunction concat_ws = ScalarFunction("concat_ws", {LogicalType::VARCHAR, LogicalType::ANY},
	                                          LogicalType::VARCHAR, ConcatWSFunction, BindConcatWSFunction);
	concat_ws.varargs = LogicalType::ANY;
	concat_ws.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	set.AddFunction(concat_ws);
}

} // namespace duckdb
