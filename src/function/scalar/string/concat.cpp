#include "duckdb/function/scalar/string_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"

#include <string.h>

using namespace std;

namespace duckdb {

static void concat_function(DataChunk &args, ExpressionState &state, Vector &result) {
	result.vector_type = VectorType::CONSTANT_VECTOR;
	// iterate over the vectors to count how large the final string will be
	idx_t constant_lengths = 0;
	vector<idx_t> result_lengths(args.size(), 0);
	for (idx_t col_idx = 0; col_idx < args.column_count(); col_idx++) {
		auto &input = args.data[col_idx];
		assert(input.type == TypeId::VARCHAR);
		if (input.vector_type == VectorType::CONSTANT_VECTOR) {
			if (ConstantVector::IsNull(input)) {
				// constant null, skip
				continue;
			}
			auto input_data = ConstantVector::GetData<string_t>(input);
			constant_lengths += input_data->GetSize();
		} else {
			// non-constant vector: set the result type to a flat vector
			result.vector_type = VectorType::FLAT_VECTOR;
			// now get the lengths of each of the input elements
			VectorData vdata;
			input.Orrify(args.size(), vdata);

			auto input_data = (string_t *)vdata.data;
			// now add the length of each vector to the result length
			for (idx_t i = 0; i < args.size(); i++) {
				auto idx = vdata.sel->get_index(i);
				if ((*vdata.nullmask)[idx]) {
					continue;
				}
				result_lengths[i] += input_data[idx].GetSize();
			}
		}
	}

	// first we allocate the empty strings for each of the values
	auto result_data = FlatVector::GetData<string_t>(result);
	for (idx_t i = 0; i < args.size(); i++) {
		// allocate an empty string of the required size
		idx_t str_length = constant_lengths + result_lengths[i];
		result_data[i] = StringVector::EmptyString(result, str_length);
		// we reuse the result_lengths vector to store the currently appended size
		result_lengths[i] = 0;
	}

	// now that the empty space for the strings has been allocated, perform the concatenation
	for (idx_t col_idx = 0; col_idx < args.column_count(); col_idx++) {
		auto &input = args.data[col_idx];

		// loop over the vector and concat to all results
		if (input.vector_type == VectorType::CONSTANT_VECTOR) {
			// constant vector
			if (ConstantVector::IsNull(input)) {
				// constant null, skip
				continue;
			}
			// append the constant vector to each of the strings
			auto input_data = ConstantVector::GetData<string_t>(input);
			auto input_ptr = input_data->GetData();
			auto input_len = input_data->GetSize();
			for (idx_t i = 0; i < args.size(); i++) {
				memcpy(result_data[i].GetData() + result_lengths[i], input_ptr, input_len);
				result_lengths[i] += input_len;
			}
		} else {
			// standard vector
			VectorData idata;
			input.Orrify(args.size(), idata);

			auto input_data = (string_t *)idata.data;
			for (idx_t i = 0; i < args.size(); i++) {
				auto idx = idata.sel->get_index(i);
				if ((*idata.nullmask)[idx]) {
					continue;
				}
				auto input_ptr = input_data[idx].GetData();
				auto input_len = input_data[idx].GetSize();
				memcpy(result_data[i].GetData() + result_lengths[i], input_ptr, input_len);
				result_lengths[i] += input_len;
			}
		}
	}
	for (idx_t i = 0; i < args.size(); i++) {
		result_data[i].Finalize();
	}
}

static void concat_operator(DataChunk &args, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<string_t, string_t, string_t, true>(
	    args.data[0], args.data[1], result, args.size(), [&](string_t a, string_t b) {
		    auto a_data = a.GetData();
		    auto b_data = b.GetData();
		    auto a_length = a.GetSize();
		    auto b_length = b.GetSize();

		    auto target_length = a_length + b_length;
		    auto target = StringVector::EmptyString(result, target_length);
		    auto target_data = target.GetData();

		    memcpy(target_data, a_data, a_length);
		    memcpy(target_data + a_length, b_data, b_length);
		    target.Finalize();
		    return target;
	    });
}

static void templated_concat_ws(DataChunk &args, string_t *sep_data, const SelectionVector &sep_sel,
                                const SelectionVector &rsel, idx_t count, Vector &result) {
	vector<idx_t> result_lengths(args.size(), 0);
	vector<bool> has_results(args.size(), false);
	auto orrified_data = unique_ptr<VectorData[]>(new VectorData[args.column_count() - 1]);
	for (idx_t col_idx = 1; col_idx < args.column_count(); col_idx++) {
		args.data[col_idx].Orrify(args.size(), orrified_data[col_idx - 1]);
	}

	// first figure out the lengths
	for (idx_t col_idx = 1; col_idx < args.column_count(); col_idx++) {
		auto &idata = orrified_data[col_idx - 1];

		auto input_data = (string_t *)idata.data;
		for (idx_t i = 0; i < count; i++) {
			auto ridx = rsel.get_index(i);
			auto sep_idx = sep_sel.get_index(ridx);
			auto idx = idata.sel->get_index(ridx);
			if ((*idata.nullmask)[idx]) {
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
	for (idx_t col_idx = 1; col_idx < args.column_count(); col_idx++) {
		auto &idata = orrified_data[col_idx - 1];
		auto input_data = (string_t *)idata.data;
		for (idx_t i = 0; i < count; i++) {
			auto ridx = rsel.get_index(i);
			auto sep_idx = sep_sel.get_index(ridx);
			auto idx = idata.sel->get_index(ridx);
			if ((*idata.nullmask)[idx]) {
				continue;
			}
			if (has_results[ridx]) {
				auto sep_size = sep_data[sep_idx].GetSize();
				auto sep_ptr = sep_data[sep_idx].GetData();
				memcpy(result_data[ridx].GetData() + result_lengths[ridx], sep_ptr, sep_size);
				result_lengths[ridx] += sep_size;
			}
			auto input_ptr = input_data[idx].GetData();
			auto input_len = input_data[idx].GetSize();
			memcpy(result_data[ridx].GetData() + result_lengths[ridx], input_ptr, input_len);
			result_lengths[ridx] += input_len;
			has_results[ridx] = true;
		}
	}
	for (idx_t i = 0; i < count; i++) {
		auto ridx = rsel.get_index(i);
		result_data[ridx].Finalize();
	}
}

static void concat_ws_function(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &separator = args.data[0];
	VectorData vdata;
	separator.Orrify(args.size(), vdata);

	result.vector_type = VectorType::CONSTANT_VECTOR;
	for (idx_t col_idx = 0; col_idx < args.column_count(); col_idx++) {
		if (args.data[col_idx].vector_type != VectorType::CONSTANT_VECTOR) {
			result.vector_type = VectorType::FLAT_VECTOR;
			break;
		}
	}
	switch (separator.vector_type) {
	case VectorType::CONSTANT_VECTOR:
		if (ConstantVector::IsNull(separator)) {
			// constant NULL as separator: return constant NULL vector
			result.vector_type = VectorType::CONSTANT_VECTOR;
			ConstantVector::SetNull(result, true);
			return;
		}
		// no null values
		templated_concat_ws(args, (string_t *)vdata.data, *vdata.sel, FlatVector::IncrementalSelectionVector,
		                    args.size(), result);
		return;
	default: {
		// default case: loop over nullmask and create a non-null selection vector
		idx_t not_null_count = 0;
		SelectionVector not_null_vector(STANDARD_VECTOR_SIZE);
		auto &result_nullmask = FlatVector::Nullmask(result);
		for (idx_t i = 0; i < args.size(); i++) {
			if ((*vdata.nullmask)[vdata.sel->get_index(i)]) {
				result_nullmask[i] = true;
			} else {
				not_null_vector.set_index(not_null_count++, i);
			}
		}
		templated_concat_ws(args, (string_t *)vdata.data, *vdata.sel, not_null_vector, not_null_count, result);
		return;
	}
	}
}

void ConcatFun::RegisterFunction(BuiltinFunctions &set) {
	// the concat operator and concat function have different behavior regarding NULLs
	// this is strange but seems consistent with postgresql and mysql
	// (sqlite does not support the concat function, only the concat operator)

	// the concat operator behaves as one would expect: any NULL value present results in a NULL
	// i.e. NULL || 'hello' = NULL
	// the concat function, however, treats NULL values as an empty string
	// i.e. concat(NULL, 'hello') = 'hello'
	// concat_ws functions similarly to the concat function, except the result is NULL if the separator is NULL
	// if the separator is not NULL, however, NULL values are counted as empty string
	// there is one separate rule: there are no separators added between NULL values
	// so the NULL value and empty string are different!
	// e.g.:
	// concat_ws(',', NULL, NULL) = ""
	// concat_ws(',', '', '') = ","
	ScalarFunction concat = ScalarFunction("concat", {SQLType::VARCHAR}, SQLType::VARCHAR, concat_function);
	concat.varargs = SQLType::VARCHAR;
	set.AddFunction(concat);

	set.AddFunction(ScalarFunction("||", {SQLType::VARCHAR, SQLType::VARCHAR}, SQLType::VARCHAR, concat_operator));

	ScalarFunction concat_ws =
	    ScalarFunction("concat_ws", {SQLType::VARCHAR, SQLType::VARCHAR}, SQLType::VARCHAR, concat_ws_function);
	concat_ws.varargs = SQLType::VARCHAR;
	set.AddFunction(concat_ws);
}

} // namespace duckdb
