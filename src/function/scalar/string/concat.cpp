#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"

#include <string.h>

namespace duckdb {

static void StringConcatFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	// iterate over the vectors to count how large the final string will be
	idx_t constant_lengths = 0;
	vector<idx_t> result_lengths(args.size(), 0);
	for (idx_t col_idx = 0; col_idx < args.ColumnCount(); col_idx++) {
		auto &input = args.data[col_idx];
		D_ASSERT(input.GetType().id() == LogicalTypeId::VARCHAR);
		if (input.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			if (ConstantVector::IsNull(input)) {
				// constant null, skip
				continue;
			}
			auto input_data = ConstantVector::GetData<string_t>(input);
			constant_lengths += input_data->GetSize();
		} else {
			// non-constant vector: set the result type to a flat vector
			result.SetVectorType(VectorType::FLAT_VECTOR);
			// now get the lengths of each of the input elements
			UnifiedVectorFormat vdata;
			input.ToUnifiedFormat(args.size(), vdata);

			auto input_data = UnifiedVectorFormat::GetData<string_t>(vdata);
			// now add the length of each vector to the result length
			for (idx_t i = 0; i < args.size(); i++) {
				auto idx = vdata.sel->get_index(i);
				if (!vdata.validity.RowIsValid(idx)) {
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
	for (idx_t col_idx = 0; col_idx < args.ColumnCount(); col_idx++) {
		auto &input = args.data[col_idx];

		// loop over the vector and concat to all results
		if (input.GetVectorType() == VectorType::CONSTANT_VECTOR) {
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
				memcpy(result_data[i].GetDataWriteable() + result_lengths[i], input_ptr, input_len);
				result_lengths[i] += input_len;
			}
		} else {
			// standard vector
			UnifiedVectorFormat idata;
			input.ToUnifiedFormat(args.size(), idata);

			auto input_data = UnifiedVectorFormat::GetData<string_t>(idata);
			for (idx_t i = 0; i < args.size(); i++) {
				auto idx = idata.sel->get_index(i);
				if (!idata.validity.RowIsValid(idx)) {
					continue;
				}
				auto input_ptr = input_data[idx].GetData();
				auto input_len = input_data[idx].GetSize();
				memcpy(result_data[i].GetDataWriteable() + result_lengths[i], input_ptr, input_len);
				result_lengths[i] += input_len;
			}
		}
	}
	for (idx_t i = 0; i < args.size(); i++) {
		result_data[i].Finalize();
	}
}

static void ConcatOperator(DataChunk &args, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<string_t, string_t, string_t>(
	    args.data[0], args.data[1], result, args.size(), [&](string_t a, string_t b) {
		    auto a_data = a.GetData();
		    auto b_data = b.GetData();
		    auto a_length = a.GetSize();
		    auto b_length = b.GetSize();

		    auto target_length = a_length + b_length;
		    auto target = StringVector::EmptyString(result, target_length);
		    auto target_data = target.GetDataWriteable();

		    memcpy(target_data, a_data, a_length);
		    memcpy(target_data + a_length, b_data, b_length);
		    target.Finalize();
		    return target;
	    });
}

static void ListConcatFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 2);
	auto count = args.size();

	Vector &lhs = args.data[0];
	Vector &rhs = args.data[1];
	if (lhs.GetType().id() == LogicalTypeId::SQLNULL) {
		result.Reference(rhs);
		return;
	}
	if (rhs.GetType().id() == LogicalTypeId::SQLNULL) {
		result.Reference(lhs);
		return;
	}

	UnifiedVectorFormat lhs_data;
	UnifiedVectorFormat rhs_data;
	lhs.ToUnifiedFormat(count, lhs_data);
	rhs.ToUnifiedFormat(count, rhs_data);
	auto lhs_entries = UnifiedVectorFormat::GetData<list_entry_t>(lhs_data);
	auto rhs_entries = UnifiedVectorFormat::GetData<list_entry_t>(rhs_data);

	auto lhs_list_size = ListVector::GetListSize(lhs);
	auto rhs_list_size = ListVector::GetListSize(rhs);
	auto &lhs_child = ListVector::GetEntry(lhs);
	auto &rhs_child = ListVector::GetEntry(rhs);
	UnifiedVectorFormat lhs_child_data;
	UnifiedVectorFormat rhs_child_data;
	lhs_child.ToUnifiedFormat(lhs_list_size, lhs_child_data);
	rhs_child.ToUnifiedFormat(rhs_list_size, rhs_child_data);

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_entries = FlatVector::GetData<list_entry_t>(result);
	auto &result_validity = FlatVector::Validity(result);

	idx_t offset = 0;
	for (idx_t i = 0; i < count; i++) {
		auto lhs_list_index = lhs_data.sel->get_index(i);
		auto rhs_list_index = rhs_data.sel->get_index(i);
		if (!lhs_data.validity.RowIsValid(lhs_list_index) && !rhs_data.validity.RowIsValid(rhs_list_index)) {
			result_validity.SetInvalid(i);
			continue;
		}
		result_entries[i].offset = offset;
		result_entries[i].length = 0;
		if (lhs_data.validity.RowIsValid(lhs_list_index)) {
			const auto &lhs_entry = lhs_entries[lhs_list_index];
			result_entries[i].length += lhs_entry.length;
			ListVector::Append(result, lhs_child, *lhs_child_data.sel, lhs_entry.offset + lhs_entry.length,
			                   lhs_entry.offset);
		}
		if (rhs_data.validity.RowIsValid(rhs_list_index)) {
			const auto &rhs_entry = rhs_entries[rhs_list_index];
			result_entries[i].length += rhs_entry.length;
			ListVector::Append(result, rhs_child, *rhs_child_data.sel, rhs_entry.offset + rhs_entry.length,
			                   rhs_entry.offset);
		}
		offset += result_entries[i].length;
	}
	D_ASSERT(ListVector::GetListSize(result) == offset);

	if (lhs.GetVectorType() == VectorType::CONSTANT_VECTOR && rhs.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

//static void ConcatFunction(DataChunk &args, ExpressionState &state, Vector &result) {
//	if (list) {
//		ListConcatFunction(args, state, result);
//    } else {
//        StringConcatFunction(args, state, result);
//	}
//}


static unique_ptr<FunctionData> BindConcatFunction(ClientContext &context, ScalarFunction &bound_function,
                                                   vector<unique_ptr<Expression>> &arguments) {
    D_ASSERT(arguments.size() > 1);

	auto &first_arg = arguments[0]->return_type;

	if (arguments.size() > 2 && (
	    first_arg == LogicalTypeId::ARRAY
	    || first_arg == LogicalTypeId::LIST)) {
        throw BinderException("list_concat only accepts two arguments");
    }

	if (first_arg == LogicalTypeId::ARRAY) {
		if (arguments[1]->return_type != LogicalTypeId::ARRAY) {
            throw BinderException("Cannot concatenate types %s and %s", first_arg.ToString(), arguments[1]->return_type.ToString());
        }
        // if either argument is an array, we cast it to a list
        arguments[0] = BoundCastExpression::AddArrayCastToList(context, std::move(arguments[0]));
        arguments[1] = BoundCastExpression::AddArrayCastToList(context, std::move(arguments[1]));
	}

	if (first_arg == LogicalTypeId::LIST) {
		// Now we can assume that the input is a list, and therefor only accepts two arguments
		D_ASSERT(arguments.size() == 2);

		auto &second_arg = arguments[1]->return_type;

		if (first_arg.id() == LogicalTypeId::UNKNOWN || second_arg.id() == LogicalTypeId::UNKNOWN) {
			throw ParameterNotResolvedException();
		} else if (first_arg.id() == LogicalTypeId::SQLNULL || second_arg.id() == LogicalTypeId::SQLNULL) {
			// we mimic postgres behaviour: list_concat(NULL, my_list) = my_list
			auto return_type = second_arg.id() == LogicalTypeId::SQLNULL ? first_arg : second_arg;
			bound_function.arguments[0] = return_type;
			bound_function.arguments[1] = return_type;
			bound_function.return_type = return_type;
		} else {
			if (first_arg.id() != LogicalTypeId::LIST || second_arg.id() != LogicalTypeId::LIST) {
				throw BinderException("Cannot concatenate types %s and %s", first_arg.ToString(), second_arg.ToString());
			}

			// Resolve list type
			LogicalType child_type = LogicalType::SQLNULL;
			for (const auto &argument : arguments) {
				auto &next_type = ListType::GetChildType(argument->return_type);
				if (!LogicalType::TryGetMaxLogicalType(context, child_type, next_type, child_type)) {
					throw BinderException("Cannot concatenate lists of types %s[] and %s[] - an explicit cast is required",
					                      child_type.ToString(), next_type.ToString());
				}
			}
			auto list_type = LogicalType::LIST(child_type);

			bound_function.arguments[0] = list_type;
			bound_function.arguments[1] = list_type;
			bound_function.return_type = list_type;
		}
		return make_uniq<VariableReturnBindData>(bound_function.return_type);
    }

	// we can now assume that the input is a string or castable to a string
    for (auto &arg : bound_function.arguments) {
        arg = LogicalType::VARCHAR;
    }
    bound_function.varargs = LogicalType::VARCHAR;
    return nullptr;
}

static unique_ptr<BaseStatistics> ListConcatStats(ClientContext &context, FunctionStatisticsInput &input) {
	auto &child_stats = input.child_stats;
	D_ASSERT(child_stats.size() == 2);

	auto &left_stats = child_stats[0];
	auto &right_stats = child_stats[1];

	auto stats = left_stats.ToUnique();
	stats->Merge(right_stats);

	return stats;
}

ScalarFunction ListConcatFun::GetFunction() {
	// the arguments and return types are actually set in the binder function
	auto fun = ScalarFunction({LogicalType::ANY, LogicalType::ANY},
	                          LogicalType::LIST(LogicalType::ANY), ListConcatFunction, BindConcatFunction, nullptr,
	                          ListConcatStats);
	fun.varargs = LogicalType::ANY;
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	return fun;
}

void ConcatFun::RegisterFunction(BuiltinFunctions &set) {
	// the concat operator and concat function have different behavior regarding NULLs
	// this is strange but seems consistent with postgresql and mysql
	// (sqlite does not support the concat function, only the concat operator)

	// the concat operator behaves as one would expect: any NULL value present results in a NULL
	// i.e. NULL || 'hello' = NULL
	// the concat function, however, treats NULL values as an empty string
	// i.e. concat(NULL, 'hello') = 'hello'

	ScalarFunction concat =
	    ScalarFunction("concat", {LogicalType::ANY}, LogicalType::ANY, StringConcatFunction, BindConcatFunction);
	concat.varargs = LogicalType::ANY;
	concat.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	set.AddFunction(concat);

	ScalarFunction concat_op =
	    ScalarFunction("||", {LogicalType::ANY, LogicalType::ANY}, LogicalType::ANY, ConcatOperator, BindConcatFunction);
	concat.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	set.AddFunction(concat_op);

	// Add the string concat function
//	auto string_concat =
//	    ScalarFunction("concat", {LogicalType::ANY, LogicalType::ANY}, LogicalType::ANY, StringConcatFunction, BindConcatFunction);
//	string_concat.varargs = LogicalType::ANY;
//	string_concat.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
//	set.AddFunction(string_concat);
//
//	// Add the list concat function
//	auto list_concat = ListConcatFun::GetFunction();
//	set.AddFunction({"list_concat", "list_cat", "array_concat", "array_cat"}, list_concat);
//
//	// Adds three functions to the "||" set, STRING, BLOB, and LIST.
//	ScalarFunctionSet concat_op("||");
//	concat_op.AddFunction(
//	    ScalarFunction({LogicalType::ANY, LogicalType::ANY}, LogicalType::ANY, ConcatOperator, BindConcatFunction));
//	// TODO: Does this one actually work?
//	concat_op.AddFunction(ScalarFunction({LogicalType::BLOB, LogicalType::BLOB}, LogicalType::BLOB, ConcatOperator));
//	concat_op.AddFunction(list_concat);
//	for (auto &fun : concat_op.functions) {
//		fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
//		fun.varargs = LogicalType::ANY;
//	}
//	set.AddFunction(concat_op);
//
//	// Add the concat_ws function (concat with separator)
//	auto string_concat_ws = ScalarFunction("concat_ws", {LogicalType::VARCHAR, LogicalType::ANY, LogicalType::ANY},
//	                                          LogicalType::VARCHAR, ConcatWSFunction, BindConcatFunction);
//	string_concat_ws.varargs = LogicalType::ANY;
//	string_concat_ws.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
//	set.AddFunction(string_concat_ws);
}

} // namespace duckdb
