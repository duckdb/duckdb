#include "duckdb/common/vector/constant_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/function/scalar/string_functions.hpp"

#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

namespace {

struct ConcatFunctionData : public FunctionData {
	ConcatFunctionData(const LogicalType &return_type_p, bool is_operator_p)
	    : return_type(return_type_p), is_operator(is_operator_p) {
	}

	LogicalType return_type;

	bool is_operator = false;

public:
	bool Equals(const FunctionData &other_p) const override;
	unique_ptr<FunctionData> Copy() const override;
};

bool ConcatFunctionData::Equals(const FunctionData &other_p) const {
	auto &other = other_p.Cast<ConcatFunctionData>();
	return return_type == other.return_type && is_operator == other.is_operator;
}

unique_ptr<FunctionData> ConcatFunctionData::Copy() const {
	return make_uniq<ConcatFunctionData>(return_type, is_operator);
}

void StringConcatFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	// iterate over the vectors to count how large the final string will be
	idx_t constant_lengths = 0;
	vector<idx_t> result_lengths(args.size(), 0);
	for (idx_t col_idx = 0; col_idx < args.ColumnCount(); col_idx++) {
		auto &input = args.data[col_idx];
		D_ASSERT(input.GetType().InternalType() == PhysicalType::VARCHAR);
		if (input.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			if (ConstantVector::IsNull(input)) {
				// constant null, skip
				continue;
			}
			auto input_data = ConstantVector::GetData<string_t>(input);
			constant_lengths += input_data->GetSize();
		} else {
			// now get the lengths of each of the input elements
			for (auto entry : input.Values<string_t>(args.size())) {
				if (!entry.IsValid()) {
					continue;
				}
				result_lengths[entry.GetIndex()] += entry.GetValue().GetSize();
			}
		}
	}

	// first we allocate the empty strings for each of the values
	auto result_data = FlatVector::ScatterWriter<string_t>(result);
	for (idx_t i = 0; i < args.size(); i++) {
		// allocate an empty string of the required size
		idx_t str_length = constant_lengths + result_lengths[i];
		result_data[i].EmptyString(str_length);
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
			for (auto entry : input.Values<string_t>(args.size())) {
				if (!entry.IsValid()) {
					continue;
				}
				auto &input_str = entry.GetValue();
				auto i = entry.GetIndex();
				auto input_ptr = input_str.GetData();
				auto input_len = input_str.GetSize();
				memcpy(result_data[i].GetDataWriteable() + result_lengths[i], input_ptr, input_len);
				result_lengths[i] += input_len;
			}
		}
	}
	for (idx_t i = 0; i < args.size(); i++) {
		result_data[i].Finalize();
	}
}

void ConcatOperator(DataChunk &args, ExpressionState &state, Vector &result) {
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

struct ListConcatInputData {
	ListConcatInputData(Vector &input, Vector &child_vec) : input(input), child_vec(child_vec) {
	}

	UnifiedVectorFormat vdata;
	Vector &input;
	Vector &child_vec;
	const list_entry_t *input_entries = nullptr;
};

void ListConcatFunction(DataChunk &args, ExpressionState &state, Vector &result, bool is_operator) {
	auto count = args.size();

	vector<ListConcatInputData> input_data;
	for (auto &input : args.data) {
		if (!is_operator && input.GetType().id() == LogicalTypeId::SQLNULL) {
			// LIST_CONCAT ignores NULL values
			continue;
		}

		auto &child_vec = ListVector::GetChildMutable(input);
		ListConcatInputData data(input, child_vec);
		input.ToUnifiedFormat(count, data.vdata);

		data.input_entries = UnifiedVectorFormat::GetData<list_entry_t>(data.vdata);
		input_data.push_back(std::move(data));
	}

	vector<sel_t> child_idx_data;
	idx_t offset = 0;
	auto result_data = FlatVector::ScatterWriter<list_entry_t>(result);
	for (idx_t i = 0; i < count; i++) {
		result_data[i].offset = offset;
		result_data[i].length = 0;
		for (auto &data : input_data) {
			auto list_index = data.vdata.sel->get_index(i);
			if (!data.vdata.validity.RowIsValid(list_index)) {
				// LIST_CONCAT ignores NULL values, but || does not
				if (is_operator) {
					result_data.SetInvalid(i);
				}
				continue;
			}
			const auto &list_entry = data.input_entries[list_index];
			result_data[i].length += list_entry.length;
			if (child_idx_data.size() < list_entry.length) {
				child_idx_data.resize(NextPowerOfTwo(list_entry.length));
			}
			for (idx_t child_idx = 0; child_idx < list_entry.length; child_idx++) {
				child_idx_data[child_idx] = NumericCast<sel_t>(list_entry.offset + child_idx);
			}
			SelectionVector child_sel(child_idx_data.data(), list_entry.length);
			ListVector::Append(result, data.child_vec, child_sel, list_entry.length);
		}
		offset += result_data[i].length;
	}
	ListVector::SetListSize(result, offset);
}

void ConcatFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.bind_info->Cast<ConcatFunctionData>();
	if (info.return_type.id() == LogicalTypeId::SQLNULL) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		return;
	}
	if (info.return_type.id() == LogicalTypeId::LIST) {
		return ListConcatFunction(args, state, result, info.is_operator);
	}
	if (info.is_operator) {
		return ConcatOperator(args, state, result);
	}
	return StringConcatFunction(args, state, result);
}

void SetArgumentType(ScalarFunction &bound_function, const LogicalType &type, bool is_operator) {
	if (is_operator) {
		bound_function.GetArguments()[0] = type;
		bound_function.GetArguments()[1] = type;
		bound_function.SetReturnType(type);
		return;
	}

	for (auto &arg : bound_function.GetArguments()) {
		arg = type;
	}
	bound_function.SetVarArgs(type);
	bound_function.SetReturnType(type);
}

unique_ptr<FunctionData> BindListConcat(ClientContext &context, ScalarFunction &bound_function,
                                        vector<unique_ptr<Expression>> &arguments, bool is_operator) {
	LogicalType child_type = LogicalType::SQLNULL;
	bool all_null = true;
	for (auto &arg : arguments) {
		auto &return_type = arg->return_type;
		if (return_type == LogicalTypeId::SQLNULL) {
			// we mimic postgres behaviour: list_concat(NULL, my_list) = my_list
			continue;
		}
		all_null = false;
		LogicalType next_type = LogicalTypeId::INVALID;
		switch (return_type.id()) {
		case LogicalTypeId::UNKNOWN:
			throw ParameterNotResolvedException();
		case LogicalTypeId::LIST:
			next_type = ListType::GetChildType(return_type);
			break;
		case LogicalTypeId::ARRAY:
			next_type = ArrayType::GetChildType(return_type);
			break;
		default: {
			string type_list;
			for (idx_t arg_idx = 0; arg_idx < arguments.size(); arg_idx++) {
				if (!type_list.empty()) {
					if (arg_idx + 1 == arguments.size()) {
						// last argument
						type_list += " and ";
					} else {
						type_list += ", ";
					}
				}
				type_list += arguments[arg_idx]->return_type.ToString();
			}
			throw BinderException(*arg, "Cannot concatenate types %s - an explicit cast is required", type_list);
		}
		}
		if (!LogicalType::TryGetMaxLogicalType(context, child_type, next_type, child_type)) {
			throw BinderException(*arg,
			                      "Cannot concatenate lists of types %s[] and %s[] - an explicit cast is required",
			                      child_type.ToString(), next_type.ToString());
		}
	}
	if (all_null) {
		// all arguments are NULL
		SetArgumentType(bound_function, LogicalTypeId::SQLNULL, is_operator);
		return make_uniq<ConcatFunctionData>(bound_function.GetReturnType(), is_operator);
	}
	auto list_type = LogicalType::LIST(child_type);

	SetArgumentType(bound_function, list_type, is_operator);
	return make_uniq<ConcatFunctionData>(bound_function.GetReturnType(), is_operator);
}

unique_ptr<FunctionData> BindConcatFunctionInternal(ClientContext &context, ScalarFunction &bound_function,
                                                    vector<unique_ptr<Expression>> &arguments, bool is_operator) {
	bool list_concat = false;
	bool all_null = true;
	// blob concat is only supported for the concat operator - regular concat converts to varchar
	bool all_blob = is_operator ? true : false;
	for (auto &arg : arguments) {
		if (arg->return_type.id() == LogicalTypeId::UNKNOWN) {
			throw ParameterNotResolvedException();
		}
		if (arg->return_type.id() == LogicalTypeId::LIST || arg->return_type.id() == LogicalTypeId::ARRAY) {
			list_concat = true;
		}
		if (arg->return_type.id() != LogicalTypeId::BLOB) {
			all_blob = false;
		}
		if (arg->return_type.id() != LogicalTypeId::SQLNULL) {
			all_null = false;
		}
	}
	if (list_concat) {
		return BindListConcat(context, bound_function, arguments, is_operator);
	}
	if (all_null) {
		if (is_operator) {
			SetArgumentType(bound_function, LogicalTypeId::SQLNULL, is_operator);
			return make_uniq<ConcatFunctionData>(bound_function.GetReturnType(), is_operator);
		} else if (bound_function.GetVarArgs().id() == LogicalTypeId::LIST ||
		           bound_function.GetVarArgs().id() == LogicalTypeId::ARRAY) {
			SetArgumentType(bound_function, LogicalTypeId::SQLNULL, is_operator);
			return make_uniq<ConcatFunctionData>(bound_function.GetReturnType(), is_operator);
		} else {
			SetArgumentType(bound_function, LogicalTypeId::VARCHAR, is_operator);
			return make_uniq<ConcatFunctionData>(bound_function.GetReturnType(), is_operator);
		}
	}
	auto return_type = all_blob ? LogicalType::BLOB : LogicalType::VARCHAR;

	// we can now assume that the input is a string or castable to a string
	SetArgumentType(bound_function, return_type, is_operator);
	return make_uniq<ConcatFunctionData>(bound_function.GetReturnType(), is_operator);
}

unique_ptr<FunctionData> BindConcatFunction(BindScalarFunctionInput &input) {
	auto &context = input.GetClientContext();
	auto &bound_function = input.GetBoundFunction();
	auto &arguments = input.GetArguments();
	return BindConcatFunctionInternal(context, bound_function, arguments, false);
}

unique_ptr<FunctionData> BindConcatOperator(BindScalarFunctionInput &input) {
	auto &context = input.GetClientContext();
	auto &bound_function = input.GetBoundFunction();
	auto &arguments = input.GetArguments();
	return BindConcatFunctionInternal(context, bound_function, arguments, true);
}

unique_ptr<BaseStatistics> ListConcatStats(ClientContext &context, FunctionStatisticsInput &input) {
	auto &child_stats = input.child_stats;
	auto stats = child_stats[0].ToUnique();
	for (idx_t i = 1; i < child_stats.size(); i++) {
		stats->Merge(child_stats[i]);
	}
	return stats;
}

} // namespace

ScalarFunction ListConcatFun::GetFunction() {
	// The arguments and return types are set in the binder function.
	auto fun =
	    ScalarFunction({}, LogicalType::LIST(LogicalType::ANY), ConcatFunction, BindConcatFunction, ListConcatStats);
	fun.SetVarArgs(LogicalType::LIST(LogicalType::ANY));
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	return fun;
}

// the concat operator and concat function have different behavior regarding NULLs
// this is strange but seems consistent with postgresql and mysql
// (sqlite does not support the concat function, only the concat operator)

// the concat operator behaves as one would expect: any NULL value present results in a NULL
// i.e. NULL || 'hello' = NULL
// the concat function, however, treats NULL values as an empty string
// i.e. concat(NULL, 'hello') = 'hello'
ScalarFunction ConcatFun::GetFunction() {
	ScalarFunction concat =
	    ScalarFunction("concat", {LogicalType::ANY}, LogicalType::ANY, ConcatFunction, BindConcatFunction);
	concat.SetVarArgs(LogicalType::ANY);
	concat.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	return concat;
}

ScalarFunction ConcatOperatorFun::GetFunction() {
	ScalarFunction concat_op = ScalarFunction("||", {LogicalType::ANY, LogicalType::ANY}, LogicalType::ANY,
	                                          ConcatFunction, BindConcatOperator);
	return concat_op;
}

} // namespace duckdb
