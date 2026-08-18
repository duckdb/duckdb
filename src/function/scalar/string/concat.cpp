#include "duckdb/common/vector/constant_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/scalar/string_functions.hpp"

#include "duckdb/planner/expression/bound_function_expression.hpp"

#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/planner/expression/bound_argument_pack.hpp"
#include "duckdb/storage/statistics/struct_stats.hpp"

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

//! The values being concatenated: the concat functions collect theirs with a "*values" parameter, the concat
//! operator takes two ordinary arguments.
vector<reference<Vector>> ConcatInputs(DataChunk &args) {
	vector<reference<Vector>> inputs;
	for (auto &column : args.data) {
		if (!ArgumentPack::IsPackType(column.GetType())) {
			inputs.push_back(column);
			continue;
		}
		for (auto &value : StructVector::GetEntries(column)) {
			inputs.push_back(value);
		}
	}
	return inputs;
}

//! The same, for the argument expressions a bind callback sees
vector<reference<Expression>> ConcatValues(vector<unique_ptr<Expression>> &arguments) {
	vector<reference<Expression>> values;
	for (auto &argument : arguments) {
		if (!ArgumentPack::IsPackType(argument->GetReturnType())) {
			values.push_back(*argument);
			continue;
		}
		for (auto &value : ArgumentPack::GetPackedChildren(*argument)) {
			values.push_back(*value);
		}
	}
	return values;
}

void StringConcatFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto inputs = ConcatInputs(args);
	// iterate over the vectors to count how large the final string will be
	idx_t constant_lengths = 0;
	vector<idx_t> result_lengths(args.size(), 0);
	for (auto &input_ref : inputs) {
		const auto &input = input_ref.get();
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
			for (auto entry : input.Values<string_t>()) {
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
	for (auto &input_ref : inputs) {
		const auto &input = input_ref.get();

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
			for (auto entry : input.Values<string_t>()) {
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
	    args.data[0], args.data[1], result, [&](string_t a, string_t b) {
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
	ListConcatInputData(const Vector &input, idx_t size)
	    : input(input), child_vec(ListVector::GetChild(input)), list_data(input.Values<list_entry_t>()) {
	}

	const Vector &input;
	const Vector &child_vec;
	VectorIterator<list_entry_t> list_data;
};

void ListConcatFunction(DataChunk &args, ExpressionState &state, Vector &result, bool is_operator) {
	auto count = args.size();

	vector<ListConcatInputData> input_data;
	for (auto &input_ref : ConcatInputs(args)) {
		const auto &input = input_ref.get();
		if (!is_operator && input.GetType().id() == LogicalTypeId::SQLNULL) {
			// LIST_CONCAT ignores NULL values
			continue;
		}
		input_data.emplace_back(input, count);
	}

	// the || operator yields NULL whenever any input is NULL, while list_concat skips NULLs
	vector<bool> row_invalid(count, false);
	if (is_operator) {
		for (auto &input : input_data) {
			for (idx_t r = 0; r < count; r++) {
				if (!input.list_data[r].IsValid()) {
					row_invalid[r] = true;
				}
			}
		}
	}

	auto result_writer = FlatVector::Writer<list_entry_t>(result, count);
	for (idx_t r = 0; r < count; r++) {
		if (row_invalid[r]) {
			result_writer.WriteNull();
			continue;
		}
		auto list = result_writer.WriteDynamicList();
		for (auto &input : input_data) {
			auto list_val = input.list_data[r];
			if (!list_val.IsValid()) {
				continue;
			}
			const auto &list_entry = list_val.GetValue();
			list.Append(input.child_vec, *FlatVector::IncrementalSelectionVector(),
			            list_entry.offset + list_entry.length, list_entry.offset, list_entry.length);
		}
	}
}

void ConcatFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.BindInfo()->Cast<ConcatFunctionData>();
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

void SetArgumentType(BoundScalarFunction &bound_function, const LogicalType &type, bool is_operator) {
	if (is_operator) {
		bound_function.GetArguments()[0] = type;
		bound_function.GetArguments()[1] = type;
		bound_function.SetReturnType(type);
		return;
	}

	for (auto &arg : bound_function.GetArguments()) {
		if (ArgumentPack::IsPackType(arg)) {
			arg = ArgumentPack::PositionalType(vector<LogicalType>(StructType::GetChildCount(arg), type));
			continue;
		}
		arg = type;
	}
	bound_function.SetReturnType(type);
}

unique_ptr<FunctionData> BindListConcat(ClientContext &context, BoundScalarFunction &bound_function,
                                        vector<unique_ptr<Expression>> &arguments, bool is_operator) {
	LogicalType child_type = LogicalType::SQLNULL;
	bool all_null = true;
	auto values = ConcatValues(arguments);
	for (auto &value : values) {
		auto &arg = value.get();
		auto &return_type = arg.GetReturnType();
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
			for (idx_t arg_idx = 0; arg_idx < values.size(); arg_idx++) {
				if (!type_list.empty()) {
					if (arg_idx + 1 == values.size()) {
						// last argument
						type_list += " and ";
					} else {
						type_list += ", ";
					}
				}
				type_list += values[arg_idx].get().GetReturnType().ToString();
			}
			throw BinderException(arg, "Cannot concatenate types %s - an explicit cast is required", type_list);
		}
		}
		if (!LogicalType::TryGetMaxLogicalType(context, child_type, next_type, child_type)) {
			throw BinderException(arg, "Cannot concatenate lists of types %s[] and %s[] - an explicit cast is required",
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

unique_ptr<FunctionData> BindConcatFunctionInternal(ClientContext &context, BoundScalarFunction &bound_function,
                                                    vector<unique_ptr<Expression>> &arguments, bool is_operator) {
	bool list_concat = false;
	bool all_null = true;
	// blob concat is only supported for the concat operator - regular concat converts to varchar
	bool all_blob = is_operator ? true : false;
	for (auto &value : ConcatValues(arguments)) {
		auto &return_type = value.get().GetReturnType();
		if (return_type.id() == LogicalTypeId::UNKNOWN) {
			throw ParameterNotResolvedException();
		}
		if (return_type.id() == LogicalTypeId::LIST || return_type.id() == LogicalTypeId::ARRAY) {
			list_concat = true;
		}
		if (return_type.id() != LogicalTypeId::BLOB) {
			all_blob = false;
		}
		if (return_type.id() != LogicalTypeId::SQLNULL) {
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
		}

		// tell list_concat apart from concat by the type its parameter accepts - for list_concat that is the
		// element type of the "*lists" parameter, which the pack carries for every value it collected
		const auto &func_args = bound_function.GetArguments();
		auto first_arg = LogicalType(LogicalTypeId::INVALID);
		if (!func_args.empty()) {
			first_arg = ArgumentPack::IsPackType(func_args[0]) && StructType::GetChildCount(func_args[0]) > 0
			                ? StructType::GetChildType(func_args[0], 0)
			                : func_args[0];
		}
		if (first_arg.id() == LogicalTypeId::LIST || first_arg.id() == LogicalTypeId::ARRAY) {
			SetArgumentType(bound_function, LogicalTypeId::SQLNULL, is_operator);
			return make_uniq<ConcatFunctionData>(bound_function.GetReturnType(), is_operator);
		}

		SetArgumentType(bound_function, LogicalTypeId::VARCHAR, is_operator);
		return make_uniq<ConcatFunctionData>(bound_function.GetReturnType(), is_operator);
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

void MergeConcatStats(unique_ptr<BaseStatistics> &stats, const BaseStatistics &next) {
	if (!stats) {
		stats = next.ToUnique();
		return;
	}
	stats->Merge(next);
}

unique_ptr<BaseStatistics> ListConcatStats(ClientContext &context, FunctionStatisticsInput &input) {
	auto &child_stats = input.child_stats;
	auto &expr = input.expr;

	// the lists are collected by a "*lists" parameter, so their statistics are the pack's member statistics
	unique_ptr<BaseStatistics> stats;
	for (idx_t i = 0; i < child_stats.size(); i++) {
		auto &child_type = expr.GetChildren()[i]->GetReturnType();
		if (!ArgumentPack::IsPackType(child_type)) {
			MergeConcatStats(stats, child_stats[i]);
			continue;
		}
		const auto member_count = StructType::GetChildCount(child_type);
		for (idx_t member = 0; member < member_count; member++) {
			MergeConcatStats(stats, StructStats::GetChildStats(child_stats[i], member));
		}
	}
	return stats;
}

} // namespace

ScalarFunction ListConcatFun::GetFunction() {
	// The arguments and return types are set in the binder function.
	FunctionSignature signature;
	signature.AddVarPositionalParameter("lists", LogicalType::LIST(LogicalType::ANY));
	signature.SetReturnType(LogicalType::LIST(LogicalType::ANY));
	auto fun = ScalarFunction("list_concat", std::move(signature), ConcatFunction);
	fun.SetBindCallback(BindConcatFunction);
	fun.SetStatisticsCallback(ListConcatStats);
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
	FunctionSignature signature;
	signature.AddParameter("value", LogicalType::ANY);
	signature.AddVarPositionalParameter("values", LogicalType::ANY);
	signature.SetReturnType(LogicalType::ANY);
	ScalarFunction concat("concat", std::move(signature), ConcatFunction);
	concat.SetBindCallback(BindConcatFunction);
	concat.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	return concat;
}

ScalarFunction ConcatOperatorFun::GetFunction() {
	ScalarFunction concat_op = ScalarFunction("||", {LogicalType::ANY, LogicalType::ANY}, LogicalType::ANY,
	                                          ConcatFunction, BindConcatOperator);
	return concat_op;
}

} // namespace duckdb
