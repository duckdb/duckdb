#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

#include <string.h>

namespace duckdb {

struct ConcatFunctionData : public FunctionData {
	ConcatFunctionData(const LogicalType &return_type_p, bool is_operator_p)
	    : return_type(return_type_p), is_operator(is_operator_p) {
	}
	~ConcatFunctionData() override;

	LogicalType return_type;

	bool is_operator = false;

public:
	bool Equals(const FunctionData &other_p) const override;
	unique_ptr<FunctionData> Copy() const override;
};

ConcatFunctionData::~ConcatFunctionData() {
}

bool ConcatFunctionData::Equals(const FunctionData &other_p) const {
	auto &other = other_p.Cast<ConcatFunctionData>();
	return return_type == other.return_type && is_operator == other.is_operator;
}

unique_ptr<FunctionData> ConcatFunctionData::Copy() const {
	return make_uniq<ConcatFunctionData>(return_type, is_operator);
}

static void StringConcatFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
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

struct ListConcatInputData {
	ListConcatInputData(Vector &input, Vector &child_vec) : input(input), child_vec(child_vec) {
	}

	UnifiedVectorFormat vdata;
	Vector &input;
	Vector &child_vec;
	UnifiedVectorFormat child_vdata;
	const list_entry_t *input_entries = nullptr;
};

static void ListConcatFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto count = args.size();

	auto result_entries = FlatVector::GetData<list_entry_t>(result);
	vector<ListConcatInputData> input_data;
	for (auto &input : args.data) {
		if (input.GetType().id() == LogicalTypeId::SQLNULL) {
			// ignore NULL values
			continue;
		}

		auto &child_vec = ListVector::GetEntry(input);
		ListConcatInputData data(input, child_vec);
		input.ToUnifiedFormat(count, data.vdata);

		data.input_entries = UnifiedVectorFormat::GetData<list_entry_t>(data.vdata);
		auto list_size = ListVector::GetListSize(input);

		child_vec.ToUnifiedFormat(list_size, data.child_vdata);

		input_data.push_back(std::move(data));
	}

	idx_t offset = 0;
	for (idx_t i = 0; i < count; i++) {
		auto &result_entry = result_entries[i];
		result_entry.offset = offset;
		result_entry.length = 0;
		for (auto &data : input_data) {
			auto list_index = data.vdata.sel->get_index(i);
			if (!data.vdata.validity.RowIsValid(list_index)) {
				continue;
			}
			const auto &list_entry = data.input_entries[list_index];
			result_entry.length += list_entry.length;
			ListVector::Append(result, data.child_vec, *data.child_vdata.sel, list_entry.offset + list_entry.length,
			                   list_entry.offset);
		}
		offset += result_entry.length;
	}
	ListVector::SetListSize(result, offset);

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

static void ConcatFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.bind_info->Cast<ConcatFunctionData>();
	if (info.return_type.id() == LogicalTypeId::LIST) {
		return ListConcatFunction(args, state, result);
	} else if (info.is_operator) {
		return ConcatOperator(args, state, result);
	}
	return StringConcatFunction(args, state, result);
}

static void SetArgumentType(ScalarFunction &bound_function, const LogicalType &type, bool is_operator) {
	if (is_operator) {
		bound_function.arguments[0] = type;
		bound_function.arguments[1] = type;
		bound_function.return_type = type;
		return;
	}

	for (auto &arg : bound_function.arguments) {
		arg = type;
	}
	bound_function.varargs = type;
	bound_function.return_type = type;
}

static unique_ptr<FunctionData> BindListConcat(ClientContext &context, ScalarFunction &bound_function,
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
		return make_uniq<ConcatFunctionData>(bound_function.return_type, is_operator);
	}
	auto list_type = LogicalType::LIST(child_type);

	SetArgumentType(bound_function, list_type, is_operator);
	return make_uniq<ConcatFunctionData>(bound_function.return_type, is_operator);
}

static unique_ptr<FunctionData> BindConcatFunctionInternal(ClientContext &context, ScalarFunction &bound_function,
                                                           vector<unique_ptr<Expression>> &arguments,
                                                           bool is_operator) {
	bool list_concat = false;
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
	}
	if (list_concat) {
		return BindListConcat(context, bound_function, arguments, is_operator);
	}
	auto return_type = all_blob ? LogicalType::BLOB : LogicalType::VARCHAR;

	// we can now assume that the input is a string or castable to a string
	SetArgumentType(bound_function, return_type, is_operator);
	return make_uniq<ConcatFunctionData>(bound_function.return_type, is_operator);
}

static unique_ptr<FunctionData> BindConcatFunction(ClientContext &context, ScalarFunction &bound_function,
                                                   vector<unique_ptr<Expression>> &arguments) {
	return BindConcatFunctionInternal(context, bound_function, arguments, false);
}

static unique_ptr<FunctionData> BindConcatOperator(ClientContext &context, ScalarFunction &bound_function,
                                                   vector<unique_ptr<Expression>> &arguments) {
	return BindConcatFunctionInternal(context, bound_function, arguments, true);
}

static unique_ptr<BaseStatistics> ListConcatStats(ClientContext &context, FunctionStatisticsInput &input) {
	auto &child_stats = input.child_stats;
	auto stats = child_stats[0].ToUnique();
	for (idx_t i = 1; i < child_stats.size(); i++) {
		stats->Merge(child_stats[i]);
	}
	return stats;
}

ScalarFunction ListConcatFun::GetFunction() {
	// The arguments and return types are set in the binder function.
	auto fun = ScalarFunction({LogicalType::LIST(LogicalType::ANY), LogicalType::LIST(LogicalType::ANY)},
	                          LogicalType::LIST(LogicalType::ANY), ConcatFunction, BindConcatFunction, nullptr,
	                          ListConcatStats);
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	return fun;
}

void ListConcatFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction({"list_concat", "list_cat", "array_concat", "array_cat"}, GetFunction());
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
	    ScalarFunction("concat", {LogicalType::ANY}, LogicalType::ANY, ConcatFunction, BindConcatFunction);
	concat.varargs = LogicalType::ANY;
	concat.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	set.AddFunction(concat);

	ScalarFunction concat_op = ScalarFunction("||", {LogicalType::ANY, LogicalType::ANY}, LogicalType::ANY,
	                                          ConcatFunction, BindConcatOperator);
	concat.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	set.AddFunction(concat_op);
}

} // namespace duckdb
