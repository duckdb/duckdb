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

static void HandleArrayBinding(ClientContext &context, vector<unique_ptr<Expression>> &arguments) {
	if (arguments[1]->return_type.id() != LogicalTypeId::ARRAY &&
	    arguments[1]->return_type.id() != LogicalTypeId::SQLNULL) {
		throw BinderException("Cannot concatenate types %s and %s", arguments[0]->return_type.ToString(),
		                      arguments[1]->return_type.ToString());
	}

	// if either argument is an array, we cast it to a list
	arguments[0] = BoundCastExpression::AddArrayCastToList(context, std::move(arguments[0]));
	arguments[1] = BoundCastExpression::AddArrayCastToList(context, std::move(arguments[1]));
}

static unique_ptr<FunctionData> HandleListBinding(ClientContext &context, ScalarFunction &bound_function,
                                                  vector<unique_ptr<Expression>> &arguments, bool is_operator) {
	// list_concat only accepts two arguments
	D_ASSERT(arguments.size() == 2);

	auto &lhs = arguments[0]->return_type;
	auto &rhs = arguments[1]->return_type;

	if (lhs.id() == LogicalTypeId::UNKNOWN || rhs.id() == LogicalTypeId::UNKNOWN) {
		throw ParameterNotResolvedException();
	} else if (lhs.id() == LogicalTypeId::SQLNULL || rhs.id() == LogicalTypeId::SQLNULL) {
		// we mimic postgres behaviour: list_concat(NULL, my_list) = my_list
		auto return_type = rhs.id() == LogicalTypeId::SQLNULL ? lhs : rhs;
		SetArgumentType(bound_function, return_type, is_operator);
		return make_uniq<ConcatFunctionData>(bound_function.return_type, is_operator);
	}
	if (lhs.id() != LogicalTypeId::LIST || rhs.id() != LogicalTypeId::LIST) {
		throw BinderException("Cannot concatenate types %s and %s", lhs.ToString(), rhs.ToString());
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

	SetArgumentType(bound_function, list_type, is_operator);
	return make_uniq<ConcatFunctionData>(bound_function.return_type, is_operator);
}

static void FindFirstTwoArguments(vector<unique_ptr<Expression>> &arguments, LogicalTypeId &first_arg,
                                  LogicalTypeId &second_arg) {
	first_arg = arguments[0]->return_type.id();
	second_arg = first_arg;
	if (arguments.size() > 1) {
		second_arg = arguments[1]->return_type.id();
	}
}

static unique_ptr<FunctionData> BindConcatFunction(ClientContext &context, ScalarFunction &bound_function,
                                                   vector<unique_ptr<Expression>> &arguments) {
	LogicalTypeId first_arg;
	LogicalTypeId second_arg;
	FindFirstTwoArguments(arguments, first_arg, second_arg);

	if (arguments.size() > 2 && (first_arg == LogicalTypeId::ARRAY || first_arg == LogicalTypeId::LIST)) {
		throw BinderException("list_concat only accepts two arguments");
	}

	if (first_arg == LogicalTypeId::ARRAY || second_arg == LogicalTypeId::ARRAY) {
		HandleArrayBinding(context, arguments);
		FindFirstTwoArguments(arguments, first_arg, second_arg);
	}

	if (first_arg == LogicalTypeId::LIST || second_arg == LogicalTypeId::LIST) {
		return HandleListBinding(context, bound_function, arguments, false);
	}

	// we can now assume that the input is a string or castable to a string
	SetArgumentType(bound_function, LogicalType::VARCHAR, false);
	return make_uniq<ConcatFunctionData>(bound_function.return_type, false);
}

static unique_ptr<FunctionData> BindConcatOperator(ClientContext &context, ScalarFunction &bound_function,
                                                   vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(arguments.size() == 2);

	LogicalTypeId lhs;
	LogicalTypeId rhs;
	FindFirstTwoArguments(arguments, lhs, rhs);

	if (lhs == LogicalTypeId::ARRAY || rhs == LogicalTypeId::ARRAY) {
		HandleArrayBinding(context, arguments);
		FindFirstTwoArguments(arguments, lhs, rhs);
	}

	if (lhs == LogicalTypeId::LIST || rhs == LogicalTypeId::LIST) {
		return HandleListBinding(context, bound_function, arguments, true);
	}

	// we can now assume that the input is a string or castable to a string
	SetArgumentType(bound_function, LogicalType::VARCHAR, true);
	return make_uniq<ConcatFunctionData>(bound_function.return_type, true);
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
