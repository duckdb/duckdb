#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/bit.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/function/scalar/string_common.hpp"
#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "utf8proc.hpp"

namespace duckdb {

namespace {

// length returns the number of unicode codepoints
struct StringLengthOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return Length<TA, TR>(input);
	}
};

struct GraphemeCountOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return GraphemeCount<TA, TR>(input);
	}
};

// strlen returns the size in bytes
struct StrLenOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return UnsafeNumericCast<TR>(input.GetSize());
	}
};

struct OctetLenOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return UnsafeNumericCast<TR>(Bit::OctetLength(input));
	}
};

// bitlen returns the size in bits
struct BitLenOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return UnsafeNumericCast<TR>(8 * input.GetSize());
	}
};

// bitstringlen returns the amount of bits in a bitstring
struct BitStringLenOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return UnsafeNumericCast<TR>(Bit::BitLength(input));
	}
};

unique_ptr<BaseStatistics> LengthPropagateStats(ClientContext &context, FunctionStatisticsInput &input) {
	auto &child_stats = input.child_stats;
	auto &expr = input.expr;
	D_ASSERT(child_stats.size() == 1);
	// can only propagate stats if the children have stats
	if (!StringStats::CanContainUnicode(child_stats[0])) {
		expr.function.SetFunctionCallback(ScalarFunction::UnaryFunction<string_t, int64_t, StrLenOperator>);
	}
	return nullptr;
}

//------------------------------------------------------------------
// ARRAY / LIST LENGTH
//------------------------------------------------------------------
void ListLengthFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input = args.data[0];
	D_ASSERT(input.GetType().id() == LogicalTypeId::LIST);
	UnaryExecutor::Execute<list_entry_t, int64_t>(
	    input, result, args.size(), [](list_entry_t input) { return UnsafeNumericCast<int64_t>(input.length); });
}

void ArrayLengthFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input = args.data[0];

	auto validity_entries = args.data[0].Validity(args.size());

	// for arrays the length is constant
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	ConstantVector::GetData<int64_t>(result)[0] = static_cast<int64_t>(ArrayType::GetSize(input.GetType()));

	// but we do need to take null values into account
	if (!validity_entries.CanHaveNull()) {
		// if there are no null values we can just return the constant
		return;
	}
	// otherwise we flatten and inherit the null values of the parent
	result.Flatten(args.size());
	auto &result_validity = FlatVector::Validity(result);
	for (idx_t r = 0; r < args.size(); r++) {
		if (!validity_entries.IsValid(r)) {
			result_validity.SetInvalid(r);
		}
	}
}

unique_ptr<FunctionData> ArrayOrListLengthBind(BindScalarFunctionInput &input) {
	auto &bound_function = input.GetBoundFunction();
	auto &arguments = input.GetArguments();
	if (arguments[0]->HasParameter() || arguments[0]->return_type.id() == LogicalTypeId::UNKNOWN) {
		throw ParameterNotResolvedException();
	}

	const auto &arg_type = arguments[0]->return_type.id();
	if (arg_type == LogicalTypeId::ARRAY) {
		bound_function.SetFunctionCallback(ArrayLengthFunction);
	} else if (arg_type == LogicalTypeId::LIST) {
		bound_function.SetFunctionCallback(ListLengthFunction);
	} else {
		// Unreachable
		throw BinderException("length can only be used on arrays or lists");
	}
	bound_function.arguments[0] = arguments[0]->return_type;
	return nullptr;
}

//------------------------------------------------------------------
// ARRAY / LIST WITH DIMENSION
//------------------------------------------------------------------
void ListLengthBinaryFunction(DataChunk &args, ExpressionState &, Vector &result) {
	auto &input = args.data[0];
	auto &dim_vec = args.data[1];
	auto count = args.size();

	UnifiedVectorFormat input_data, dim_data;
	input.ToUnifiedFormat(count, input_data);
	dim_vec.ToUnifiedFormat(count, dim_data);

	auto *result_data = FlatVector::GetDataMutable<int64_t>(result);
	auto &result_validity = FlatVector::Validity(result);

	for (idx_t i = 0; i < count; i++) {
		auto input_idx = input_data.sel->get_index(i);
		auto dim_idx = dim_data.sel->get_index(i);

		if (!input_data.validity.RowIsValid(input_idx) || !dim_data.validity.RowIsValid(dim_idx)) {
			result_validity.SetInvalid(i);
			continue;
		}

		auto dim = UnifiedVectorFormat::GetData<int64_t>(dim_data)[dim_idx];
		if (dim <= 0) {
			result_validity.SetInvalid(i);
			continue;
		}

		// Walk down dim-1 levels of nesting to find the target list
		Vector *current_vec = &input;
		idx_t current_idx = input_idx;
		bool valid = true;

		for (int64_t d = 1; d < dim; d++) {
			if (current_vec->GetType().id() != LogicalTypeId::LIST) {
				valid = false;
				break;
			}
			UnifiedVectorFormat current_data;
			current_vec->ToUnifiedFormat(count, current_data);
			auto mapped = current_data.sel->get_index(current_idx);
			if (!current_data.validity.RowIsValid(mapped)) {
				valid = false;
				break;
			}
			auto entry = UnifiedVectorFormat::GetData<list_entry_t>(current_data)[mapped];
			if (entry.length == 0) {
				valid = false;
				break;
			}
			// Descend into the first element of this list
			current_vec = &ListVector::GetEntry(*current_vec);
			current_idx = entry.offset;
		}

		if (!valid || current_vec->GetType().id() != LogicalTypeId::LIST) {
			result_validity.SetInvalid(i);
			continue;
		}

		UnifiedVectorFormat current_data;
		current_vec->ToUnifiedFormat(count, current_data);
		auto mapped = current_data.sel->get_index(current_idx);
		if (!current_data.validity.RowIsValid(mapped)) {
			result_validity.SetInvalid(i);
			continue;
		}
		auto entry = UnifiedVectorFormat::GetData<list_entry_t>(current_data)[mapped];
		if (entry.length == 0) {
			result_validity.SetInvalid(i);
			continue;
		}
		result_data[i] = UnsafeNumericCast<int64_t>(entry.length);
	}
}

struct ArrayLengthBinaryFunctionData : public FunctionData {
	vector<int64_t> dimensions;

	unique_ptr<FunctionData> Copy() const override {
		auto copy = make_uniq<ArrayLengthBinaryFunctionData>();
		copy->dimensions = dimensions;
		return std::move(copy);
	}

	bool Equals(const FunctionData &other) const override {
		auto &other_data = other.Cast<const ArrayLengthBinaryFunctionData>();
		return dimensions == other_data.dimensions;
	}
};

void ArrayLengthBinaryFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto type = args.data[0].GetType();
	auto &dimension = args.data[1];

	auto &expr = state.expr.Cast<BoundFunctionExpression>();
	auto &data = expr.bind_info->Cast<ArrayLengthBinaryFunctionData>();
	auto &dimensions = data.dimensions;
	auto max_dimension = static_cast<int64_t>(dimensions.size());

	UnaryExecutor::Execute<int64_t, int64_t>(dimension, result, args.size(), [&](int64_t dimension) {
		if (dimension < 1 || dimension > max_dimension) {
			throw OutOfRangeException(StringUtil::Format(
			    "array_length dimension '%lld' out of range (min: '1', max: '%lld')", dimension, max_dimension));
		}
		return dimensions[UnsafeNumericCast<idx_t>(dimension - 1)];
	});
}

unique_ptr<FunctionData> ArrayOrListLengthBinaryBind(BindScalarFunctionInput &input) {
	auto &bound_function = input.GetBoundFunction();
	auto &arguments = input.GetArguments();
	if (arguments[0]->HasParameter() || arguments[0]->return_type.id() == LogicalTypeId::UNKNOWN) {
		throw ParameterNotResolvedException();
	}
	auto type = arguments[0]->return_type;
	if (type.id() == LogicalTypeId::ARRAY) {
		bound_function.arguments[0] = type;
		bound_function.SetFunctionCallback(ArrayLengthBinaryFunction);

		// If the input is an array, the dimensions are constant, so we can calculate them at bind time
		vector<int64_t> dimensions;
		while (true) {
			if (type.id() == LogicalTypeId::ARRAY) {
				dimensions.push_back(UnsafeNumericCast<int64_t>(ArrayType::GetSize(type)));
				type = ArrayType::GetChildType(type);
			} else {
				break;
			}
		}
		auto data = make_uniq<ArrayLengthBinaryFunctionData>();
		data->dimensions = dimensions;
		return std::move(data);

	} else if (type.id() == LogicalTypeId::LIST) {
		bound_function.SetFunctionCallback(ListLengthBinaryFunction);
		bound_function.arguments[0] = type;
		return nullptr;
	} else {
		// Unreachable
		throw BinderException("array_length can only be used on arrays or lists");
	}
}

} // namespace

ScalarFunctionSet LengthFun::GetFunctions() {
	ScalarFunctionSet length("length");
	length.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::BIGINT,
	                                  ScalarFunction::UnaryFunction<string_t, int64_t, StringLengthOperator>, nullptr,
	                                  LengthPropagateStats));
	length.AddFunction(ScalarFunction({LogicalType::BIT}, LogicalType::BIGINT,
	                                  ScalarFunction::UnaryFunction<string_t, int64_t, BitStringLenOperator>));
	length.AddFunction(
	    ScalarFunction({LogicalType::LIST(LogicalType::ANY)}, LogicalType::BIGINT, nullptr, ArrayOrListLengthBind));
	return (length);
}

ScalarFunctionSet LengthGraphemeFun::GetFunctions() {
	ScalarFunctionSet length_grapheme("length_grapheme");
	length_grapheme.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::BIGINT,
	                                           ScalarFunction::UnaryFunction<string_t, int64_t, GraphemeCountOperator>,
	                                           nullptr, LengthPropagateStats));
	return (length_grapheme);
}

ScalarFunctionSet ArrayLengthFun::GetFunctions() {
	ScalarFunctionSet array_length("array_length");
	array_length.AddFunction(
	    ScalarFunction({LogicalType::LIST(LogicalType::ANY)}, LogicalType::BIGINT, nullptr, ArrayOrListLengthBind));
	array_length.AddFunction(ScalarFunction({LogicalType::LIST(LogicalType::ANY), LogicalType::BIGINT},
	                                        LogicalType::BIGINT, nullptr, ArrayOrListLengthBinaryBind));
	for (auto &func : array_length.functions) {
		func.SetFallible();
	}
	return (array_length);
}

ScalarFunction StrlenFun::GetFunction() {
	return ScalarFunction("strlen", {LogicalType::VARCHAR}, LogicalType::BIGINT,
	                      ScalarFunction::UnaryFunction<string_t, int64_t, StrLenOperator>);
}

ScalarFunctionSet BitLengthFun::GetFunctions() {
	ScalarFunctionSet bit_length("bit_length");
	bit_length.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::BIGINT,
	                                      ScalarFunction::UnaryFunction<string_t, int64_t, BitLenOperator>));
	bit_length.AddFunction(ScalarFunction({LogicalType::BIT}, LogicalType::BIGINT,
	                                      ScalarFunction::UnaryFunction<string_t, int64_t, BitStringLenOperator>));
	return (bit_length);
}

ScalarFunctionSet OctetLengthFun::GetFunctions() {
	// length for BLOB type
	ScalarFunctionSet octet_length("octet_length");
	octet_length.AddFunction(ScalarFunction({LogicalType::BLOB}, LogicalType::BIGINT,
	                                        ScalarFunction::UnaryFunction<string_t, int64_t, StrLenOperator>));
	octet_length.AddFunction(ScalarFunction({LogicalType::BIT}, LogicalType::BIGINT,
	                                        ScalarFunction::UnaryFunction<string_t, int64_t, OctetLenOperator>));
	return (octet_length);
}

} // namespace duckdb
