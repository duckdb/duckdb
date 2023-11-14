#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/common/types/bit.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "utf8proc.hpp"

namespace duckdb {

// length returns the number of unicode codepoints
struct StringLengthOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return LengthFun::Length<TA, TR>(input);
	}
};

struct GraphemeCountOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return LengthFun::GraphemeCount<TA, TR>(input);
	}
};

struct ArrayLengthOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return input.length;
	}
};

struct ArrayLengthBinaryOperator {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA input, TB dimension) {
		if (dimension != 1) {
			throw NotImplementedException("array_length for dimensions other than 1 not implemented");
		}
		return input.length;
	}
};

// strlen returns the size in bytes
struct StrLenOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return input.GetSize();
	}
};

struct OctetLenOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return Bit::OctetLength(input);
	}
};

// bitlen returns the size in bits
struct BitLenOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return 8 * input.GetSize();
	}
};

// bitstringlen returns the amount of bits in a bitstring
struct BitStringLenOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return Bit::BitLength(input);
	}
};

// char returns the amount of characters in the string without considering trailing spaces
struct CharLenOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static idx_t Operation(string_t input, ValidityMask &mask, idx_t idx, void *dataptr) {
		D_ASSERT(mask.RowIsValid(idx)); // TODO?
		auto length = *reinterpret_cast<idx_t *>(dataptr);
		D_ASSERT(input.GetSize() == length);
		auto string_data = input.GetDataUnsafe();
		while (length > 0 && string_data[length - 1] == ' ') {
			length--;
		}
		return length;
	}
};

static unique_ptr<BaseStatistics> LengthPropagateStats(ClientContext &context, FunctionStatisticsInput &input) {
	auto &child_stats = input.child_stats;
	auto &expr = input.expr;
	D_ASSERT(child_stats.size() == 1);
	// can only propagate stats if the children have stats
	if (!StringStats::CanContainUnicode(child_stats[0])) {
		expr.function.function = ScalarFunction::UnaryFunction<string_t, int64_t, StrLenOperator>;
	}
	return nullptr;
}

static unique_ptr<FunctionData> ListLengthBind(ClientContext &context, ScalarFunction &bound_function,
                                               vector<unique_ptr<Expression>> &arguments) {
	if (arguments[0]->HasParameter()) {
		throw ParameterNotResolvedException();
	}
	bound_function.arguments[0] = arguments[0]->return_type;
	return nullptr;
}

static void CharLength(DataChunk &input, ExpressionState &state, Vector &result) {
	D_ASSERT(input.ColumnCount() == 1);
	D_ASSERT(input.GetTypes()[0].id() == LogicalTypeId::CHAR);
	D_ASSERT(result.GetType() == LogicalType::BIGINT);
	auto width = StringType::GetWidth(input.data[0].GetType());
	return UnaryExecutor::GenericExecute<string_t, int64_t, CharLenOperator>(input.data[0], result, input.size(),
	                                                                         &width);
}

static unique_ptr<FunctionData> CharLengthBind(ClientContext &context, ScalarFunction &bound_function,
                                               vector<unique_ptr<Expression>> &arguments) {
	auto &type = arguments[0]->return_type;
	if (type.id() != LogicalTypeId::CHAR) {
		throw ParameterNotResolvedException();
	}
	bound_function.arguments[0] = type;
	return nullptr;
}

void LengthFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunction array_length_unary =
	    ScalarFunction({LogicalType::LIST(LogicalType::ANY)}, LogicalType::BIGINT,
	                   ScalarFunction::UnaryFunction<list_entry_t, int64_t, ArrayLengthOperator>, ListLengthBind);
	ScalarFunctionSet length("length");
	length.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::BIGINT,
	                                  ScalarFunction::UnaryFunction<string_t, int64_t, StringLengthOperator>, nullptr,
	                                  nullptr, LengthPropagateStats));
	length.AddFunction(ScalarFunction({LogicalType::BIT}, LogicalType::BIGINT,
	                                  ScalarFunction::UnaryFunction<string_t, int64_t, BitStringLenOperator>));
	length.AddFunction(ScalarFunction({LogicalType::CHAR(1)}, LogicalType::BIGINT, CharLength, CharLengthBind));
	length.AddFunction(array_length_unary);
	set.AddFunction(length);
	length.name = "len";
	set.AddFunction(length);

	ScalarFunctionSet length_grapheme("length_grapheme");
	length_grapheme.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::BIGINT,
	                                           ScalarFunction::UnaryFunction<string_t, int64_t, GraphemeCountOperator>,
	                                           nullptr, nullptr, LengthPropagateStats));
	set.AddFunction(length_grapheme);

	ScalarFunctionSet array_length("array_length");
	array_length.AddFunction(array_length_unary);
	array_length.AddFunction(ScalarFunction(
	    {LogicalType::LIST(LogicalType::ANY), LogicalType::BIGINT}, LogicalType::BIGINT,
	    ScalarFunction::BinaryFunction<list_entry_t, int64_t, int64_t, ArrayLengthBinaryOperator>, ListLengthBind));
	set.AddFunction(array_length);

	set.AddFunction(ScalarFunction("strlen", {LogicalType::VARCHAR}, LogicalType::BIGINT,
	                               ScalarFunction::UnaryFunction<string_t, int64_t, StrLenOperator>));
	ScalarFunctionSet bit_length("bit_length");
	bit_length.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::BIGINT,
	                                      ScalarFunction::UnaryFunction<string_t, int64_t, BitLenOperator>));
	bit_length.AddFunction(ScalarFunction({LogicalType::BIT}, LogicalType::BIGINT,
	                                      ScalarFunction::UnaryFunction<string_t, int64_t, BitStringLenOperator>));
	set.AddFunction(bit_length);
	// length for BLOB type
	ScalarFunctionSet octet_length("octet_length");
	octet_length.AddFunction(ScalarFunction({LogicalType::BLOB}, LogicalType::BIGINT,
	                                        ScalarFunction::UnaryFunction<string_t, int64_t, StrLenOperator>));
	octet_length.AddFunction(ScalarFunction({LogicalType::BIT}, LogicalType::BIGINT,
	                                        ScalarFunction::UnaryFunction<string_t, int64_t, OctetLenOperator>));
	set.AddFunction(octet_length);
}

} // namespace duckdb
