#include "duckdb/function/function_set.hpp"
#include "duckdb/function/scalar/compressed_materialization_functions.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

namespace duckdb {

const vector<LogicalType> CompressedIntegralTypes() {
	return {LogicalType::UTINYINT, LogicalType::USMALLINT, LogicalType::UINTEGER, LogicalType::UBIGINT};
}

unique_ptr<FunctionData> CompressIntegralBind(ClientContext &context, ScalarFunction &bound_function,
                                              vector<unique_ptr<Expression>> &arguments) {
	const auto input_type_size = GetTypeIdSize(arguments[0]->return_type.InternalType());
	const auto result_type_size = GetTypeIdSize(bound_function.return_type.InternalType());
	if (result_type_size >= input_type_size) {
		throw InvalidInputException("Cannot compress to larger type!");
	}
	if (!arguments[1]->IsFoldable()) {
		throw InvalidInputException("Argument \"min_val\" must be constant!");
	}
	return nullptr;
}

template <class INPUT_TYPE, class RESULT_TYPE>
struct TemplatedCompressIntegral {
	static inline RESULT_TYPE Operation(const INPUT_TYPE &input, const INPUT_TYPE &min_val) {
		D_ASSERT(min_val <= input);
		return input - min_val;
	}
};

template <class RESULT_TYPE>
struct TemplatedCompressIntegral<hugeint_t, RESULT_TYPE> {
	static inline RESULT_TYPE Operation(const hugeint_t &input, const hugeint_t &min_val) {
		D_ASSERT(min_val <= input);
		return (input - min_val).lower;
	}
};

template <class INPUT_TYPE, class RESULT_TYPE>
static void CompressIntegralFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data[1].GetVectorType() == VectorType::CONSTANT_VECTOR);
	const auto min_val = ConstantVector::GetData<INPUT_TYPE>(args.data[0])[0];
	UnaryExecutor::Execute<INPUT_TYPE, RESULT_TYPE>(args.data[0], result, args.size(), [&](const INPUT_TYPE &input) {
		return TemplatedCompressIntegral<INPUT_TYPE, RESULT_TYPE>::Operation(input, min_val);
	});
}

template <class INPUT_TYPE, class RESULT_TYPE>
static ScalarFunction GetCompressFunction(const LogicalType &input_type, const LogicalType &result_type) {
	return ScalarFunction({input_type}, result_type, CompressIntegralFunction<INPUT_TYPE, RESULT_TYPE>,
	                      CompressIntegralBind);
}

template <class RESULT_TYPE>
static ScalarFunction GetCompressFunctionSwitch(const LogicalType &input_type, const LogicalType &result_type) {
	switch (input_type.id()) {
	case LogicalTypeId::SMALLINT:
		return GetCompressFunction<int16_t, RESULT_TYPE>(input_type, result_type);
	case LogicalTypeId::INTEGER:
		return GetCompressFunction<int32_t, RESULT_TYPE>(input_type, result_type);
	case LogicalTypeId::BIGINT:
		return GetCompressFunction<int64_t, RESULT_TYPE>(input_type, result_type);
	case LogicalTypeId::HUGEINT:
		return GetCompressFunction<hugeint_t, RESULT_TYPE>(input_type, result_type);
	case LogicalTypeId::USMALLINT:
		return GetCompressFunction<uint16_t, RESULT_TYPE>(input_type, result_type);
	case LogicalTypeId::UINTEGER:
		return GetCompressFunction<uint32_t, RESULT_TYPE>(input_type, result_type);
	case LogicalTypeId::UBIGINT:
		return GetCompressFunction<uint64_t, RESULT_TYPE>(input_type, result_type);
	default:
		throw InternalException("Unexpected input type in GetCompressFunctionSwitch");
	}
}

template <class RESULT_TYPE>
static ScalarFunctionSet GetCompressFunctionSet(const LogicalType &result_type) {
	ScalarFunctionSet set(CompressedMaterializationIntegralCompressFun::GetFunctionName(result_type));
	for (const auto &input_type : LogicalType::Integral()) {
		if (GetTypeIdSize(result_type.InternalType()) < GetTypeIdSize(input_type.InternalType())) {
			set.AddFunction(GetCompressFunctionSwitch<RESULT_TYPE>(input_type, result_type));
		}
	}
	return set;
}

static ScalarFunctionSet GetCompressFunctionSetSwitch(const LogicalType &result_type) {
	switch (result_type.id()) {
	case LogicalTypeId::UTINYINT:
		return GetCompressFunctionSet<uint8_t>(result_type);
	case LogicalTypeId::USMALLINT:
		return GetCompressFunctionSet<uint16_t>(result_type);
	case LogicalTypeId::UINTEGER:
		return GetCompressFunctionSet<uint32_t>(result_type);
	case LogicalTypeId::UBIGINT:
		return GetCompressFunctionSet<uint64_t>(result_type);
	default:
		throw InternalException("Unexpected result type in GetCompressFunctionSetSwitch");
	}
}

void CompressedMaterializationIntegralCompressFun::RegisterFunction(duckdb::BuiltinFunctions &set) {
	for (const auto &result_type : CompressedIntegralTypes()) {
		set.AddFunction(GetCompressFunctionSetSwitch(result_type));
	}
}

string CompressedMaterializationIntegralCompressFun::GetFunctionName(const duckdb::LogicalType &result_type) {
	return StringUtil::Format("cm_compress_integral_%s", LogicalTypeIdToString(result_type.id()));
}

unique_ptr<FunctionData> DecompressIntegralBind(ClientContext &context, ScalarFunction &bound_function,
                                                vector<unique_ptr<Expression>> &arguments) {
	const auto input_type_size = GetTypeIdSize(arguments[0]->return_type.InternalType());
	const auto result_type_size = GetTypeIdSize(bound_function.return_type.InternalType());
	if (result_type_size <= input_type_size) {
		throw InvalidInputException("Cannot decompress to smaller type!");
	}
	if (!arguments[1]->IsFoldable()) {
		throw InvalidInputException("Argument \"min_val\" must be constant!");
	}
	return nullptr;
}

template <class INPUT_TYPE, class RESULT_TYPE>
static inline RESULT_TYPE TemplatedDecompressIntegral(const INPUT_TYPE &input, const RESULT_TYPE &min_val) {
	return min_val + input;
}

template <class INPUT_TYPE, class RESULT_TYPE>
static void DecompressIntegralFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data[1].GetVectorType() == VectorType::CONSTANT_VECTOR);
	const auto min_val = ConstantVector::GetData<RESULT_TYPE>(args.data[0])[0];
	UnaryExecutor::Execute<INPUT_TYPE, RESULT_TYPE>(args.data[0], result, args.size(), [&](const INPUT_TYPE &input) {
		return TemplatedDecompressIntegral<INPUT_TYPE, RESULT_TYPE>(input, min_val);
	});
}

template <class INPUT_TYPE, class RESULT_TYPE>
static ScalarFunction GetDecompressFunction(const LogicalType &input_type, const LogicalType &result_type) {
	return ScalarFunction(CompressedMaterializationIntegralDecompressFun::GetFunctionName(result_type), {input_type},
	                      result_type, DecompressIntegralFunction<INPUT_TYPE, RESULT_TYPE>, DecompressIntegralBind);
}

template <class RESULT_TYPE>
static ScalarFunction GetDecompressFunctionSwitch(const LogicalType &input_type, const LogicalType &result_type) {
	switch (input_type.id()) {
	case LogicalTypeId::UTINYINT:
		return GetDecompressFunction<uint8_t, RESULT_TYPE>(input_type, result_type);
	case LogicalTypeId::USMALLINT:
		return GetDecompressFunction<uint16_t, RESULT_TYPE>(input_type, result_type);
	case LogicalTypeId::UINTEGER:
		return GetDecompressFunction<uint32_t, RESULT_TYPE>(input_type, result_type);
	case LogicalTypeId::UBIGINT:
		return GetDecompressFunction<uint64_t, RESULT_TYPE>(input_type, result_type);
	default:
		throw InternalException("Unexpected result type in GetDecompressFunctionSwitch");
	}
}

template <class RESULT_TYPE>
static ScalarFunctionSet GetDecompressFunctionSet(const LogicalType &result_type) {
	ScalarFunctionSet set(CompressedMaterializationIntegralDecompressFun::GetFunctionName(result_type));
	for (const auto &input_type : CompressedIntegralTypes()) {
		if (GetTypeIdSize(result_type.InternalType()) > GetTypeIdSize(input_type.InternalType())) {
			set.AddFunction(GetDecompressFunctionSwitch<RESULT_TYPE>(input_type, result_type));
		}
	}
	return set;
}

static ScalarFunctionSet GetDecompressFunctionSetSwitch(const LogicalType &result_type) {
	switch (result_type.id()) {
	case LogicalTypeId::SMALLINT:
		return GetDecompressFunctionSet<int16_t>(result_type);
	case LogicalTypeId::INTEGER:
		return GetDecompressFunctionSet<int32_t>(result_type);
	case LogicalTypeId::BIGINT:
		return GetDecompressFunctionSet<int64_t>(result_type);
	case LogicalTypeId::HUGEINT:
		return GetDecompressFunctionSet<hugeint_t>(result_type);
	case LogicalTypeId::USMALLINT:
		return GetDecompressFunctionSet<uint16_t>(result_type);
	case LogicalTypeId::UINTEGER:
		return GetDecompressFunctionSet<uint32_t>(result_type);
	case LogicalTypeId::UBIGINT:
		return GetDecompressFunctionSet<uint64_t>(result_type);
	default:
		throw InternalException("Unexpected input type in GetDecompressFunctionSetSwitch");
	}
}

void CompressedMaterializationIntegralDecompressFun::RegisterFunction(duckdb::BuiltinFunctions &set) {
	for (const auto &result_type : LogicalType::Integral()) {
		set.AddFunction(GetDecompressFunctionSetSwitch(result_type));
	}
}

string CompressedMaterializationIntegralDecompressFun::GetFunctionName(const duckdb::LogicalType &result_type) {
	return StringUtil::Format("cm_decompress_integral_%s", LogicalTypeIdToString(result_type.id()));
}

} // namespace duckdb