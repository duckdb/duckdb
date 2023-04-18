#include "duckdb/function/function_set.hpp"
#include "duckdb/function/scalar/compressed_materialization_functions.hpp"

namespace duckdb {

static string IntegralCompressFunctionName(const LogicalType &result_type) {
	return StringUtil::Format("__internal_compress_integral_%s",
	                          StringUtil::Lower(LogicalTypeIdToString(result_type.id())));
}

template <class INPUT_TYPE, class RESULT_TYPE>
struct TemplatedIntegralCompress {
	static inline RESULT_TYPE Operation(const INPUT_TYPE &input, const INPUT_TYPE &min_val) {
		D_ASSERT(min_val <= input);
		return input - min_val;
	}
};

template <class RESULT_TYPE>
struct TemplatedIntegralCompress<hugeint_t, RESULT_TYPE> {
	static inline RESULT_TYPE Operation(const hugeint_t &input, const hugeint_t &min_val) {
		D_ASSERT(min_val <= input);
		return (input - min_val).lower;
	}
};

template <class INPUT_TYPE, class RESULT_TYPE>
static void IntegralCompressFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data[1].GetVectorType() == VectorType::CONSTANT_VECTOR);
	const auto min_val = ConstantVector::GetData<INPUT_TYPE>(args.data[1])[0];
	UnaryExecutor::Execute<INPUT_TYPE, RESULT_TYPE>(args.data[0], result, args.size(), [&](const INPUT_TYPE &input) {
		return TemplatedIntegralCompress<INPUT_TYPE, RESULT_TYPE>::Operation(input, min_val);
	});
}

template <class INPUT_TYPE, class RESULT_TYPE>
static scalar_function_t GetIntegralCompressFunction(const LogicalType &input_type, const LogicalType &result_type) {
	return IntegralCompressFunction<INPUT_TYPE, RESULT_TYPE>;
}

template <class INPUT_TYPE>
static scalar_function_t GetIntegralCompressFunctionResultSwitch(const LogicalType &input_type,
                                                                 const LogicalType &result_type) {
	switch (result_type.id()) {
	case LogicalTypeId::UTINYINT:
		return GetIntegralCompressFunction<INPUT_TYPE, uint8_t>(input_type, result_type);
	case LogicalTypeId::USMALLINT:
		return GetIntegralCompressFunction<INPUT_TYPE, uint16_t>(input_type, result_type);
	case LogicalTypeId::UINTEGER:
		return GetIntegralCompressFunction<INPUT_TYPE, uint32_t>(input_type, result_type);
	case LogicalTypeId::UBIGINT:
		return GetIntegralCompressFunction<INPUT_TYPE, uint64_t>(input_type, result_type);
	default:
		throw InternalException("Unexpected result type in GetIntegralCompressFunctionResultSwitch");
	}
}

static scalar_function_t GetIntegralCompressFunctionInputSwitch(const LogicalType &input_type,
                                                                const LogicalType &result_type) {
	switch (input_type.id()) {
	case LogicalTypeId::SMALLINT:
		return GetIntegralCompressFunctionResultSwitch<int16_t>(input_type, result_type);
	case LogicalTypeId::INTEGER:
		return GetIntegralCompressFunctionResultSwitch<int32_t>(input_type, result_type);
	case LogicalTypeId::BIGINT:
		return GetIntegralCompressFunctionResultSwitch<int64_t>(input_type, result_type);
	case LogicalTypeId::HUGEINT:
		return GetIntegralCompressFunctionResultSwitch<hugeint_t>(input_type, result_type);
	case LogicalTypeId::USMALLINT:
		return GetIntegralCompressFunctionResultSwitch<uint16_t>(input_type, result_type);
	case LogicalTypeId::UINTEGER:
		return GetIntegralCompressFunctionResultSwitch<uint32_t>(input_type, result_type);
	case LogicalTypeId::UBIGINT:
		return GetIntegralCompressFunctionResultSwitch<uint64_t>(input_type, result_type);
	default:
		throw InternalException("Unexpected input type in GetIntegralCompressFunctionInputSwitch");
	}
}

static string IntegralDecompressFunctionName(const LogicalType &result_type) {
	return StringUtil::Format("__internal_decompress_integral_%s",
	                          StringUtil::Lower(LogicalTypeIdToString(result_type.id())));
}

unique_ptr<FunctionData> IntegralDecompressBind(ClientContext &context, ScalarFunction &bound_function,
                                                vector<unique_ptr<Expression>> &arguments) {
	const auto input_type_size = GetTypeIdSize(arguments[0]->return_type.InternalType());
	const auto result_type_size = GetTypeIdSize(bound_function.return_type.InternalType());
	if (result_type_size <= input_type_size) {
		throw InternalException("Cannot decompress to smaller type!");
	}
	if (!arguments[1]->IsFoldable()) {
		throw InternalException("Second argument must be constant!");
	}
	return nullptr;
}

template <class INPUT_TYPE, class RESULT_TYPE>
static inline RESULT_TYPE TemplatedIntegralDecompress(const INPUT_TYPE &input, const RESULT_TYPE &min_val) {
	return min_val + input;
}

template <class INPUT_TYPE, class RESULT_TYPE>
static void IntegralDecompressFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data[1].GetVectorType() == VectorType::CONSTANT_VECTOR);
	const auto min_val = ConstantVector::GetData<RESULT_TYPE>(args.data[1])[0];
	UnaryExecutor::Execute<INPUT_TYPE, RESULT_TYPE>(args.data[0], result, args.size(), [&](const INPUT_TYPE &input) {
		return TemplatedIntegralDecompress<INPUT_TYPE, RESULT_TYPE>(input, min_val);
	});
}

template <class INPUT_TYPE, class RESULT_TYPE>
static scalar_function_t GetIntegralDecompressFunction(const LogicalType &input_type, const LogicalType &result_type) {
	return IntegralDecompressFunction<INPUT_TYPE, RESULT_TYPE>;
}

template <class INPUT_TYPE>
static scalar_function_t GetIntegralDecompressFunctionResultSwitch(const LogicalType &input_type,
                                                                   const LogicalType &result_type) {
	switch (result_type.id()) {
	case LogicalTypeId::SMALLINT:
		return GetIntegralDecompressFunction<INPUT_TYPE, int16_t>(input_type, result_type);
	case LogicalTypeId::INTEGER:
		return GetIntegralDecompressFunction<INPUT_TYPE, int32_t>(input_type, result_type);
	case LogicalTypeId::BIGINT:
		return GetIntegralDecompressFunction<INPUT_TYPE, int64_t>(input_type, result_type);
	case LogicalTypeId::HUGEINT:
		return GetIntegralDecompressFunction<INPUT_TYPE, hugeint_t>(input_type, result_type);
	case LogicalTypeId::USMALLINT:
		return GetIntegralDecompressFunction<INPUT_TYPE, uint16_t>(input_type, result_type);
	case LogicalTypeId::UINTEGER:
		return GetIntegralDecompressFunction<INPUT_TYPE, uint32_t>(input_type, result_type);
	case LogicalTypeId::UBIGINT:
		return GetIntegralDecompressFunction<INPUT_TYPE, uint64_t>(input_type, result_type);
	default:
		throw InternalException("Unexpected input type in GetIntegralDecompressFunctionSetSwitch");
	}
}

static scalar_function_t GetIntegralDecompressFunctionInputSwitch(const LogicalType &input_type,
                                                                  const LogicalType &result_type) {
	switch (input_type.id()) {
	case LogicalTypeId::UTINYINT:
		return GetIntegralDecompressFunctionResultSwitch<uint8_t>(input_type, result_type);
	case LogicalTypeId::USMALLINT:
		return GetIntegralDecompressFunctionResultSwitch<uint16_t>(input_type, result_type);
	case LogicalTypeId::UINTEGER:
		return GetIntegralDecompressFunctionResultSwitch<uint32_t>(input_type, result_type);
	case LogicalTypeId::UBIGINT:
		return GetIntegralDecompressFunctionResultSwitch<uint64_t>(input_type, result_type);
	default:
		throw InternalException("Unexpected result type in GetIntegralDecompressFunctionInputSwitch");
	}
}

template <CompressedMaterializationDirection DIRECTION>
static void CMIntegralSerialize(FieldWriter &writer, const FunctionData *bind_data_p, const ScalarFunction &function) {
	writer.WriteField(DIRECTION);
	writer.WriteSerializable(function.return_type);
	writer.WriteRegularSerializableList(function.arguments);
}

unique_ptr<FunctionData> CMIntegralDeserialize(ClientContext &context, FieldReader &reader,
                                               ScalarFunction &bound_function) {
	auto direction = reader.ReadRequired<CompressedMaterializationDirection>();
	auto return_type = reader.ReadRequiredSerializable<LogicalType, LogicalType>();
	auto arguments = reader.template ReadRequiredSerializableList<LogicalType, LogicalType>();

	switch (direction) {
	case CompressedMaterializationDirection::COMPRESS:
		bound_function.function = GetIntegralCompressFunctionInputSwitch(arguments[0], return_type);
		break;
	case CompressedMaterializationDirection::DECOMPRESS:
		bound_function.function = GetIntegralDecompressFunctionInputSwitch(arguments[0], return_type);
		break;
	default:
		throw InternalException("Invalid CompressedMaterializationDirection encountered in CMIntegralDeserialize");
	}
	bound_function.arguments = arguments;

	return nullptr;
}

ScalarFunction CMIntegralCompressFun::GetFunction(const LogicalType &input_type, const LogicalType &result_type) {
	ScalarFunction result(IntegralCompressFunctionName(result_type), {input_type, input_type}, result_type,
	                      GetIntegralCompressFunctionInputSwitch(input_type, result_type),
	                      CompressedMaterializationFunctions::Bind);
	result.serialize = CMIntegralSerialize<CompressedMaterializationDirection::COMPRESS>;
	result.deserialize = CMIntegralDeserialize;
	return result;
}

static ScalarFunctionSet GetIntegralCompressFunctionSet(const LogicalType &result_type) {
	ScalarFunctionSet set(IntegralCompressFunctionName(result_type));
	for (const auto &input_type : LogicalType::Integral()) {
		if (GetTypeIdSize(result_type.InternalType()) < GetTypeIdSize(input_type.InternalType())) {
			set.AddFunction(CMIntegralCompressFun::GetFunction(input_type, result_type));
		}
	}
	return set;
}

void CMIntegralCompressFun::RegisterFunction(BuiltinFunctions &set) {
	for (const auto &result_type : CompressedMaterializationFunctions::IntegralTypes()) {
		set.AddFunction(GetIntegralCompressFunctionSet(result_type));
	}
}

ScalarFunction CMIntegralDecompressFun::GetFunction(const LogicalType &input_type, const LogicalType &result_type) {
	ScalarFunction result(IntegralDecompressFunctionName(result_type), {input_type, result_type}, result_type,
	                      GetIntegralDecompressFunctionInputSwitch(input_type, result_type),
	                      CompressedMaterializationFunctions::Bind);
	result.serialize = CMIntegralSerialize<CompressedMaterializationDirection::DECOMPRESS>;
	result.deserialize = CMIntegralDeserialize;
	return result;
}

static ScalarFunctionSet GetIntegralDecompressFunctionSet(const LogicalType &result_type) {
	ScalarFunctionSet set(IntegralDecompressFunctionName(result_type));
	for (const auto &input_type : CompressedMaterializationFunctions::IntegralTypes()) {
		if (GetTypeIdSize(result_type.InternalType()) > GetTypeIdSize(input_type.InternalType())) {
			set.AddFunction(CMIntegralDecompressFun::GetFunction(input_type, result_type));
		}
	}
	return set;
}

void CMIntegralDecompressFun::RegisterFunction(BuiltinFunctions &set) {
	for (const auto &result_type : LogicalType::Integral()) {
		if (GetTypeIdSize(result_type.InternalType()) > 1) {
			set.AddFunction(GetIntegralDecompressFunctionSet(result_type));
		}
	}
}

} // namespace duckdb
