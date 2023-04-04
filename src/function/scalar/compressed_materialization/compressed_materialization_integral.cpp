#include "duckdb/function/function_set.hpp"
#include "duckdb/function/scalar/compressed_materialization_functions.hpp"

namespace duckdb {

unique_ptr<FunctionData> IntegralCompressBind(ClientContext &context, ScalarFunction &bound_function,
                                              vector<unique_ptr<Expression>> &arguments) {
	const auto input_type_size = GetTypeIdSize(arguments[0]->return_type.InternalType());
	const auto result_type_size = GetTypeIdSize(bound_function.return_type.InternalType());
	if (result_type_size >= input_type_size) {
		throw InvalidInputException("Cannot compress to larger type!");
	}
	// TODO: make sure there's no implicit cast going on before entering this function
	if (!arguments[1]->IsFoldable()) {
		throw InvalidInputException("Argument \"min_val\" must be constant!");
	}
	return nullptr;
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
static ScalarFunction GetIntegralCompressFunction(const LogicalType &input_type, const LogicalType &result_type) {
	return ScalarFunction({input_type, input_type}, result_type, IntegralCompressFunction<INPUT_TYPE, RESULT_TYPE>,
	                      IntegralCompressBind);
}

template <class RESULT_TYPE>
static ScalarFunction GetIntegralCompressFunctionSwitch(const LogicalType &input_type, const LogicalType &result_type) {
	switch (input_type.id()) {
	case LogicalTypeId::SMALLINT:
		return GetIntegralCompressFunction<int16_t, RESULT_TYPE>(input_type, result_type);
	case LogicalTypeId::INTEGER:
		return GetIntegralCompressFunction<int32_t, RESULT_TYPE>(input_type, result_type);
	case LogicalTypeId::BIGINT:
		return GetIntegralCompressFunction<int64_t, RESULT_TYPE>(input_type, result_type);
	case LogicalTypeId::HUGEINT:
		return GetIntegralCompressFunction<hugeint_t, RESULT_TYPE>(input_type, result_type);
	case LogicalTypeId::USMALLINT:
		return GetIntegralCompressFunction<uint16_t, RESULT_TYPE>(input_type, result_type);
	case LogicalTypeId::UINTEGER:
		return GetIntegralCompressFunction<uint32_t, RESULT_TYPE>(input_type, result_type);
	case LogicalTypeId::UBIGINT:
		return GetIntegralCompressFunction<uint64_t, RESULT_TYPE>(input_type, result_type);
	default:
		throw InternalException("Unexpected input type in GetIntegralCompressFunctionSwitch");
	}
}

template <class RESULT_TYPE>
static ScalarFunctionSet GetIntegralCompressFunctionSet(const LogicalType &result_type) {
	ScalarFunctionSet set(CompressedMaterialization::IntegralCompressFunctionName(result_type));
	for (const auto &input_type : LogicalType::Integral()) {
		if (GetTypeIdSize(result_type.InternalType()) < GetTypeIdSize(input_type.InternalType())) {
			set.AddFunction(GetIntegralCompressFunctionSwitch<RESULT_TYPE>(input_type, result_type));
		}
	}
	return set;
}

static ScalarFunctionSet GetIntegralCompressFunctionSetSwitch(const LogicalType &result_type) {
	switch (result_type.id()) {
	case LogicalTypeId::UTINYINT:
		return GetIntegralCompressFunctionSet<uint8_t>(result_type);
	case LogicalTypeId::USMALLINT:
		return GetIntegralCompressFunctionSet<uint16_t>(result_type);
	case LogicalTypeId::UINTEGER:
		return GetIntegralCompressFunctionSet<uint32_t>(result_type);
	case LogicalTypeId::UBIGINT:
		return GetIntegralCompressFunctionSet<uint64_t>(result_type);
	default:
		throw InternalException("Unexpected result type in GetIntegralCompressFunctionSetSwitch");
	}
}

void CompressedMaterializationIntegralCompressFun::RegisterFunction(duckdb::BuiltinFunctions &set) {
	for (const auto &result_type : CompressedMaterialization::IntegralCompressedTypes()) {
		set.AddFunction(GetIntegralCompressFunctionSetSwitch(result_type));
	}
}

unique_ptr<FunctionData> IntegralDecompressBind(ClientContext &context, ScalarFunction &bound_function,
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
static ScalarFunction GetIntegralDecompressFunction(const LogicalType &input_type, const LogicalType &result_type) {
	return ScalarFunction({input_type, result_type}, result_type, IntegralDecompressFunction<INPUT_TYPE, RESULT_TYPE>,
	                      IntegralDecompressBind);
}

template <class RESULT_TYPE>
static ScalarFunction GetIntegralDecompressFunctionSwitch(const LogicalType &input_type,
                                                          const LogicalType &result_type) {
	switch (input_type.id()) {
	case LogicalTypeId::UTINYINT:
		return GetIntegralDecompressFunction<uint8_t, RESULT_TYPE>(input_type, result_type);
	case LogicalTypeId::USMALLINT:
		return GetIntegralDecompressFunction<uint16_t, RESULT_TYPE>(input_type, result_type);
	case LogicalTypeId::UINTEGER:
		return GetIntegralDecompressFunction<uint32_t, RESULT_TYPE>(input_type, result_type);
	case LogicalTypeId::UBIGINT:
		return GetIntegralDecompressFunction<uint64_t, RESULT_TYPE>(input_type, result_type);
	default:
		throw InternalException("Unexpected result type in GetIntegralDecompressFunctionSwitch");
	}
}

template <class RESULT_TYPE>
static ScalarFunctionSet GetIntegralDecompressFunctionSet(const LogicalType &result_type) {
	ScalarFunctionSet set(CompressedMaterialization::IntegralDecompressFunctionName(result_type));
	for (const auto &input_type : CompressedMaterialization::IntegralCompressedTypes()) {
		if (GetTypeIdSize(result_type.InternalType()) > GetTypeIdSize(input_type.InternalType())) {
			set.AddFunction(GetIntegralDecompressFunctionSwitch<RESULT_TYPE>(input_type, result_type));
		}
	}
	return set;
}

static ScalarFunctionSet GetIntegralDecompressFunctionSetSwitch(const LogicalType &result_type) {
	switch (result_type.id()) {
	case LogicalTypeId::SMALLINT:
		return GetIntegralDecompressFunctionSet<int16_t>(result_type);
	case LogicalTypeId::INTEGER:
		return GetIntegralDecompressFunctionSet<int32_t>(result_type);
	case LogicalTypeId::BIGINT:
		return GetIntegralDecompressFunctionSet<int64_t>(result_type);
	case LogicalTypeId::HUGEINT:
		return GetIntegralDecompressFunctionSet<hugeint_t>(result_type);
	case LogicalTypeId::USMALLINT:
		return GetIntegralDecompressFunctionSet<uint16_t>(result_type);
	case LogicalTypeId::UINTEGER:
		return GetIntegralDecompressFunctionSet<uint32_t>(result_type);
	case LogicalTypeId::UBIGINT:
		return GetIntegralDecompressFunctionSet<uint64_t>(result_type);
	default:
		throw InternalException("Unexpected input type in GetIntegralDecompressFunctionSetSwitch");
	}
}

void CompressedMaterializationIntegralDecompressFun::RegisterFunction(duckdb::BuiltinFunctions &set) {
	for (const auto &result_type : LogicalType::Integral()) {
		if (GetTypeIdSize(result_type.InternalType()) > 1) {
			set.AddFunction(GetIntegralDecompressFunctionSetSwitch(result_type));
		}
	}
}

} // namespace duckdb
