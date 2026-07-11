#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/for_vector.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/function/scalar/compressed_materialization_functions.hpp"
#include "duckdb/function/scalar/compressed_materialization_utils.hpp"

namespace duckdb {

namespace {

// Raw int-to-int cast of a DECIMAL's integer payload: no value scaling, just a reinterpret + up/downcast.
// The compressed_materialization rule only emits these when stats prove the payload range fits the target,
// so the narrowing never loses information.
template <class FROM, class TO>
struct RawIntCast {
	static inline TO Operation(FROM input) {
		return UnsafeNumericCast<TO>(input);
	}
};

template <class TO>
struct RawIntCast<hugeint_t, TO> {
	static inline TO Operation(hugeint_t input) {
		return Hugeint::Cast<TO>(input); // non-throwing; value is known to fit
	}
};

template <class FROM>
struct RawIntCast<FROM, hugeint_t> {
	static inline hugeint_t Operation(FROM input) {
		return Hugeint::Convert(input); // widening to hugeint never overflows
	}
};

// FOR no-op: a DECIMAL scanned as a FOR vector already stores its payload in a narrow unsigned int. When
// that stored width equals the compress target, the compressed value IS the stored payload (values are in
// [0, max] so the signed/unsigned reinterpret is bit-identical) - reference it with zero per-row work.
template <class RESULT_TYPE>
bool TryDecimalCompressFOR(Vector &input, Vector &result, idx_t count) {
	if (input.GetVectorType() != VectorType::FOR_VECTOR ||
	    GetTypeIdSize(FORVector::GetStoredType(input)) != sizeof(RESULT_TYPE)) {
		return false;
	}
	Vector view(result.GetType(), FORVector::GetData(input), count);
	view.SetVectorType(VectorType::FLAT_VECTOR);
	view.BufferMutable().AddAuxiliaryData(make_uniq<VectorBufferHolder>(input.GetBufferRef()));
	FlatVector::SetValidity(view, FORVector::Validity(input));
	result.Reference(view);
	return true;
}

// COMPRESS: read the DECIMAL payload as its physical int PHYS_T, downcast to the narrow signed RESULT_TYPE.
template <class PHYS_T, class RESULT_TYPE>
void DecimalCompressFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 1);
	if (TryDecimalCompressFOR<RESULT_TYPE>(args.data[0], result, args.size())) {
		return;
	}
	UnaryExecutor::Execute<PHYS_T, RESULT_TYPE>(
	    args.data[0], result, args.size(),
	    [&](const PHYS_T &input) { return RawIntCast<PHYS_T, RESULT_TYPE>::Operation(input); },
#if defined(D_ASSERT_IS_ENABLED)
	    FunctionErrors::CAN_THROW_RUNTIME_ERROR);
#else
	    FunctionErrors::CANNOT_ERROR);
#endif
}

// DECOMPRESS: widen the narrow signed INPUT_TYPE back to the DECIMAL's physical int PHYS_T; the result vector
// is typed DECIMAL(p,s), so the widened payload is reinterpreted as the original decimal with no scaling.
template <class INPUT_TYPE, class PHYS_T>
void DecimalDecompressFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 1);
	UnaryExecutor::Execute<INPUT_TYPE, PHYS_T>(
	    args.data[0], result, args.size(),
	    [&](const INPUT_TYPE &input) { return RawIntCast<INPUT_TYPE, PHYS_T>::Operation(input); },
	    FunctionErrors::CANNOT_ERROR);
}

string DecimalCompressFunctionName(const LogicalType &result_type) {
	return StringUtil::Format("__internal_compress_decimal_%s",
	                          StringUtil::Lower(LogicalTypeIdToString(result_type.id())));
}

const char *DecimalDecompressFunctionName() {
	return "__internal_decompress_decimal";
}

// compress: dispatch on the DECIMAL's physical type (input) and the narrow signed target (result)
template <class PHYS_T>
scalar_function_t GetDecimalCompressFunctionResultSwitch(const LogicalType &result_type) {
	switch (result_type.id()) {
	case LogicalTypeId::TINYINT:
		return DecimalCompressFunction<PHYS_T, int8_t>;
	case LogicalTypeId::SMALLINT:
		return DecimalCompressFunction<PHYS_T, int16_t>;
	case LogicalTypeId::INTEGER:
		return DecimalCompressFunction<PHYS_T, int32_t>;
	case LogicalTypeId::BIGINT:
		return DecimalCompressFunction<PHYS_T, int64_t>;
	default:
		throw InternalException("Unexpected result type in GetDecimalCompressFunctionResultSwitch");
	}
}

scalar_function_t GetDecimalCompressFunctionInputSwitch(const LogicalType &input_type, const LogicalType &result_type) {
	switch (input_type.InternalType()) {
	case PhysicalType::INT16:
		return GetDecimalCompressFunctionResultSwitch<int16_t>(result_type);
	case PhysicalType::INT32:
		return GetDecimalCompressFunctionResultSwitch<int32_t>(result_type);
	case PhysicalType::INT64:
		return GetDecimalCompressFunctionResultSwitch<int64_t>(result_type);
	case PhysicalType::INT128:
		return GetDecimalCompressFunctionResultSwitch<hugeint_t>(result_type);
	default:
		throw InternalException("Unexpected input type in GetDecimalCompressFunctionInputSwitch");
	}
}

// decompress: dispatch on the narrow signed source (input) and the DECIMAL's physical type (result)
template <class INPUT_TYPE>
scalar_function_t GetDecimalDecompressFunctionResultSwitch(const LogicalType &result_type) {
	switch (result_type.InternalType()) {
	case PhysicalType::INT16:
		return DecimalDecompressFunction<INPUT_TYPE, int16_t>;
	case PhysicalType::INT32:
		return DecimalDecompressFunction<INPUT_TYPE, int32_t>;
	case PhysicalType::INT64:
		return DecimalDecompressFunction<INPUT_TYPE, int64_t>;
	case PhysicalType::INT128:
		return DecimalDecompressFunction<INPUT_TYPE, hugeint_t>;
	default:
		throw InternalException("Unexpected result type in GetDecimalDecompressFunctionResultSwitch");
	}
}

scalar_function_t GetDecimalDecompressFunctionInputSwitch(const LogicalType &input_type,
                                                          const LogicalType &result_type) {
	switch (input_type.id()) {
	case LogicalTypeId::TINYINT:
		return GetDecimalDecompressFunctionResultSwitch<int8_t>(result_type);
	case LogicalTypeId::SMALLINT:
		return GetDecimalDecompressFunctionResultSwitch<int16_t>(result_type);
	case LogicalTypeId::INTEGER:
		return GetDecimalDecompressFunctionResultSwitch<int32_t>(result_type);
	case LogicalTypeId::BIGINT:
		return GetDecimalDecompressFunctionResultSwitch<int64_t>(result_type);
	default:
		throw InternalException("Unexpected input type in GetDecimalDecompressFunctionInputSwitch");
	}
}

void CMDecimalCompressSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data,
                                const BoundScalarFunction &function) {
	serializer.WriteProperty(100, "arguments", function.GetArguments());
	serializer.WriteProperty(101, "return_type", function.GetReturnType());
}

template <scalar_function_t (*GET_FUNCTION)(const LogicalType &, const LogicalType &)>
unique_ptr<FunctionData> CMDecimalDeserialize(Deserializer &deserializer, BoundScalarFunction &function) {
	function.GetArguments() = deserializer.ReadProperty<vector<LogicalType>>(100, "arguments");
	auto return_type = deserializer.ReadProperty<LogicalType>(101, "return_type");
	function.SetFunctionCallback(GET_FUNCTION(function.GetArguments()[0], return_type));
	return nullptr;
}

// A representative DECIMAL for each physical int width, used only for catalog registration / name
// resolution during deserialization. The concrete (precision, scale) is stamped by the optimizer at
// construction time and carried through serialization, so the placeholder width is never load-bearing.
const vector<LogicalType> &DecimalRepresentatives() {
	static const vector<LogicalType> types {LogicalType::DECIMAL(4, 0), LogicalType::DECIMAL(9, 0),
	                                        LogicalType::DECIMAL(18, 0), LogicalType::DECIMAL(38, 0)};
	return types;
}

ScalarFunctionSet GetDecimalCompressFunctionSet(const LogicalType &result_type) {
	ScalarFunctionSet set {Identifier(DecimalCompressFunctionName(result_type))};
	for (const auto &input_type : DecimalRepresentatives()) {
		if (GetTypeIdSize(result_type.InternalType()) < GetTypeIdSize(input_type.InternalType())) {
			set.AddFunction(CMDecimalCompressFun::GetFunction(input_type, result_type));
		}
	}
	return set;
}

ScalarFunctionSet GetDecimalDecompressFunctionSet() {
	ScalarFunctionSet set {Identifier(DecimalDecompressFunctionName())};
	static const vector<LogicalType> narrow_types {LogicalType::TINYINT, LogicalType::SMALLINT, LogicalType::INTEGER,
	                                               LogicalType::BIGINT};
	for (const auto &input_type : narrow_types) {
		// register against the widest representative decimal; the real target type is stamped per call
		set.AddFunction(CMDecimalDecompressFun::GetFunction(input_type, LogicalType::DECIMAL(38, 0)));
	}
	return set;
}

} // namespace

ScalarFunction CMDecimalCompressFun::GetFunction(const LogicalType &input_type, const LogicalType &result_type) {
	ScalarFunction result(Identifier(DecimalCompressFunctionName(result_type)), {input_type}, result_type,
	                      GetDecimalCompressFunctionInputSwitch(input_type, result_type), CMUtils::Bind);
	result.SetSerializeCallback(CMDecimalCompressSerialize);
	result.SetDeserializeCallback(CMDecimalDeserialize<GetDecimalCompressFunctionInputSwitch>);
#if defined(D_ASSERT_IS_ENABLED)
	result.SetFallible();
#else
	result.SetErrorMode(FunctionErrors::CANNOT_ERROR);
#endif
	return result;
}

ScalarFunction CMDecimalDecompressFun::GetFunction(const LogicalType &input_type, const LogicalType &result_type) {
	ScalarFunction result(Identifier(DecimalDecompressFunctionName()), {input_type}, result_type,
	                      GetDecimalDecompressFunctionInputSwitch(input_type, result_type), CMUtils::Bind);
	result.SetSerializeCallback(CMDecimalCompressSerialize);
	result.SetDeserializeCallback(CMDecimalDeserialize<GetDecimalDecompressFunctionInputSwitch>);
	return result;
}

ScalarFunctionSet InternalCompressDecimalTinyintFun::GetFunctions() {
	return GetDecimalCompressFunctionSet(LogicalType(LogicalTypeId::TINYINT));
}

ScalarFunctionSet InternalCompressDecimalSmallintFun::GetFunctions() {
	return GetDecimalCompressFunctionSet(LogicalType(LogicalTypeId::SMALLINT));
}

ScalarFunctionSet InternalCompressDecimalIntegerFun::GetFunctions() {
	return GetDecimalCompressFunctionSet(LogicalType(LogicalTypeId::INTEGER));
}

ScalarFunctionSet InternalCompressDecimalBigintFun::GetFunctions() {
	return GetDecimalCompressFunctionSet(LogicalType(LogicalTypeId::BIGINT));
}

ScalarFunctionSet InternalDecompressDecimalFun::GetFunctions() {
	return GetDecimalDecompressFunctionSet();
}

} // namespace duckdb
