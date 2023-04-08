#include "duckdb/common/bswap.hpp"
#include "duckdb/function/scalar/compressed_materialization_functions.hpp"

namespace duckdb {

static const vector<LogicalType> StringCompressedTypes() {
	return {LogicalType::USMALLINT, LogicalType::UINTEGER, LogicalType::UBIGINT, LogicalTypeId::HUGEINT};
}

template <class RESULT_TYPE>
static inline RESULT_TYPE StringCompress(const string_t &input) {
	if (input.GetSize() >= sizeof(RESULT_TYPE)) {
		throw InvalidInputException("String of size %u too large to be compressed to integer of size %u",
		                            input.GetSize(), sizeof(RESULT_TYPE));
	}

	RESULT_TYPE result;
	if (sizeof(RESULT_TYPE) <= string_t::INLINE_LENGTH) {
		memcpy(&result, data_ptr_t(&input) + sizeof(uint32_t), sizeof(RESULT_TYPE));
	} else {
		result = 0;
		memcpy(&result, input.GetDataUnsafe(), input.GetSize());
	}
	((uint8_t *)&result)[sizeof(RESULT_TYPE) - 1] = input.GetSize();
	return BSwap<RESULT_TYPE>(result);
}

template <class RESULT_TYPE>
static void StringCompressFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	UnaryExecutor::Execute<string_t, RESULT_TYPE>(args.data[0], result, args.size(), StringCompress<RESULT_TYPE>);
}

template <class RESULT_TYPE>
static ScalarFunction GetStringCompressFunction(const LogicalType &result_type) {
	return ScalarFunction(StringUtil::Format("cm_compress_string_%s", LogicalTypeIdToString(result_type.id())),
	                      {LogicalType::VARCHAR}, result_type, StringCompressFunction<RESULT_TYPE>);
}

static ScalarFunction GetStringCompressFunctionSwitch(const LogicalType &result_type) {
	switch (result_type.id()) {
	case LogicalTypeId::UTINYINT:
		return GetStringCompressFunction<uint8_t>(result_type);
	case LogicalTypeId::USMALLINT:
		return GetStringCompressFunction<uint16_t>(result_type);
	case LogicalTypeId::UINTEGER:
		return GetStringCompressFunction<uint32_t>(result_type);
	case LogicalTypeId::UBIGINT:
		return GetStringCompressFunction<uint64_t>(result_type);
	case LogicalTypeId::HUGEINT:
		return GetStringCompressFunction<hugeint_t>(result_type);
	default:
		throw InternalException("Unexpected type in GetStringCompressFunctionSwitch");
	}
}

void CMStringCompressFun::RegisterFunction(BuiltinFunctions &set) {
	for (const auto &result_type : StringCompressedTypes()) {
		set.AddFunction(GetStringCompressFunctionSwitch(result_type));
	}
}

ScalarFunction CMStringCompressFun::GetFunction(const LogicalType &result_type) {
	return GetStringCompressFunctionSwitch(result_type);
}

template <class INPUT_TYPE>
static inline string_t StringDecompress(const INPUT_TYPE &input, Vector &result_v) {
	const auto input_swapped = BSwap<INPUT_TYPE>(input);
	const auto &string_size = ((uint8_t *)&input_swapped)[sizeof(INPUT_TYPE) - 1];
	if (sizeof(INPUT_TYPE) <= string_t::INLINE_LENGTH) {
		string_t result(string_size);
		memcpy(data_ptr_t(&result) + sizeof(uint32_t), &input_swapped, sizeof(INPUT_TYPE));
		memset(data_ptr_t(&result) + sizeof(uint32_t) + sizeof(INPUT_TYPE) - 1, '\0',
		       sizeof(string_t) - sizeof(uint32_t) - sizeof(INPUT_TYPE));
		return result;
	} else {
		return StringVector::AddString(result_v, (const char *)&input_swapped, string_size);
	}
}

template <class INPUT_TYPE>
static void StringDecompressFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	UnaryExecutor::Execute<INPUT_TYPE, string_t>(args.data[0], result, args.size(), [&](const INPUT_TYPE &input) {
		return StringDecompress<INPUT_TYPE>(input, result);
	});
}

template <class INPUT_TYPE>
static ScalarFunction GetStringDecompressFunction(const LogicalType &input_type) {
	return ScalarFunction({input_type}, LogicalType::VARCHAR, StringDecompressFunction<INPUT_TYPE>);
}

static ScalarFunction GetStringDecompressFunctionSwitch(const LogicalType &input_type) {
	switch (input_type.id()) {
	case LogicalTypeId::USMALLINT:
		return GetStringDecompressFunction<uint16_t>(input_type);
	case LogicalTypeId::UINTEGER:
		return GetStringDecompressFunction<uint32_t>(input_type);
	case LogicalTypeId::UBIGINT:
		return GetStringDecompressFunction<uint64_t>(input_type);
	case LogicalTypeId::HUGEINT:
		return GetStringDecompressFunction<hugeint_t>(input_type);
	default:
		throw InternalException("Unexpected type in GetStringDecompressFunctionSwitch");
	}
}

static ScalarFunctionSet GetStringDecompressFunctionSet() {
	ScalarFunctionSet set("cm_decompress_string");
	for (const auto &input_type : StringCompressedTypes()) {
		set.AddFunction(GetStringDecompressFunctionSwitch(input_type));
	}
	return set;
}

void CMStringDecompressFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(GetStringDecompressFunctionSet());
}

ScalarFunction CMStringDecompressFun::GetFunction(const LogicalType &input_type) {
	return GetStringDecompressFunctionSwitch(input_type);
}

} // namespace duckdb
