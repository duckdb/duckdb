#include "duckdb/common/bswap.hpp"
#include "duckdb/function/scalar/compressed_materialization_functions.hpp"

namespace duckdb {

template <class RESULT_TYPE>
static inline RESULT_TYPE StringCompress(const string_t &input) {
	if (input.GetSize() >= sizeof(RESULT_TYPE)) {
		throw InvalidInputException("String of size %u too large to be compressed to integer of size %u",
		                            input.GetSize(), sizeof(RESULT_TYPE));
	}
	RESULT_TYPE result = 0;
	memcpy(&result, input.GetDataUnsafe(), input.GetSize());
	((uint8_t *)&result)[sizeof(RESULT_TYPE) - 1] = input.GetSize();
	return BSwap<RESULT_TYPE>(result);
}

template <class RESULT_TYPE>
static void StringCompressFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	UnaryExecutor::Execute<string_t, RESULT_TYPE>(args.data[0], result, args.size(), StringCompress<RESULT_TYPE>);
}

template <class RESULT_TYPE>
static ScalarFunction GetStringCompressFunction(const LogicalType &result_type) {
	return ScalarFunction(CompressedMaterialization::StringCompressFunctionName(result_type), {LogicalType::VARCHAR},
	                      result_type, StringCompressFunction<RESULT_TYPE>);
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

void CompressedMaterializationStringCompressFun::RegisterFunction(duckdb::BuiltinFunctions &set) {
	for (const auto &result_type : CompressedMaterialization::StringCompressedTypes()) {
		set.AddFunction(GetStringCompressFunctionSwitch(result_type));
	}
}

struct CMStringDecompressLocalState : public FunctionLocalState {
public:
	static unique_ptr<FunctionLocalState> Init(ExpressionState &state, const BoundFunctionExpression &expr,
	                                           FunctionData *bind_data) {
		return make_unique<CMStringDecompressLocalState>();
	}

public:
	StringHeap heap;
};

template <class INPUT_TYPE>
static inline string_t StringDecompress(const INPUT_TYPE &input, StringHeap &heap) {
	const auto input_swapped = BSwap<INPUT_TYPE>(input);
	const auto string_size = ((uint8_t *)&input_swapped)[sizeof(INPUT_TYPE) - 1];
	if (string_size > string_t::INLINE_LENGTH) {
		return heap.AddString((const char *)&input_swapped, string_size);
	} else {
		return string_t((const char *)&input_swapped, string_size);
	}
}

template <class INPUT_TYPE>
static void StringDecompressFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &heap = ((CMStringDecompressLocalState &)*ExecuteFunctionState::GetFunctionState(state)).heap;
	heap.Reset();
	UnaryExecutor::Execute<INPUT_TYPE, string_t>(args.data[0], result, args.size(), [&](const INPUT_TYPE &input) {
		return StringDecompress<INPUT_TYPE>(input, heap);
	});
}

template <class INPUT_TYPE>
static ScalarFunction GetStringDecompressFunction(const LogicalType &input_type) {
	return ScalarFunction({input_type}, LogicalType::VARCHAR, StringDecompressFunction<INPUT_TYPE>, nullptr, nullptr,
	                      nullptr, CMStringDecompressLocalState::Init);
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
	ScalarFunctionSet set(CompressedMaterialization::StringDecompressFunctionName());
	for (const auto &input_type : CompressedMaterialization::StringCompressedTypes()) {
		set.AddFunction(GetStringDecompressFunctionSwitch(input_type));
	}
	return set;
}

void CompressedMaterializationStringDecompressFun::RegisterFunction(duckdb::BuiltinFunctions &set) {
	set.AddFunction(GetStringDecompressFunctionSet());
}

} // namespace duckdb
