#include "duckdb/common/bswap.hpp"
#include "duckdb/function/scalar/compressed_materialization_functions.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

namespace duckdb {

static string StringCompressFunctionName(const LogicalType &result_type) {
	return StringUtil::Format("__internal_compress_string_%s",
	                          StringUtil::Lower(LogicalTypeIdToString(result_type.id())));
}

template <idx_t LENGTH>
static inline void TemplatedReverseMemCpy(const data_ptr_t &__restrict dest, const const_data_ptr_t &__restrict src) {
	for (idx_t i = 0; i < LENGTH; i++) {
		dest[i] = src[LENGTH - 1 - i];
	}
}

static inline void ReverseMemCpy(const data_ptr_t &__restrict dest, const const_data_ptr_t &__restrict src,
                                 const idx_t &length) {
	for (idx_t i = 0; i < length; i++) {
		dest[i] = src[length - 1 - i];
	}
}

template <class RESULT_TYPE>
static inline RESULT_TYPE StringCompressInternal(const string_t &input) {
	RESULT_TYPE result;
	const auto result_ptr = data_ptr_cast(&result);
	if (sizeof(RESULT_TYPE) <= string_t::INLINE_LENGTH) {
		TemplatedReverseMemCpy<sizeof(RESULT_TYPE)>(result_ptr, const_data_ptr_cast(input.GetPrefix()));
	} else if (input.IsInlined()) {
		static constexpr auto REMAINDER = sizeof(RESULT_TYPE) - string_t::INLINE_LENGTH;
		TemplatedReverseMemCpy<string_t::INLINE_LENGTH>(result_ptr + REMAINDER, const_data_ptr_cast(input.GetPrefix()));
		memset(result_ptr, '\0', REMAINDER);
	} else {
		const auto remainder = sizeof(RESULT_TYPE) - input.GetSize();
		ReverseMemCpy(result_ptr + remainder, data_ptr_cast(input.GetPointer()), input.GetSize());
		memset(result_ptr, '\0', remainder);
	}
	result_ptr[0] = UnsafeNumericCast<data_t>(input.GetSize());
	return result;
}

template <class RESULT_TYPE>
static inline RESULT_TYPE StringCompress(const string_t &input) {
	D_ASSERT(input.GetSize() < sizeof(RESULT_TYPE));
	return StringCompressInternal<RESULT_TYPE>(input);
}

template <class RESULT_TYPE>
static inline RESULT_TYPE MiniStringCompress(const string_t &input) {
	if (sizeof(RESULT_TYPE) <= string_t::INLINE_LENGTH) {
		return UnsafeNumericCast<RESULT_TYPE>(input.GetSize() + *const_data_ptr_cast(input.GetPrefix()));
	} else if (input.GetSize() == 0) {
		return 0;
	} else {
		return UnsafeNumericCast<RESULT_TYPE>(input.GetSize() + *const_data_ptr_cast(input.GetPointer()));
	}
}

template <>
inline uint8_t StringCompress(const string_t &input) {
	D_ASSERT(input.GetSize() <= sizeof(uint8_t));
	return MiniStringCompress<uint8_t>(input);
}

template <class RESULT_TYPE>
static void StringCompressFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	UnaryExecutor::Execute<string_t, RESULT_TYPE>(args.data[0], result, args.size(), StringCompress<RESULT_TYPE>);
}

template <class RESULT_TYPE>
static scalar_function_t GetStringCompressFunction(const LogicalType &result_type) {
	return StringCompressFunction<RESULT_TYPE>;
}

static scalar_function_t GetStringCompressFunctionSwitch(const LogicalType &result_type) {
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

static string StringDecompressFunctionName() {
	return "__internal_decompress_string";
}

struct StringDecompressLocalState : public FunctionLocalState {
public:
	explicit StringDecompressLocalState(ClientContext &context) : allocator(Allocator::Get(context)) {
	}

	static unique_ptr<FunctionLocalState> Init(ExpressionState &state, const BoundFunctionExpression &expr,
	                                           FunctionData *bind_data) {
		return make_uniq<StringDecompressLocalState>(state.GetContext());
	}

public:
	ArenaAllocator allocator;
};

template <class INPUT_TYPE>
static inline string_t StringDecompress(const INPUT_TYPE &input, ArenaAllocator &allocator) {
	const auto input_ptr = const_data_ptr_cast(&input);
	string_t result(input_ptr[0]);
	if (sizeof(INPUT_TYPE) <= string_t::INLINE_LENGTH) {
		const auto result_ptr = data_ptr_cast(result.GetPrefixWriteable());
		TemplatedReverseMemCpy<sizeof(INPUT_TYPE)>(result_ptr, input_ptr);
		memset(result_ptr + sizeof(INPUT_TYPE) - 1, '\0', string_t::INLINE_LENGTH - sizeof(INPUT_TYPE) + 1);
	} else if (result.GetSize() <= string_t::INLINE_LENGTH) {
		static constexpr auto REMAINDER = sizeof(INPUT_TYPE) - string_t::INLINE_LENGTH;
		const auto result_ptr = data_ptr_cast(result.GetPrefixWriteable());
		TemplatedReverseMemCpy<string_t::INLINE_LENGTH>(result_ptr, input_ptr + REMAINDER);
	} else {
		result.SetPointer(char_ptr_cast(allocator.Allocate(sizeof(INPUT_TYPE))));
		TemplatedReverseMemCpy<sizeof(INPUT_TYPE)>(data_ptr_cast(result.GetPointer()), input_ptr);
		memcpy(result.GetPrefixWriteable(), result.GetPointer(), string_t::PREFIX_LENGTH);
	}
	return result;
}

template <class INPUT_TYPE>
static inline string_t MiniStringDecompress(const INPUT_TYPE &input, ArenaAllocator &allocator) {
	if (input == 0) {
		string_t result(uint32_t(0));
		memset(result.GetPrefixWriteable(), '\0', string_t::INLINE_BYTES);
		return result;
	}

	string_t result(1);
	if (sizeof(INPUT_TYPE) <= string_t::INLINE_LENGTH) {
		memset(result.GetPrefixWriteable(), '\0', string_t::INLINE_BYTES);
		*data_ptr_cast(result.GetPrefixWriteable()) = input - 1;
	} else {
		result.SetPointer(char_ptr_cast(allocator.Allocate(1)));
		*data_ptr_cast(result.GetPointer()) = input - 1;
		memset(result.GetPrefixWriteable(), '\0', string_t::PREFIX_LENGTH);
		*result.GetPrefixWriteable() = *result.GetPointer();
	}
	return result;
}

template <>
inline string_t StringDecompress(const uint8_t &input, ArenaAllocator &allocator) {
	return MiniStringDecompress<uint8_t>(input, allocator);
}

template <class INPUT_TYPE>
static void StringDecompressFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &allocator = ExecuteFunctionState::GetFunctionState(state)->Cast<StringDecompressLocalState>().allocator;
	allocator.Reset();
	UnaryExecutor::Execute<INPUT_TYPE, string_t>(args.data[0], result, args.size(), [&](const INPUT_TYPE &input) {
		return StringDecompress<INPUT_TYPE>(input, allocator);
	});
}

template <class INPUT_TYPE>
static scalar_function_t GetStringDecompressFunction(const LogicalType &input_type) {
	return StringDecompressFunction<INPUT_TYPE>;
}

static scalar_function_t GetStringDecompressFunctionSwitch(const LogicalType &input_type) {
	switch (input_type.id()) {
	case LogicalTypeId::UTINYINT:
		return GetStringDecompressFunction<uint8_t>(input_type);
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

static void CMStringCompressSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data,
                                      const ScalarFunction &function) {
	serializer.WriteProperty(100, "arguments", function.arguments);
	serializer.WriteProperty(101, "return_type", function.return_type);
}

unique_ptr<FunctionData> CMStringCompressDeserialize(Deserializer &deserializer, ScalarFunction &function) {
	function.arguments = deserializer.ReadProperty<vector<LogicalType>>(100, "arguments");
	auto return_type = deserializer.ReadProperty<LogicalType>(101, "return_type");
	function.function = GetStringCompressFunctionSwitch(return_type);
	return nullptr;
}

ScalarFunction CMStringCompressFun::GetFunction(const LogicalType &result_type) {
	ScalarFunction result(StringCompressFunctionName(result_type), {LogicalType::VARCHAR}, result_type,
	                      GetStringCompressFunctionSwitch(result_type), CompressedMaterializationFunctions::Bind);
	result.serialize = CMStringCompressSerialize;
	result.deserialize = CMStringCompressDeserialize;
	return result;
}

void CMStringCompressFun::RegisterFunction(BuiltinFunctions &set) {
	for (const auto &result_type : CompressedMaterializationFunctions::StringTypes()) {
		set.AddFunction(CMStringCompressFun::GetFunction(result_type));
	}
}

static void CMStringDecompressSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data,
                                        const ScalarFunction &function) {
	serializer.WriteProperty(100, "arguments", function.arguments);
}

unique_ptr<FunctionData> CMStringDecompressDeserialize(Deserializer &deserializer, ScalarFunction &function) {
	function.arguments = deserializer.ReadProperty<vector<LogicalType>>(100, "arguments");
	function.function = GetStringDecompressFunctionSwitch(function.arguments[0]);
	return nullptr;
}

ScalarFunction CMStringDecompressFun::GetFunction(const LogicalType &input_type) {
	ScalarFunction result(StringDecompressFunctionName(), {input_type}, LogicalType::VARCHAR,
	                      GetStringDecompressFunctionSwitch(input_type), CompressedMaterializationFunctions::Bind,
	                      nullptr, nullptr, StringDecompressLocalState::Init);
	result.serialize = CMStringDecompressSerialize;
	result.deserialize = CMStringDecompressDeserialize;
	return result;
}

static ScalarFunctionSet GetStringDecompressFunctionSet() {
	ScalarFunctionSet set(StringDecompressFunctionName());
	for (const auto &input_type : CompressedMaterializationFunctions::StringTypes()) {
		set.AddFunction(CMStringDecompressFun::GetFunction(input_type));
	}
	return set;
}

void CMStringDecompressFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(GetStringDecompressFunctionSet());
}

} // namespace duckdb
