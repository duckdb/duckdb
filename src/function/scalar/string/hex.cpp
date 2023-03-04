#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/scalar/string_functions.hpp"

#include <string.h>

namespace duckdb {

struct HexStrOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, Vector &result) {
		auto data = input.GetDataUnsafe();
		auto size = input.GetSize();

		// Allocate empty space
		auto target = StringVector::EmptyString(result, size * 2);
		auto output = target.GetDataWriteable();

		for (idx_t i = 0; i < size; ++i) {
			memcpy(output, &StringUtil::BYTE_TO_HEXS[static_cast<idx_t>(data[i]) * 2], 2);
			output += 2;
		}

		target.Finalize();
		return target;
	}
};

struct FromHexOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, Vector &result) {
		auto data = input.GetDataUnsafe();
		auto size = input.GetSize();

		D_ASSERT(size < NumericLimits<idx_t>::Maximum());
		auto buffer_size = (size + 1) / 2;

		// Allocate empty space
		auto target = StringVector::EmptyString(result, buffer_size);
		auto output = target.GetDataWriteable();

		idx_t i = 0;
		if (size % 2 != 0) {
			*output = StringUtil::HEX_TO_DIGIT[static_cast<idx_t>(data[i])];
			i++;
			output++;
		}

		for (; i < size; i += 2) {
			uint8_t byte = (uint8_t)StringUtil::HEX_TO_DIGIT[static_cast<idx_t>(data[i])] * 0x10 +
			               (uint8_t)StringUtil::HEX_TO_DIGIT[static_cast<idx_t>(data[i + 1])];
			*output = byte;
			output++;
		}

		target.Finalize();
		return target;
	}
};

template <bool SKIP_ZERO>
struct HexNumericOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, Vector &result) {
		char buffer[sizeof(INPUT_TYPE) * 2];
		char *ptr = buffer;
		idx_t buffer_size = 0;

		bool seen_non_zero = false;
		for (idx_t offset = sizeof(INPUT_TYPE) * 8; offset >= 8; offset -= 8) {
			uint8_t byte = (uint8_t)(input >> (offset - 8));
            // at least one byte space
			if (byte == 0 && SKIP_ZERO && !seen_non_zero && offset > 8) {
				continue;
			}
			seen_non_zero = true;
			memcpy(ptr, &StringUtil::BYTE_TO_HEXS[static_cast<idx_t>(byte) * 2], 2);
			ptr += 2;
			buffer_size += 2;
		}

		// Allocate empty space
		auto target = StringVector::EmptyString(result, buffer_size);
		auto output = target.GetDataWriteable();
		memcpy(output, buffer, buffer_size);

		target.Finalize();
		return target;
	}
};

/*
static string_t ToHexScalarFunction(string_t input_str) {
    auto size = input_str.GetSize();
    auto raw_ptr = input_str.GetDataUnsafe();
    for (idx_t i = 0; i < size; ++i) {

    }
}
*/

/*
static void ToHexFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &input_vec = args.data[0];

    vector<char> buffer;
    UnaryExecutor::Execute<string_t, string_t>(
        input_vec, result, args.size(),
        [&](string_t input_string) {
            return StringVector::AddString(result,
                                           TranslateScalarFunction(input_string, needle_string, thread_string, buffer));
        });
}
*/

static void ToHexFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 1);
	auto &input = args.data[0];
	idx_t count = args.size();

	switch (input.GetType().InternalType()) {
	case PhysicalType::VARCHAR:
		UnaryExecutor::ExecuteString<string_t, string_t, HexStrOperator>(input, result, count);
		break;
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		UnaryExecutor::ExecuteString<int8_t, string_t, HexNumericOperator<true>>(input, result, count);
		break;
	case PhysicalType::INT16:
		UnaryExecutor::ExecuteString<int16_t, string_t, HexNumericOperator<true>>(input, result, count);
		break;
	case PhysicalType::INT32:
		UnaryExecutor::ExecuteString<int32_t, string_t, HexNumericOperator<true>>(input, result, count);
		break;
	case PhysicalType::INT64:
		UnaryExecutor::ExecuteString<int64_t, string_t, HexNumericOperator<true>>(input, result, count);
		break;
	// case PhysicalType::INT128:
	//	UnaryExecutor::ExecuteString<hugeint_t, string_t, HexNumericOperator<true>>(input, result, count);
	//	break;
	case PhysicalType::UINT8:
		UnaryExecutor::ExecuteString<uint8_t, string_t, HexNumericOperator<true>>(input, result, count);
		break;
	case PhysicalType::UINT16:
		UnaryExecutor::ExecuteString<uint16_t, string_t, HexNumericOperator<true>>(input, result, count);
		break;
	case PhysicalType::UINT32:
		UnaryExecutor::ExecuteString<uint32_t, string_t, HexNumericOperator<true>>(input, result, count);
		break;
	case PhysicalType::UINT64:
		UnaryExecutor::ExecuteString<uint64_t, string_t, HexNumericOperator<true>>(input, result, count);
		break;
	// case PhysicalType::FLOAT:
	//	UnaryExecutor::ExecuteString<float, string_t, HexNumericOperator<false>>(input, result, count);
	//	break;
	// case PhysicalType::DOUBLE:
	//	UnaryExecutor::ExecuteString<double, string_t, HexNumericOperator<false>>(input, result, count);
	//	break;
	default:
		throw NotImplementedException("Specifier type not implemented");
	}
}

static void FromHexFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 1);
	D_ASSERT(args.data[0].GetType().InternalType() == PhysicalType::VARCHAR);
	auto &input = args.data[0];
	idx_t count = args.size();

	UnaryExecutor::ExecuteString<string_t, string_t, FromHexOperator>(input, result, count);
}

void HexFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet to_hex("to_hex");
	ScalarFunctionSet from_hex("from_hex");

	to_hex.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, ToHexFunction));

	for (auto &type : LogicalType::Integral()) {
		if (type.id() == LogicalTypeId::HUGEINT) {
			continue;
		}
		to_hex.AddFunction(ScalarFunction({type}, LogicalType::VARCHAR, ToHexFunction));
	}

	from_hex.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::BLOB, FromHexFunction));

	set.AddFunction(to_hex);
	set.AddFunction(from_hex);
}

} // namespace duckdb
