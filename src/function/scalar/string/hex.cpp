#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/scalar/string_functions.hpp"

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
			*output = StringUtil::HEX_UPPER_TABLE[(data[i] >> 4) & 0x0F];
			output++;
			*output = StringUtil::HEX_UPPER_TABLE[data[i] & 0x0F];
			output++;
			// memcpy(output, &StringUtil::BYTE_TO_HEXS[static_cast<idx_t>(data[i]) * 2], 2);
			// output += 2;
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

		if (size > NumericLimits<uint32_t>::Maximum()) {
			throw InvalidInputException("Hexadecimal input length larger than 2^32 are not supported");
		}

		D_ASSERT(size <= NumericLimits<uint32_t>::Maximum());
		auto buffer_size = (size + 1) / 2;

		// Allocate empty space
		auto target = StringVector::EmptyString(result, buffer_size);
		auto output = target.GetDataWriteable();

		// Treated as a single byte
		idx_t i = 0;
		if (size % 2 != 0) {
			*output = StringUtil::GetHexValue(data[i]);
			// *output = StringUtil::HEX_TO_DIGIT[static_cast<idx_t>(data[i])];
			i++;
			output++;
		}

		for (; i < size; i += 2) {
			uint8_t major = StringUtil::GetHexValue(data[i]);
			uint8_t minor = StringUtil::GetHexValue(data[i + 1]);
			// uint8_t byte = (uint8_t)StringUtil::HEX_TO_DIGIT[static_cast<idx_t>(data[i])] * 0x10 +
			//               (uint8_t)StringUtil::HEX_TO_DIGIT[static_cast<idx_t>(data[i + 1])];
			// *output = byte;
			*output = (major << 4) | minor;
			output++;
		}

		target.Finalize();
		return target;
	}
};

struct HexIntegralOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, Vector &result) {
		// Sufficient space for maximum length
		char buffer[sizeof(INPUT_TYPE) * 2];
		char *ptr = buffer;
		idx_t buffer_size = 0;

		bool seen_non_zero = false;
		for (idx_t offset = sizeof(INPUT_TYPE) * 8; offset >= 8; offset -= 8) {
			uint8_t byte = (uint8_t)(input >> (offset - 8));
			// offset == 8: do not continue. The result has at least one byte
			if (byte == 0 && !seen_non_zero && offset > 8) {
				continue;
			}
			seen_non_zero = true;
			*ptr = StringUtil::HEX_UPPER_TABLE[(byte >> 4) & 0x0F];
			ptr++;
			*ptr = StringUtil::HEX_UPPER_TABLE[byte & 0x0F];
			ptr++;
			// memcpy(ptr, &StringUtil::BYTE_TO_HEXS[static_cast<idx_t>(byte) * 2], 2);
			// ptr += 2;
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

struct HexHugeIntOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, Vector &result) {
		char buffer[sizeof(INPUT_TYPE) * 2];
		char *ptr = buffer;
		idx_t buffer_size = 0;

		uint64_t lower = input.lower;
		int64_t upper = input.upper;

		bool seen_non_zero = false;
		for (idx_t offset = 64; offset >= 8; offset -= 8) {
			uint8_t byte = (uint8_t)(upper >> (offset - 8));

			if (byte == 0 && !seen_non_zero) {
				continue;
			}
			seen_non_zero = true;
			*ptr = StringUtil::HEX_UPPER_TABLE[(byte >> 4) & 0x0F];
			ptr++;
			*ptr = StringUtil::HEX_UPPER_TABLE[byte & 0x0F];
			ptr++;
			// memcpy(ptr, &StringUtil::BYTE_TO_HEXS[static_cast<idx_t>(byte) * 2], 2);
			// ptr += 2;
			buffer_size += 2;
		}

		for (idx_t offset = 64; offset >= 8; offset -= 8) {
			uint8_t byte = (uint8_t)(lower >> (offset - 8));

			// at least one byte space
			if (byte == 0 && !seen_non_zero && offset > 8) {
				continue;
			}
			seen_non_zero = true;
			*ptr = StringUtil::HEX_UPPER_TABLE[(byte >> 4) & 0x0F];
			ptr++;
			*ptr = StringUtil::HEX_UPPER_TABLE[byte & 0x0F];
			ptr++;
			// memcpy(ptr, &StringUtil::BYTE_TO_HEXS[static_cast<idx_t>(byte) * 2], 2);
			// ptr += 2;
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

static void ToHexFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 1);
	auto &input = args.data[0];
	idx_t count = args.size();

	switch (input.GetType().InternalType()) {
	case PhysicalType::VARCHAR:
		UnaryExecutor::ExecuteString<string_t, string_t, HexStrOperator>(input, result, count);
		break;
	case PhysicalType::INT64:
		UnaryExecutor::ExecuteString<int64_t, string_t, HexIntegralOperator>(input, result, count);
		break;
	case PhysicalType::INT128:
		UnaryExecutor::ExecuteString<hugeint_t, string_t, HexHugeIntOperator>(input, result, count);
		break;
	case PhysicalType::UINT64:
		UnaryExecutor::ExecuteString<uint64_t, string_t, HexIntegralOperator>(input, result, count);
		break;
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
	ScalarFunctionSet hex("hex");
	ScalarFunctionSet from_hex("from_hex");
	ScalarFunctionSet unhex("unhex");

	to_hex.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, ToHexFunction));
	hex.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, ToHexFunction));

	for (auto &type : LogicalType::Integral()) {
		to_hex.AddFunction(ScalarFunction({type}, LogicalType::VARCHAR, ToHexFunction));
		hex.AddFunction(ScalarFunction({type}, LogicalType::VARCHAR, ToHexFunction));
	}

	from_hex.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::BLOB, FromHexFunction));
	unhex.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::BLOB, FromHexFunction));

	set.AddFunction(to_hex);
	set.AddFunction(hex);
	set.AddFunction(from_hex);
	set.AddFunction(unhex);
}

} // namespace duckdb
