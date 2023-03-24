#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/blob.hpp"
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
			*output = Blob::HEX_TABLE[(data[i] >> 4) & 0x0F];
			output++;
			*output = Blob::HEX_TABLE[data[i] & 0x0F];
			output++;
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
			i++;
			output++;
		}

		for (; i < size; i += 2) {
			uint8_t major = StringUtil::GetHexValue(data[i]);
			uint8_t minor = StringUtil::GetHexValue(data[i + 1]);
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
		for (idx_t offset = sizeof(INPUT_TYPE) * 8; offset >= 4; offset -= 4) {
			uint8_t byte = (input >> (offset - 4)) & 0x0F;
			if (byte == 0 && !seen_non_zero && offset > 4) {
				continue;
			}
			seen_non_zero = true;
			*ptr = Blob::HEX_TABLE[byte];
			ptr++;
			buffer_size++;
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
		for (idx_t offset = 64; offset >= 4; offset -= 4) {
			uint8_t byte = (upper >> (offset - 4)) & 0x0F;

			if (byte == 0 && !seen_non_zero) {
				continue;
			}
			seen_non_zero = true;
			*ptr = Blob::HEX_TABLE[byte];
			ptr++;
			buffer_size++;
		}

		for (idx_t offset = 64; offset >= 4; offset -= 4) {
			uint8_t byte = (lower >> (offset - 4)) & 0x0F;

			// at least one byte space
			if (byte == 0 && !seen_non_zero && offset > 4) {
				continue;
			}
			seen_non_zero = true;
			*ptr = Blob::HEX_TABLE[byte];
			ptr++;
			buffer_size++;
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

static idx_t NumberOfLeadingingZeros(uint64_t x) {
	if (x == 0)
		return 64;
	int n = 1;
	if (x >> 32 == 0) {
		n += 32;
		x <<= 32;
	}
	if (x >> 48 == 0) {
		n += 16;
		x <<= 16;
	}
	if (x >> 56 == 0) {
		n += 8;
		x <<= 8;
	}
	if (x >> 60 == 0) {
		n += 4;
		x <<= 4;
	}

	if (x >> 62 == 0) {
		n += 2;
		x <<= 2;
	}
	n -= x >> 63;
	return n;
}

static void WriteBinBytes(uint64_t x, char *&output, idx_t leading_zero, idx_t type_size) {
	D_ASSERT(type_size >= leading_zero);
	idx_t offset = type_size - leading_zero;
	idx_t remainder = 8 - (leading_zero % 8);
	for (idx_t i = 0; i < remainder; ++i) {
		*output = ((x >> (offset - 1)) & 0x01) + '0';
		output++;
		offset--;
	}

	for (; offset >= 8; offset -= 8) {
		uint8_t byte = (x >> (offset - 8)) & 0xFF;
		for (idx_t i = 8; i >= 1; --i) {
			*output = ((byte >> (i - 1)) & 0x01) + '0';
			output++;
		}
	}
}

struct BinaryStrOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, Vector &result) {
		auto data = input.GetDataUnsafe();
		auto size = input.GetSize();

		// Allocate empty space
		auto target = StringVector::EmptyString(result, size * 8);
		auto output = target.GetDataWriteable();

		for (idx_t i = 0; i < size; ++i) {
			uint8_t byte = data[i];
			for (idx_t i = 8; i >= 1; --i) {
				*output = ((byte >> (i - 1)) & 0x01) + '0';
				output++;
			}
		}

		target.Finalize();
		return target;
	}
};

struct BinaryIntegralOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, Vector &result) {

		if (input == 0) {
			auto target = StringVector::EmptyString(result, 1);
			auto output = target.GetDataWriteable();
			*output = '0';
			target.Finalize();
			return target;
		}

		idx_t num_leading_zero = NumberOfLeadingingZeros(input);

		auto target = StringVector::EmptyString(result, sizeof(INPUT_TYPE) * 8 - num_leading_zero);
		auto output = target.GetDataWriteable();

		WriteBinBytes(input, output, num_leading_zero, sizeof(INPUT_TYPE) * 8);

		target.Finalize();
		return target;
	}
};

struct BinaryHugeIntOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, Vector &result) {

		if (input == 0) {
			auto target = StringVector::EmptyString(result, 1);
			auto output = target.GetDataWriteable();
			*output = '0';
			target.Finalize();
			return target;
		}

		uint64_t lower = input.lower;
		int64_t upper = input.upper;
		idx_t upper_num_leading_zero = NumberOfLeadingingZeros(upper);
		idx_t lower_num_leading_zero = 0;
		idx_t num_leading_zero = upper_num_leading_zero;
		if (upper_num_leading_zero == 64) {
			lower_num_leading_zero = NumberOfLeadingingZeros(lower);
			num_leading_zero += lower_num_leading_zero;
		}

		auto target = StringVector::EmptyString(result, sizeof(INPUT_TYPE) * 8 - num_leading_zero);
		auto output = target.GetDataWriteable();

		WriteBinBytes(upper, output, upper_num_leading_zero, 64);
		WriteBinBytes(lower, output, lower_num_leading_zero, 64);

		target.Finalize();
		return target;
	}
};

struct FromBinaryOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, Vector &result) {
		auto data = input.GetDataUnsafe();
		auto size = input.GetSize();

		if (size > NumericLimits<uint32_t>::Maximum()) {
			throw InvalidInputException("Binary input length larger than 2^32 are not supported");
		}

		D_ASSERT(size <= NumericLimits<uint32_t>::Maximum());
		auto buffer_size = (size + 7) / 8;

		// Allocate empty space
		auto target = StringVector::EmptyString(result, buffer_size);
		auto output = target.GetDataWriteable();

		// Treated as a single byte
		idx_t i = 0;
		if (size % 8 != 0) {
			uint8_t byte = 0;
			for (idx_t j = size % 8; j > 0; --j) {
				byte |= StringUtil::GetBinaryValue(data[i]) << (j - 1);
				i++;
			}
			*output = byte;
			output++;
		}

		while (i < size) {
			uint8_t byte = 0;
			for (idx_t j = 8; j > 0; --j) {
				byte |= StringUtil::GetBinaryValue(data[i]) << (j - 1);
				i++;
			}
			*output = byte;
			output++;
		}

		target.Finalize();
		return target;
	}
};

static void ToBinaryFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 1);
	auto &input = args.data[0];
	idx_t count = args.size();

	switch (input.GetType().InternalType()) {
	case PhysicalType::VARCHAR:
		UnaryExecutor::ExecuteString<string_t, string_t, BinaryStrOperator>(input, result, count);
		break;
	case PhysicalType::INT64:
		UnaryExecutor::ExecuteString<int64_t, string_t, BinaryIntegralOperator>(input, result, count);
		break;
	case PhysicalType::UINT64:
		UnaryExecutor::ExecuteString<uint64_t, string_t, BinaryIntegralOperator>(input, result, count);
		break;
	case PhysicalType::INT128:
		UnaryExecutor::ExecuteString<hugeint_t, string_t, BinaryHugeIntOperator>(input, result, count);
		break;
	default:
		throw NotImplementedException("Specifier type not implemented");
	}
}

static void FromBinaryFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 1);
	D_ASSERT(args.data[0].GetType().InternalType() == PhysicalType::VARCHAR);
	auto &input = args.data[0];
	idx_t count = args.size();

	UnaryExecutor::ExecuteString<string_t, string_t, FromBinaryOperator>(input, result, count);
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

	to_hex.AddFunction(ScalarFunction({LogicalType::BIGINT}, LogicalType::VARCHAR, ToHexFunction));

	to_hex.AddFunction(ScalarFunction({LogicalType::UBIGINT}, LogicalType::VARCHAR, ToHexFunction));

	to_hex.AddFunction(ScalarFunction({LogicalType::HUGEINT}, LogicalType::VARCHAR, ToHexFunction));

	from_hex.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::BLOB, FromHexFunction));

	set.AddFunction(to_hex);
	set.AddFunction(from_hex);

	// mysql
	to_hex.name = "hex";
	from_hex.name = "unhex";
	set.AddFunction(to_hex);
	set.AddFunction(from_hex);

	ScalarFunctionSet to_binary("to_binary");
	ScalarFunctionSet from_binary("from_binary");
	to_binary.AddFunction(ScalarFunction({LogicalType::BIGINT}, LogicalType::VARCHAR, ToBinaryFunction));
	to_binary.AddFunction(ScalarFunction({LogicalType::UBIGINT}, LogicalType::VARCHAR, ToBinaryFunction));
	to_binary.AddFunction(ScalarFunction({LogicalType::HUGEINT}, LogicalType::VARCHAR, ToBinaryFunction));
	to_binary.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, ToBinaryFunction));

	from_binary.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::BLOB, FromBinaryFunction));

	set.AddFunction(to_binary);
	set.AddFunction(from_binary);

	to_binary.name = "bin";
	from_binary.name = "unbin";
	set.AddFunction(to_binary);
	set.AddFunction(from_binary);
}

} // namespace duckdb
