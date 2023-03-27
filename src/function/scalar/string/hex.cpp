#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/storage/compression/chimp/algorithm/chimp_utils.hpp"

namespace duckdb {

static void WriteHexBytes(uint64_t x, char *&output, idx_t leading_zero, idx_t type_size) {
	D_ASSERT(type_size >= leading_zero);

	idx_t offset = type_size - ((leading_zero / 4) * 4);

	for (; offset >= 4; offset -= 4) {
		uint8_t byte = (x >> (offset - 4)) & 0x0F;
		*output = Blob::HEX_TABLE[byte];
		output++;
	}
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
		if (input == 0) {
			auto target = StringVector::EmptyString(result, 1);
			auto output = target.GetDataWriteable();
			*output = '0';
			target.Finalize();
			return target;
		}

		idx_t num_leading_zero = CountZeros<uint64_t>::Leading(input);

		auto target = StringVector::EmptyString(result, sizeof(INPUT_TYPE) * 2 - (num_leading_zero / 4));
		auto output = target.GetDataWriteable();

		WriteHexBytes(input, output, num_leading_zero, sizeof(INPUT_TYPE) * 8);

		target.Finalize();
		return target;
	}
};

struct HexHugeIntOperator {
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
		idx_t upper_num_leading_zero = CountZeros<uint64_t>::Leading(upper);
		idx_t lower_num_leading_zero = 0;
		idx_t num_leading_zero = upper_num_leading_zero;
		if (upper_num_leading_zero == 64) {
			lower_num_leading_zero = CountZeros<uint64_t>::Leading(lower);
			num_leading_zero += lower_num_leading_zero;
		}

		auto target = StringVector::EmptyString(result, sizeof(INPUT_TYPE) * 2 - (num_leading_zero / 4));
		auto output = target.GetDataWriteable();

		WriteHexBytes(upper, output, upper_num_leading_zero, 64);
		WriteHexBytes(lower, output, lower_num_leading_zero, 64);

		target.Finalize();
		return target;
	}
};

template <class INPUT, class OP>
static void ToHexFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 1);
	auto &input = args.data[0];
	idx_t count = args.size();
	UnaryExecutor::ExecuteString<INPUT, string_t, OP>(input, result, count);
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

		idx_t num_leading_zero = CountZeros<uint64_t>::Leading(input);

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
		idx_t upper_num_leading_zero = CountZeros<uint64_t>::Leading(upper);
		idx_t lower_num_leading_zero = 0;
		idx_t num_leading_zero = upper_num_leading_zero;
		if (upper_num_leading_zero == 64) {
			lower_num_leading_zero = CountZeros<uint64_t>::Leading(lower);
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

template <class INPUT, class OP>
static void ToBinaryFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 1);
	auto &input = args.data[0];
	idx_t count = args.size();
	UnaryExecutor::ExecuteString<INPUT, string_t, OP>(input, result, count);
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

	to_hex.AddFunction(
	    ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, ToHexFunction<string_t, HexStrOperator>));

	to_hex.AddFunction(
	    ScalarFunction({LogicalType::BIGINT}, LogicalType::VARCHAR, ToHexFunction<int64_t, HexIntegralOperator>));

	to_hex.AddFunction(
	    ScalarFunction({LogicalType::UBIGINT}, LogicalType::VARCHAR, ToHexFunction<uint64_t, HexIntegralOperator>));

	to_hex.AddFunction(
	    ScalarFunction({LogicalType::HUGEINT}, LogicalType::VARCHAR, ToHexFunction<hugeint_t, HexHugeIntOperator>));

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
	to_binary.AddFunction(
	    ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, ToBinaryFunction<string_t, BinaryStrOperator>));
	to_binary.AddFunction(ScalarFunction({LogicalType::UBIGINT}, LogicalType::VARCHAR,
	                                     ToBinaryFunction<uint64_t, BinaryIntegralOperator>));
	to_binary.AddFunction(
	    ScalarFunction({LogicalType::BIGINT}, LogicalType::VARCHAR, ToBinaryFunction<int64_t, BinaryIntegralOperator>));
	to_binary.AddFunction(ScalarFunction({LogicalType::HUGEINT}, LogicalType::VARCHAR,
	                                     ToBinaryFunction<hugeint_t, BinaryHugeIntOperator>));

	from_binary.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::BLOB, FromBinaryFunction));

	set.AddFunction(to_binary);
	set.AddFunction(from_binary);

	to_binary.name = "bin";
	from_binary.name = "unbin";
	set.AddFunction(to_binary);
	set.AddFunction(from_binary);
}

} // namespace duckdb
