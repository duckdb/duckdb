#include "duckdb/common/bit_utils.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "core_functions/scalar/string_functions.hpp"

namespace duckdb {

static void WriteHexBytes(uint64_t x, char *&output, idx_t buffer_size) {
	idx_t offset = buffer_size * 4;

	for (; offset >= 4; offset -= 4) {
		uint8_t byte = (x >> (offset - 4)) & 0x0F;
		*output = Blob::HEX_TABLE[byte];
		output++;
	}
}

template <class T>
static void WriteHugeIntHexBytes(T x, char *&output, idx_t buffer_size) {
	idx_t offset = buffer_size * 4;
	auto upper = x.upper;
	auto lower = x.lower;

	for (; offset >= 68; offset -= 4) {
		uint8_t byte = (upper >> (offset - 68)) & 0x0F;
		*output = Blob::HEX_TABLE[byte];
		output++;
	}

	for (; offset >= 4; offset -= 4) {
		uint8_t byte = (lower >> (offset - 4)) & 0x0F;
		*output = Blob::HEX_TABLE[byte];
		output++;
	}
}

static void WriteBinBytes(uint64_t x, char *&output, idx_t buffer_size) {
	idx_t offset = buffer_size;
	for (; offset >= 1; offset -= 1) {
		*output = NumericCast<char>(((x >> (offset - 1)) & 0x01) + '0');
		output++;
	}
}

template <class T>
static void WriteHugeIntBinBytes(T x, char *&output, idx_t buffer_size) {
	auto upper = x.upper;
	auto lower = x.lower;
	idx_t offset = buffer_size;

	for (; offset >= 65; offset -= 1) {
		*output = ((upper >> (offset - 65)) & 0x01) + '0';
		output++;
	}

	for (; offset >= 1; offset -= 1) {
		*output = ((lower >> (offset - 1)) & 0x01) + '0';
		output++;
	}
}

struct HexStrOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, Vector &result) {
		auto data = input.GetData();
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

struct HexIntegralOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, Vector &result) {

		auto num_leading_zero = CountZeros<uint64_t>::Leading(static_cast<uint64_t>(input));
		idx_t num_bits_to_check = 64 - num_leading_zero;
		D_ASSERT(num_bits_to_check <= sizeof(INPUT_TYPE) * 8);

		idx_t buffer_size = (num_bits_to_check + 3) / 4;

		// Special case: All bits are zero
		if (buffer_size == 0) {
			auto target = StringVector::EmptyString(result, 1);
			auto output = target.GetDataWriteable();
			*output = '0';
			target.Finalize();
			return target;
		}

		D_ASSERT(buffer_size > 0);
		auto target = StringVector::EmptyString(result, buffer_size);
		auto output = target.GetDataWriteable();

		WriteHexBytes(static_cast<uint64_t>(input), output, buffer_size);

		target.Finalize();
		return target;
	}
};

struct HexHugeIntOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, Vector &result) {

		idx_t num_leading_zero = CountZeros<hugeint_t>::Leading(UnsafeNumericCast<hugeint_t>(input));
		idx_t buffer_size = sizeof(INPUT_TYPE) * 2 - (num_leading_zero / 4);

		// Special case: All bits are zero
		if (buffer_size == 0) {
			auto target = StringVector::EmptyString(result, 1);
			auto output = target.GetDataWriteable();
			*output = '0';
			target.Finalize();
			return target;
		}

		D_ASSERT(buffer_size > 0);
		auto target = StringVector::EmptyString(result, buffer_size);
		auto output = target.GetDataWriteable();

		WriteHugeIntHexBytes<hugeint_t>(input, output, buffer_size);

		target.Finalize();
		return target;
	}
};

struct HexUhugeIntOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, Vector &result) {

		idx_t num_leading_zero = CountZeros<uhugeint_t>::Leading(UnsafeNumericCast<uhugeint_t>(input));
		idx_t buffer_size = sizeof(INPUT_TYPE) * 2 - (num_leading_zero / 4);

		// Special case: All bits are zero
		if (buffer_size == 0) {
			auto target = StringVector::EmptyString(result, 1);
			auto output = target.GetDataWriteable();
			*output = '0';
			target.Finalize();
			return target;
		}

		D_ASSERT(buffer_size > 0);
		auto target = StringVector::EmptyString(result, buffer_size);
		auto output = target.GetDataWriteable();

		WriteHugeIntHexBytes<uhugeint_t>(input, output, buffer_size);

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
		auto data = input.GetData();
		auto size = input.GetSize();

		// Allocate empty space
		auto target = StringVector::EmptyString(result, size * 8);
		auto output = target.GetDataWriteable();

		for (idx_t i = 0; i < size; ++i) {
			auto byte = static_cast<uint8_t>(data[i]);
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

		auto num_leading_zero = CountZeros<uint64_t>::Leading(static_cast<uint64_t>(input));
		idx_t num_bits_to_check = 64 - num_leading_zero;
		D_ASSERT(num_bits_to_check <= sizeof(INPUT_TYPE) * 8);

		idx_t buffer_size = num_bits_to_check;

		// Special case: All bits are zero
		if (buffer_size == 0) {
			auto target = StringVector::EmptyString(result, 1);
			auto output = target.GetDataWriteable();
			*output = '0';
			target.Finalize();
			return target;
		}

		D_ASSERT(buffer_size > 0);
		auto target = StringVector::EmptyString(result, buffer_size);
		auto output = target.GetDataWriteable();

		WriteBinBytes(static_cast<uint64_t>(input), output, buffer_size);

		target.Finalize();
		return target;
	}
};

struct BinaryHugeIntOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, Vector &result) {
		auto num_leading_zero = CountZeros<hugeint_t>::Leading(UnsafeNumericCast<hugeint_t>(input));
		idx_t buffer_size = sizeof(INPUT_TYPE) * 8 - num_leading_zero;

		// Special case: All bits are zero
		if (buffer_size == 0) {
			auto target = StringVector::EmptyString(result, 1);
			auto output = target.GetDataWriteable();
			*output = '0';
			target.Finalize();
			return target;
		}

		auto target = StringVector::EmptyString(result, buffer_size);
		auto output = target.GetDataWriteable();

		WriteHugeIntBinBytes<hugeint_t>(input, output, buffer_size);

		target.Finalize();
		return target;
	}
};

struct BinaryUhugeIntOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, Vector &result) {
		auto num_leading_zero = CountZeros<hugeint_t>::Leading(UnsafeNumericCast<uhugeint_t>(input));
		idx_t buffer_size = sizeof(INPUT_TYPE) * 8 - num_leading_zero;

		// Special case: All bits are zero
		if (buffer_size == 0) {
			auto target = StringVector::EmptyString(result, 1);
			auto output = target.GetDataWriteable();
			*output = '0';
			target.Finalize();
			return target;
		}

		auto target = StringVector::EmptyString(result, buffer_size);
		auto output = target.GetDataWriteable();

		WriteHugeIntBinBytes<uhugeint_t>(input, output, buffer_size);

		target.Finalize();
		return target;
	}
};

struct FromHexOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, Vector &result) {
		auto data = input.GetData();
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
			*output = static_cast<char>(StringUtil::GetHexValue(data[i]));
			i++;
			output++;
		}

		for (; i < size; i += 2) {
			uint8_t major = StringUtil::GetHexValue(data[i]);
			uint8_t minor = StringUtil::GetHexValue(data[i + 1]);
			*output = static_cast<char>((major << 4) | minor);
			output++;
		}

		target.Finalize();
		return target;
	}
};

struct FromBinaryOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, Vector &result) {
		auto data = input.GetData();
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
			*output = static_cast<char>(byte); // binary eh
			output++;
		}

		while (i < size) {
			uint8_t byte = 0;
			for (idx_t j = 8; j > 0; --j) {
				byte |= StringUtil::GetBinaryValue(data[i]) << (j - 1);
				i++;
			}
			*output = static_cast<char>(byte);
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

ScalarFunctionSet HexFun::GetFunctions() {
	ScalarFunctionSet to_hex;
	to_hex.AddFunction(
	    ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, ToHexFunction<string_t, HexStrOperator>));
	to_hex.AddFunction(
	    ScalarFunction({LogicalType::VARINT}, LogicalType::VARCHAR, ToHexFunction<string_t, HexStrOperator>));
	to_hex.AddFunction(
	    ScalarFunction({LogicalType::BLOB}, LogicalType::VARCHAR, ToHexFunction<string_t, HexStrOperator>));
	to_hex.AddFunction(
	    ScalarFunction({LogicalType::BIGINT}, LogicalType::VARCHAR, ToHexFunction<int64_t, HexIntegralOperator>));
	to_hex.AddFunction(
	    ScalarFunction({LogicalType::UBIGINT}, LogicalType::VARCHAR, ToHexFunction<uint64_t, HexIntegralOperator>));
	to_hex.AddFunction(
	    ScalarFunction({LogicalType::HUGEINT}, LogicalType::VARCHAR, ToHexFunction<hugeint_t, HexHugeIntOperator>));
	to_hex.AddFunction(
	    ScalarFunction({LogicalType::UHUGEINT}, LogicalType::VARCHAR, ToHexFunction<uhugeint_t, HexUhugeIntOperator>));
	return to_hex;
}

ScalarFunction UnhexFun::GetFunction() {
	ScalarFunction function({LogicalType::VARCHAR}, LogicalType::BLOB, FromHexFunction);
	BaseScalarFunction::SetReturnsError(function);
	return function;
}

ScalarFunctionSet BinFun::GetFunctions() {
	ScalarFunctionSet to_binary;

	to_binary.AddFunction(
	    ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, ToBinaryFunction<string_t, BinaryStrOperator>));
	to_binary.AddFunction(
	    ScalarFunction({LogicalType::VARINT}, LogicalType::VARCHAR, ToBinaryFunction<string_t, BinaryStrOperator>));
	to_binary.AddFunction(ScalarFunction({LogicalType::UBIGINT}, LogicalType::VARCHAR,
	                                     ToBinaryFunction<uint64_t, BinaryIntegralOperator>));
	to_binary.AddFunction(
	    ScalarFunction({LogicalType::BIGINT}, LogicalType::VARCHAR, ToBinaryFunction<int64_t, BinaryIntegralOperator>));
	to_binary.AddFunction(ScalarFunction({LogicalType::HUGEINT}, LogicalType::VARCHAR,
	                                     ToBinaryFunction<hugeint_t, BinaryHugeIntOperator>));
	to_binary.AddFunction(ScalarFunction({LogicalType::UHUGEINT}, LogicalType::VARCHAR,
	                                     ToBinaryFunction<uhugeint_t, BinaryUhugeIntOperator>));
	return to_binary;
}

ScalarFunction UnbinFun::GetFunction() {
	ScalarFunction function({LogicalType::VARCHAR}, LogicalType::BLOB, FromBinaryFunction);
	BaseScalarFunction::SetReturnsError(function);
	return function;
}

} // namespace duckdb
