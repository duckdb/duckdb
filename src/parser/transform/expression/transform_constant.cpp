#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<ConstantExpression> Transformer::TransformValue(duckdb_libpgquery::PGValue val) {
	switch (val.type) {
	case duckdb_libpgquery::T_PGInteger:
		D_ASSERT(val.val.ival <= NumericLimits<int32_t>::Maximum());
		return make_uniq<ConstantExpression>(Value::INTEGER((int32_t)val.val.ival));
	case duckdb_libpgquery::T_PGBitString: {
		// SQL hex string literal X'...' - convert to BLOB
		// PostgreSQL parser returns the string as "xDEADBEEF" format
		string hex_str(val.val.str);

		// Skip the 'x' or 'X' prefix if present
		idx_t start_pos = 0;
		if (!hex_str.empty() && (hex_str[0] == 'x' || hex_str[0] == 'X')) {
			start_pos = 1;
		}

		// Validate and convert hex string to binary
		idx_t hex_len = hex_str.size() - start_pos;
		if (hex_len % 2 != 0) {
			throw ParserException("Invalid hexadecimal string literal: odd number of hex digits");
		}

		idx_t blob_size = hex_len / 2;
		auto blob_data = make_unsafe_uniq_array_uninitialized<data_t>(blob_size);

		for (idx_t i = 0; i < blob_size; i++) {
			int high = Blob::HEX_MAP[static_cast<unsigned char>(hex_str[start_pos + i * 2])];
			int low = Blob::HEX_MAP[static_cast<unsigned char>(hex_str[start_pos + i * 2 + 1])];
			if (high < 0 || low < 0) {
				throw ParserException("Invalid hexadecimal string literal: invalid hex character");
			}
			blob_data[i] = static_cast<data_t>((high << 4) | low);
		}

		return make_uniq<ConstantExpression>(Value::BLOB(const_data_ptr_cast(blob_data.get()), blob_size));
	}
	case duckdb_libpgquery::T_PGString:
		return make_uniq<ConstantExpression>(Value(string(val.val.str)));
	case duckdb_libpgquery::T_PGFloat: {
		string_t str_val(val.val.str);
		bool try_cast_as_integer = true;
		bool try_cast_as_decimal = true;
		optional_idx decimal_position = optional_idx::Invalid();
		idx_t num_underscores = 0;
		idx_t num_integer_underscores = 0;
		for (idx_t i = 0; i < str_val.GetSize(); i++) {
			if (val.val.str[i] == '.') {
				// decimal point: cast as either decimal or double
				try_cast_as_integer = false;
				decimal_position = i;
			}
			if (val.val.str[i] == 'e' || val.val.str[i] == 'E') {
				// found exponent, cast as double
				try_cast_as_integer = false;
				try_cast_as_decimal = false;
			}
			if (val.val.str[i] == '_') {
				num_underscores++;
				if (!decimal_position.IsValid()) {
					num_integer_underscores++;
				}
			}
		}
		if (try_cast_as_integer) {
			int64_t bigint_value;
			// try to cast as bigint first
			if (TryCast::Operation<string_t, int64_t>(str_val, bigint_value)) {
				// successfully cast to bigint: bigint value
				return make_uniq<ConstantExpression>(Value::BIGINT(bigint_value));
			}
			hugeint_t hugeint_value;
			// if that is not successful; try to cast as hugeint
			if (TryCast::Operation<string_t, hugeint_t>(str_val, hugeint_value)) {
				// successfully cast to bigint: bigint value
				return make_uniq<ConstantExpression>(Value::HUGEINT(hugeint_value));
			}
			uhugeint_t uhugeint_value;
			// if that is not successful; try to cast as uhugeint
			if (TryCast::Operation<string_t, uhugeint_t>(str_val, uhugeint_value)) {
				// successfully cast to bigint: bigint value
				return make_uniq<ConstantExpression>(Value::UHUGEINT(uhugeint_value));
			}
		}
		idx_t decimal_offset = val.val.str[0] == '-' ? 3 : 2;
		if (try_cast_as_decimal && decimal_position.IsValid() &&
		    str_val.GetSize() - num_underscores < Decimal::MAX_WIDTH_DECIMAL + decimal_offset) {
			// figure out the width/scale based on the decimal position
			auto width = NumericCast<uint8_t>(str_val.GetSize() - 1 - num_underscores);
			auto scale = NumericCast<uint8_t>(width - decimal_position.GetIndex() + num_integer_underscores);
			if (val.val.str[0] == '-') {
				width--;
			}
			if (width <= Decimal::MAX_WIDTH_DECIMAL) {
				// we can cast the value as a decimal
				Value val = Value(str_val);
				val = val.DefaultCastAs(LogicalType::DECIMAL(width, scale));
				return make_uniq<ConstantExpression>(std::move(val));
			}
		}
		// if there is a decimal or the value is too big to cast as either hugeint or bigint
		double dbl_value = Cast::Operation<string_t, double>(str_val);
		return make_uniq<ConstantExpression>(Value::DOUBLE(dbl_value));
	}
	case duckdb_libpgquery::T_PGNull:
		return make_uniq<ConstantExpression>(Value(LogicalType::SQLNULL));
	default:
		throw NotImplementedException("Value not implemented!");
	}
}

unique_ptr<ParsedExpression> Transformer::TransformConstant(duckdb_libpgquery::PGAConst &c) {
	auto constant = TransformValue(c.val);
	SetQueryLocation(*constant, c.location);
	return std::move(constant);
}

} // namespace duckdb
