#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/bignum.hpp"
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
		string bit_string(val.val.str);
		if (bit_string.empty() || bit_string[0] != 'x') {
			return make_uniq<ConstantExpression>(Value(string(val.val.str)));
		}
		// X'...' hex literal: val.val.str = "xFF..." (lowercase 'x' prefix + hex digits)
		const char *hex = bit_string.c_str() + 1; // skip 'x' prefix
		idx_t hex_len = bit_string.size() - 1;
		if (hex_len % 2 != 0) {
			throw ParserException("Hex string literal must have an even number of hex digits");
		}
		// Build \xHH-escaped string that Blob::ToBlob (via Value::BLOB) expects
		idx_t blob_len = hex_len / 2;
		string escaped;
		escaped.reserve(blob_len * 4);
		for (idx_t i = 0; i < hex_len; i += 2) {
			escaped += "\\x";
			escaped += hex[i];
			escaped += hex[i + 1];
		}
		return make_uniq<ConstantExpression>(Value::BLOB(escaped));
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
			// if that is not successful; try to cast as bignum for very large integers
			// this preserves precision for integers that exceed uhugeint limits
			try {
				auto bignum_str = Bignum::VarcharToBignum(str_val);
				return make_uniq<ConstantExpression>(Value::BIGNUM(bignum_str));
			} catch (const ConversionException &) {
				// if bignum parsing fails (e.g., invalid format), continue to decimal or double fallback
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
