#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/types/decimal.hpp"

namespace duckdb {
using namespace std;
using namespace duckdb_libpgquery;

unique_ptr<ConstantExpression> Transformer::TransformValue(PGValue val) {
	switch (val.type) {
	case T_PGInteger:
		assert(val.val.ival <= NumericLimits<int32_t>::Maximum());
		return make_unique<ConstantExpression>(Value::INTEGER((int32_t)val.val.ival));
	case T_PGBitString: // FIXME: this should actually convert to BLOB
	case T_PGString:
		return make_unique<ConstantExpression>(Value(string(val.val.str)));
	case T_PGFloat: {
		string_t str_val(val.val.str);
		bool try_cast_as_integer = true;
		bool try_cast_as_decimal = true;
		int decimal_position = -1;
		for(idx_t i = 0; i < str_val.GetSize(); i++) {
			if (val.val.str[i] == '.') {
				// decimal point: cast as either decimal or double
				try_cast_as_integer = false;
				decimal_position = i;
			}
			if (val.val.str[i] == 'e') {
				// found exponent, cast as double
				try_cast_as_decimal = false;
			}
		}
		if (try_cast_as_integer) {
			int64_t bigint_value;
			// try to cast as bigint first
			if (TryCast::Operation<string_t, int64_t>(str_val, bigint_value)) {
				// successfully cast to bigint: bigint value
				return make_unique<ConstantExpression>(Value::BIGINT(bigint_value));
			}
			hugeint_t hugeint_value;
			// if that is not successful; try to cast as hugeint
			if (TryCast::Operation<string_t, hugeint_t>(str_val, hugeint_value)) {
				// successfully cast to bigint: bigint value
				return make_unique<ConstantExpression>(Value::HUGEINT(hugeint_value));
			}
		}
		if (try_cast_as_decimal && decimal_position >= 0) {
			// figure out the width/scale based on the decimal position
			int width = str_val.GetSize() - 1;
			int scale = width - decimal_position;
			if (val.val.str[0] == '-') {
				width--;
			}
			if (width <= Decimal::MAX_WIDTH_DECIMAL) {
				// we can cast the value as a decimal
				Value val = Value(str_val);
				val = val.CastAs(LogicalType(LogicalTypeId::DECIMAL, width, scale));
				return make_unique<ConstantExpression>(move(val));
			}
		}
		// if there is a decimal or the value is too big to cast as either hugeint or bigint
		double dbl_value = Cast::Operation<string_t, double>(str_val);
		if (!Value::DoubleIsValid(dbl_value)) {
			throw ParserException("Double value \"%s\" is out of range!", val.val.str);
		}
		return make_unique<ConstantExpression>(Value::DOUBLE(dbl_value));
	}
	case T_PGNull:
		return make_unique<ConstantExpression>(Value(LogicalType::SQLNULL));
	default:
		throw NotImplementedException("Value not implemented!");
	}
}

unique_ptr<ParsedExpression> Transformer::TransformConstant(PGAConst *c) {
	return TransformValue(c->val);
}

} // namespace duckdb
